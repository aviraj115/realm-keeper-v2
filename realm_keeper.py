# Standard library
import os
import json
import time
import uuid
import logging
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from typing import Optional, Set, Dict, List
import threading

# Discord
import discord
from discord import app_commands, HTTPException, GatewayNotFound
from discord.ext import commands, tasks
from discord.ext.commands import Cooldown, BucketType

# Third-party
import mmh3
from dotenv import load_dotenv
from passlib.hash import bcrypt_sha256
import backoff
from aiohttp import TCPConnector, ClientTimeout
import platform
from pybloom_live import ScalableBloomFilter
import base64
import secrets
import psutil
import aiofiles
import sys
import aiohttp

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)

# Set event loop policy for Windows if needed
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Create event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# Configuration and logging setup
load_dotenv()

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

# Configure connection settings
HTTP_TIMEOUT = ClientTimeout(total=30, connect=10)
MAX_RETRIES = 3
MAX_CONNECTIONS = 100

# Move this near the top, before any classes that use it
def admin_cooldown():
    """Decorator for admin command cooldowns"""
    async def predicate(interaction: discord.Interaction):
        if interaction.user.guild_permissions.administrator:
            return True
        
        # Get cooldown bucket
        bucket = commands._buckets.get_bucket(interaction.command)
        if bucket is None:
            bucket = commands.Cooldown(1, 300, commands.BucketType.user)
            commands._buckets[interaction.command] = bucket
            
        # Check cooldown
        retry_after = bucket.update_rate_limit()
        if retry_after:
            raise commands.CommandOnCooldown(
                bucket, retry_after, commands.BucketType.user
            )
        return True
        
    return app_commands.check(predicate)

class AdaptiveWorkerPool:
    """Thread pool that scales based on load"""
    def __init__(self, min_workers=4, max_workers=16):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.pool = ThreadPoolExecutor(
            max_workers=min_workers,
            thread_name_prefix="worker"
        )
        self._lock = threading.Lock()
    
    def submit(self, fn, *args, **kwargs):
        """Submit task to pool"""
        return self.pool.submit(fn, *args, **kwargs)
    
    def scale_up(self):
        """Add workers if below max"""
        with self._lock:
            current = len(self.pool._threads)
            if current < self.max_workers:
                new_size = min(current + 2, self.max_workers)
                self._resize(new_size)
                logging.info(f"Scaled up workers to {new_size}")
    
    def scale_down(self):
        """Remove workers if above min"""
        with self._lock:
            current = len(self.pool._threads)
            if current > self.min_workers:
                new_size = max(current - 1, self.min_workers)
                self._resize(new_size)
                logging.info(f"Scaled down workers to {new_size}")
    
    def _resize(self, new_size: int):
        """Resize pool to target size"""
        old_pool = self.pool
        self.pool = ThreadPoolExecutor(
            max_workers=new_size,
            thread_name_prefix="worker"
        )
        old_pool.shutdown(wait=False)
    
    def shutdown(self):
        """Shutdown pool"""
        if self.pool:
            self.pool.shutdown(wait=True)

# Initialize worker pool
worker_pool = AdaptiveWorkerPool()

# Initialize bot with optimized connection handling
class RealmBot(commands.AutoShardedBot):
    def __init__(self):
        # Initialize bot with settings
        super().__init__(
            command_prefix="!",
            intents=intents,
            case_insensitive=True,
            max_messages=10000,
            timeout=HTTP_TIMEOUT,
            http_retry_count=MAX_RETRIES
        )
        
        # Initialize components
        self.session = None  # Will be set in setup_hook
        self.connector = None
        self.realm_keeper = None
        self.config = Config()
        self.key_cleanup = KeyCleanup(self)
        self.command_sync = CommandSync(self)
        self.key_validator = KeyValidator(self)
    
    async def setup_hook(self):
        """Initialize bot systems"""
        try:
            # Create session and connector
            self.connector = TCPConnector(
                limit=MAX_CONNECTIONS,
                ttl_dns_cache=300,
                force_close=False,
                enable_cleanup_closed=True
            )
            self.session = aiohttp.ClientSession(connector=self.connector)
            
            # Initialize systems
            await self.config.load()
            await self.key_cleanup.start()
            await self.command_sync.sync_all()
            
            # Add realm keeper cog
            self.realm_keeper = RealmKeeper(self)
            await self.add_cog(self.realm_keeper)
            
        except Exception as e:
            logging.error(f"Setup error: {str(e)}")
            raise
    
    async def close(self):
        """Cleanup on shutdown"""
        if self.session:
            await self.session.close()
        if self.connector:
            await self.connector.close()
        await super().close()

# Add near the top with other globals
bot = None

# Update main function
async def main():
    """Main entry point"""
    try:
        # Initialize bot
        bot = RealmBot()
        
        # Start bot
        async with bot:
            await bot.start(TOKEN)
            
    except Exception as e:
        logging.error(f"Startup error: {str(e)}")
        raise

# Configuration handling
class GuildConfig:
    __slots__ = ('role_id', 'main_store', 'command', 'success_msgs')
    
    def __init__(self, role_id: int, valid_keys: Set[str], command: str = "claim", 
                 success_msgs: list = None):
        self.role_id = role_id
        self.main_store = valid_keys
        self.command = command
        self.success_msgs = success_msgs or DEFAULT_SUCCESS_MESSAGES.copy()
        
        # Initialize Bloom filter
        self.bloom = ScalableBloomFilter(
            initial_capacity=1000,
            error_rate=0.001,
            mode=ScalableBloomFilter.SMALL_SET_GROWTH
        )
        self._rebuild_bloom()
    
    def _rebuild_bloom(self):
        """Rebuild Bloom filter from main store"""
        self.bloom.clear()
        for full_hash in self.main_store:
            hash_part = full_hash.split(KeySecurity.DELIMITER)[0] if KeySecurity.DELIMITER in full_hash else full_hash
            self.bloom.add(hash_part)
    
    async def add_key(self, full_hash: str, guild_id: int):
        """Add a key with Bloom filter update"""
        self.main_store.add(full_hash)
        hash_part = full_hash.split(KeySecurity.DELIMITER)[0] if KeySecurity.DELIMITER in full_hash else full_hash
        self.bloom.add(hash_part)
        await key_cache.invalidate(guild_id)
    
    async def remove_key(self, full_hash: str, guild_id: int):
        """Remove a key and rebuild Bloom filter"""
        self.main_store.discard(full_hash)
        self._rebuild_bloom()  # Need to rebuild since we can't remove from Bloom filter
        await key_cache.invalidate(guild_id)
    
    async def bulk_add_keys(self, hashes: Set[str], guild_id: int):
        """Add multiple keys efficiently"""
        self.main_store.update(hashes)
        for full_hash in hashes:
            hash_part = full_hash.split(KeySecurity.DELIMITER)[0] if KeySecurity.DELIMITER in full_hash else full_hash
            self.bloom.add(hash_part)
        await key_cache.invalidate(guild_id)

class Config:
    def __init__(self):
        self.guilds = {}  # {guild_id: GuildConfig}
        self._lock = asyncio.Lock()
        self._backup_path = "config.backup.json"
        self._config_path = "config.json"
    
    async def load(self):
        """Load configuration with error handling and backup"""
        try:
            async with aiofiles.open(self._config_path, 'r') as f:
                data = json.loads(await f.read())
                
            # Create backup
            async with aiofiles.open(self._backup_path, 'w') as f:
                await f.write(json.dumps(data))
                
            # Process guild configs
            self.guilds = {
                int(guild_id): GuildConfig(
                    cfg["role_id"],
                    set(cfg["valid_keys"]),
                    cfg.get("command", "claim"),
                    cfg.get("success_msgs", DEFAULT_SUCCESS_MESSAGES.copy())
                )
                for guild_id, cfg in data.items()
            }
            
        except FileNotFoundError:
            logging.warning("Config file not found, starting fresh")
            self.guilds = {}
            await self.save()  # Create initial config
            
        except json.JSONDecodeError as e:
            logging.error(f"Config corruption detected: {str(e)}")
            await self._handle_corruption()
            
        except Exception as e:
            logging.error(f"Critical config error: {str(e)}")
            raise
    
    async def save(self):
        """Save configuration with atomic write"""
        async with self._lock:
            temp_path = "config.tmp.json"
            try:
                # Prepare data
                data = {
                    str(guild_id): {
                        "role_id": cfg.role_id,
                        "valid_keys": list(cfg.main_store),
                        "command": cfg.command,
                        "success_msgs": cfg.success_msgs
                    }
                    for guild_id, cfg in self.guilds.items()
                }
                
                # Write to temp file first
                async with aiofiles.open(temp_path, 'w') as f:
                    await f.write(json.dumps(data, indent=4))
                
                # Atomic rename using os.replace
                import os
                os.replace(temp_path, self._config_path)
                
            except Exception as e:
                logging.error(f"Save error: {str(e)}")
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                raise
    
    async def _handle_corruption(self):
        """Handle corrupted config"""
        try:
            # Try to load backup
            async with aiofiles.open(self._backup_path, 'r') as f:
                data = json.loads(await f.read())
                
            self.guilds = {
                int(guild_id): GuildConfig(
                    cfg["role_id"],
                    set(cfg["valid_keys"]),
                    cfg.get("command", "claim"),
                    cfg.get("success_msgs", DEFAULT_SUCCESS_MESSAGES.copy())
                )
                for guild_id, cfg in data.items()
            }
            
            logging.info("Restored config from backup")
            await self.save()  # Save restored config
            
        except Exception as e:
            logging.error(f"Backup restoration failed: {str(e)}")
            self.guilds = {}  # Start fresh as last resort

config: Dict[int, GuildConfig] = {}

RESERVED_NAMES = {'sync', 'setup', 'addkey', 'addkeys', 'removekey', 'removekeys', 'clearkeys', 'keys', 'grimoire'}

class InteractionBucket:
    def get_key(self, interaction: discord.Interaction) -> int:
        return interaction.user.id

class CustomCooldown:
    def __init__(self, rate: int, per: float):
        self.cooldown = Cooldown(rate, per)
        # Use nested dict for per-guild cooldowns: {guild_id: {user_id: expiry}}
        self._cooldowns: Dict[int, Dict[int, float]] = defaultdict(dict)
        self._bucket = InteractionBucket()
        self._last_cleanup = time.time()
        self.cleanup_interval = 3600  # Cleanup every hour
    
    def _cleanup_expired(self, now: float):
        """Remove expired cooldowns periodically"""
        if now - self._last_cleanup < self.cleanup_interval:
            return
            
        for guild_id in list(self._cooldowns.keys()):
            guild_cooldowns = self._cooldowns[guild_id]
            # Remove expired user cooldowns
            expired = [
                user_id for user_id, expiry in guild_cooldowns.items()
                if now >= expiry
            ]
            for user_id in expired:
                del guild_cooldowns[user_id]
            # Remove empty guild entries
            if not guild_cooldowns:
                del self._cooldowns[guild_id]
                
        self._last_cleanup = now
    
    def get_retry_after(self, interaction: discord.Interaction) -> Optional[float]:
        # Skip cooldown for admins
        if interaction.user.guild_permissions.administrator:
            return None
            
        now = time.time()
        self._cleanup_expired(now)
        
        guild_id = interaction.guild.id
        user_id = self._bucket.get_key(interaction)
        
        guild_cooldowns = self._cooldowns[guild_id]
        if user_id in guild_cooldowns:
            if now < guild_cooldowns[user_id]:
                return guild_cooldowns[user_id] - now
            else:
                del guild_cooldowns[user_id]
        
        guild_cooldowns[user_id] = now + self.cooldown.per
        return None
    
    def reset_cooldown(self, interaction: discord.Interaction):
        """Reset cooldown for a user in a guild"""
        guild_id = interaction.guild.id
        user_id = self._bucket.get_key(interaction)
        if guild_id in self._cooldowns:
            self._cooldowns[guild_id].pop(user_id, None)
            
    def get_cooldowns_for_guild(self, guild_id: int) -> int:
        """Get number of active cooldowns for a guild"""
        now = time.time()
        if guild_id not in self._cooldowns:
            return 0
        return sum(1 for expiry in self._cooldowns[guild_id].values() if now < expiry)

claim_cooldown = CustomCooldown(1, 300)

# Replace single lock with sharded locks
SHARD_COUNT = 16
key_locks: Dict[int, List[asyncio.Lock]] = defaultdict(
    lambda: [asyncio.Lock() for _ in range(SHARD_COUNT)]
)

def get_shard(user_id: int) -> int:
    """Get shard number for user"""
    return user_id % SHARD_COUNT

async def save_config():
    try:
        async with aiofiles.open('config.json', 'w') as f:
            serialized = {
                str(guild_id): {
                    "role_id": cfg.role_id,
                    "valid_keys": list(cfg.main_store),
                    "command": cfg.command,
                    "success_msgs": cfg.success_msgs
                }
                for guild_id, cfg in config.items()
            }
            await f.write(json.dumps(serialized, indent=4))
        
        for i in range(2, -1, -1):
            src = f'config.json{"." + str(i) if i else ""}'
            dest = f'config.json.{i+1}'
            try:
                if await aiofiles.os.path.exists(src):
                    if await aiofiles.os.path.exists(dest):
                        await aiofiles.os.remove(dest)
                    await aiofiles.os.rename(src, dest)
            except Exception as e:
                logging.error(f"Backup rotation error: {str(e)}")
                
    except Exception as e:
        logging.error(f"Failed to save config: {str(e)}")
        raise

class KeyCache:
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.cache = defaultdict(lambda: {
            'lookup': defaultdict(set),
            'last_access': 0,
            'hits': 0,
            'misses': 0,
            'size': 0
        })
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        self._chunk_size = 500
    
    async def warm_cache(self, guild_id: int, guild_config: GuildConfig):
        """Warm cache with memory-efficient chunking"""
        try:
            logging.info(f"Starting cache warm for guild {guild_id}")
            start_time = time.time()
            
            # Create temporary lookup
            temp_lookup = defaultdict(set)
            hashes = list(guild_config.main_store)
            total_hashes = len(hashes)
            
            # Process in memory-efficient chunks
            for i in range(0, total_hashes, self._chunk_size):
                chunk = hashes[i:i + self._chunk_size]
                
                # Process chunk
                for full_hash in chunk:
                    if KeySecurity.DELIMITER in full_hash:
                        hash_part = full_hash.split(KeySecurity.DELIMITER)[0]
                    else:
                        hash_part = full_hash
                        
                    # Generate quick lookup hash
                    quick_hash = mmh3.hash(hash_part.encode(), signed=False)
                    temp_lookup[quick_hash].add(full_hash)
                
                # Progress logging
                progress = min(100, (i + len(chunk)) * 100 / total_hashes)
                if i % (self._chunk_size * 10) == 0:
                    logging.info(f"Cache warm {progress:.1f}% complete for guild {guild_id}")
                
                # Free memory and yield
                del chunk
                await asyncio.sleep(0)
            
            # Check cache size before updating
            new_size = sum(len(matches) for matches in temp_lookup.values())
            if new_size > self.max_size:
                logging.warning(
                    f"Cache size ({new_size}) exceeds limit ({self.max_size}) "
                    f"for guild {guild_id}"
                )
                await self._evict_entries(new_size - self.max_size)
            
            # Update cache atomically
            async with self._lock:
                self.cache[guild_id].update({
                    'lookup': temp_lookup,
                    'last_access': time.time(),
                    'size': new_size
                })
            
            duration = time.time() - start_time
            logging.info(
                f"Cache warm completed for guild {guild_id}:\n"
                f"• Hashes: {total_hashes}\n"
                f"• Lookup entries: {len(temp_lookup)}\n"
                f"• Duration: {duration:.2f}s"
            )
            
        except Exception as e:
            logging.error(f"Cache warm failed for guild {guild_id}: {str(e)}")
            await self.invalidate(guild_id)
    
    async def _evict_entries(self, needed_space: int):
        """Evict least recently used entries"""
        async with self._lock:
            # Sort guilds by last access
            sorted_guilds = sorted(
                self.cache.items(),
                key=lambda x: x[1]['last_access']
            )
            
            space_freed = 0
            for guild_id, cache_data in sorted_guilds:
                if space_freed >= needed_space:
                    break
                    
                space_freed += cache_data['size']
                del self.cache[guild_id]
                logging.info(f"Evicted cache for guild {guild_id}")
    
    async def get_matches(self, guild_id: int, key: str) -> Optional[Set[str]]:
        """Get potential matches with metrics"""
        try:
            cache_data = self.cache[guild_id]
            cache_data['last_access'] = time.time()
            
            quick_hash = mmh3.hash(key.encode(), signed=False)
            matches = cache_data['lookup'].get(quick_hash)
            
            if matches:
                cache_data['hits'] += 1
                return matches
            
            cache_data['misses'] += 1
            return None
            
        except Exception as e:
            logging.error(f"Cache lookup failed: {str(e)}")
            return None
    
    async def invalidate(self, guild_id: int):
        """Invalidate guild cache"""
        async with self._lock:
            if guild_id in self.cache:
                del self.cache[guild_id]
                logging.info(f"Invalidated cache for guild {guild_id}")
    
    def get_metrics(self, guild_id: int) -> dict:
        """Get cache metrics for a guild"""
        if guild_id not in self.cache:
            return {}
            
        cache_data = self.cache[guild_id]
        total_ops = cache_data['hits'] + cache_data['misses']
        
        return {
            'size': cache_data['size'],
            'hits': cache_data['hits'],
            'misses': cache_data['misses'],
            'hit_rate': cache_data['hits'] / total_ops if total_ops > 0 else 0,
            'last_access': cache_data['last_access']
        }

# Initialize cache
key_cache = KeyCache()

async def load_config():
    """Load configuration and warm caches"""
    global config
    try:
        async with aiofiles.open('config.json', 'r') as f:
            data = json.loads(await f.read())
            config = {
                int(guild_id): GuildConfig(
                    cfg["role_id"],
                    set(cfg["valid_keys"]),
                    cfg.get("command", "claim"),
                    cfg.get("success_msgs", DEFAULT_SUCCESS_MESSAGES.copy())
                )
                for guild_id, cfg in data.items()
            }
            
            # Warm cache for all guilds
            for guild_id, guild_config in config.items():
                await key_cache.warm_cache(guild_id, guild_config)
                
    except FileNotFoundError:
        config = {}
    except Exception as e:
        logging.error(f"Failed to load config: {str(e)}")
        config = {}

async def create_dynamic_command(command_name: str, guild_id: int):
    """Create a dynamic claim command for a guild"""
    guild = bot.get_guild(guild_id)
    if not guild:
        return

    # Remove existing command if it exists
    try:
        existing = bot.tree.get_command(command_name, guild=guild)
        if existing:
            bot.tree.remove_command(command_name, guild=guild)
    except:
        pass

    @app_commands.command(name=command_name, description="✨ Claim your role with a mystical key")
    async def dynamic_claim(interaction: discord.Interaction):
        """Dynamic claim command"""
        if interaction.guild_id != guild_id:
            return
        await interaction.response.send_modal(ArcaneGatewayModal())

    bot.tree.add_command(dynamic_claim, guild=guild)
    await bot.tree.sync(guild=guild)

class CommandSync:
    def __init__(self, bot):
        self.bot = bot
        
    async def sync_all(self):
        """Sync commands to all guilds"""
        try:
            await self.bot.tree.sync()
            logging.info("Synced commands globally")
        except Exception as e:
            logging.error(f"Command sync error: {str(e)}")

class RealmKeeper(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.worker_pool = AdaptiveWorkerPool(
            min_workers=4,
            max_workers=16
        )
        self.key_security = KeySecurity()
        self.monitor_task = None
        self.command_sync = CommandSync(self.bot)

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        if self.worker_pool:
            self.worker_pool.shutdown()
        if self.monitor_task:
            self.monitor_task.cancel()
    
    @commands.Cog.listener()
    async def on_ready(self):
        """Initialize bot systems with monitoring"""
        try:
            logging.info("🔄 Starting bot systems...")
            
            # Load config first
            await load_config()
            
            # Start background tasks
            cleanup_task.start()
            save_stats_task.start()
            memory_check.start()
            monitor_workers.start()
            
            # Pre-warm systems
            logging.info("⚡ Pre-warming crypto...")
            fake_key = str(uuid.uuid4())
            for _ in range(4):
                await asyncio.get_event_loop().run_in_executor(
                    self.worker_pool.pool,  # Use the actual ThreadPoolExecutor
                    self.key_security.hash_key,
                    fake_key
                )
            
            # Pre-warm connection pool
            logging.info("🌐 Pre-warming connections...")
            async with self.bot.session.get(
                "https://discord.com/api/v9/gateway",
                timeout=HTTP_TIMEOUT
            ) as resp:
                await resp.read()
            
            # Pre-warm caches
            logging.info("⚡ Pre-warming caches...")
            for guild_id, guild_config in config.items():
                await key_cache.warm_cache(guild_id, guild_config)
            
            # Restore dynamic commands
            restored = 0
            for guild_id, guild_config in config.items():
                try:
                    await create_dynamic_command(guild_config.command, guild_id)
                    restored += 1
                except Exception as e:
                    logging.error(f"Failed to restore command for guild {guild_id}: {e}")
            
            logging.info(f"Restored {restored} custom commands")
            
            # Initialize worker pools
            logging.info("👷 Starting worker pools...")
            # self.worker_pool.start()
            
            # Sync commands
            await self.command_sync.sync_all()
            
            # Update presence
            await self.bot.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name="⚡ for magical keys"
                )
            )
            
            # Log ready state
            guild_count = len(self.bot.guilds)
            key_count = sum(len(cfg.main_store) for cfg in config.values())
            logging.info(
                f"✅ Bot ready!\n"
                f"• Guilds: {guild_count}\n"
                f"• Total keys: {key_count}\n"
                f"• Workers: {len(self.key_security.worker_pool._threads)}\n"
                f"• Connections: {len(self.bot.http._HTTPClient__session.connector._conns)}\n"
                f"• Commands restored: {restored}"
            )
            
        except Exception as e:
            logging.error(f"Startup error: {str(e)}")
            # Continue with basic functionality

    @commands.Cog.listener()
    async def on_error(self, event, *args, **kwargs):
        """Handle connection errors gracefully"""
        error = sys.exc_info()[1]
        if isinstance(error, (HTTPException, GatewayNotFound)):
            logging.error(f"Discord API error in {event}: {str(error)}")
            await backoff.expo(
                self.bot.connect,
                max_tries=MAX_RETRIES,
                max_time=60
            )
        else:
            logging.error(f"Error in {event}: {str(error)}")

    @commands.Cog.listener()
    async def on_connect(self):
        """Log successful connections"""
        logging.info(f"Connected to Discord API with {self.bot.shard_count} shards")
        logging.info(f"Active connections: {len(self.bot.http._HTTPClient__session.connector._conns)}")

    @app_commands.command(name="addkey", description="🔑 Add single key")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def addkey(self, interaction: discord.Interaction, key: str):
        """Add a single key"""
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        try:
            uuid_obj = uuid.UUID(key, version=4)
            if str(uuid_obj) != key.lower():
                raise ValueError("Not a valid UUIDv4")
        except ValueError:
            await interaction.response.send_message("❌ Invalid UUID format! Use UUIDv4.", ephemeral=True)
            return

        expiry_seconds = None
        if any(KeySecurity.verify_key(key, h)[0] for h in guild_config.main_store):
            await interaction.response.send_message("❌ Key exists!", ephemeral=True)
            return

        hashed = KeySecurity.hash_key(key, expiry_seconds)
        await guild_config.add_key(hashed, guild_id)
        await save_config()
        stats.log_keys_added(guild_id, 1)
        await audit.log_key_add(interaction, 1)
        
        msg = "✅ Key added!"
        if expiry_seconds:
            msg += f"\n• Expires in: {expiry_seconds} seconds"
        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="addkeys", description="🔑 Bulk add keys")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def addkeys(self, interaction: discord.Interaction):
        """Bulk add keys"""
        await interaction.response.send_modal(BulkKeyModal())

    @app_commands.command(name="setup", description="⚙️ Initial setup")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def setup(self, interaction: discord.Interaction):
        """Initial server setup"""
        await interaction.response.send_modal(SetupModal())

    @app_commands.command(name="keys", description="📊 View key statistics")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def keys(self, interaction: discord.Interaction):
        """View key statistics for the server"""
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return
        
        # Get current key stats
        total_keys = len(guild_config.main_store)
        expired = sum(1 for h in guild_config.main_store 
                     if KeySecurity.DELIMITER in h and 
                     json.loads(h.split(KeySecurity.DELIMITER)[1]).get('exp', 0) < time.time())
        
        # Get usage stats
        stats_data = stats.get_stats(guild_id)
        
        embed = discord.Embed(
            title="🔑 Key Statistics",
            color=discord.Color.blue()
        )
        
        # Key stats
        keys_info = (
            f"• Total: {total_keys}\n"
            f"• Expired: {expired}\n"
            f"• Added: {stats_data['keys_added']}\n"
            f"• Removed: {stats_data['keys_removed']}"
        )
        embed.add_field(name="Keys", value=keys_info, inline=False)
        
        # Claim stats
        claims_info = (
            f"• Total: {stats_data['total_claims']}\n"
            f"• Successful: {stats_data['successful_claims']}\n"
            f"• Failed: {stats_data['failed_claims']}"
        )
        embed.add_field(name="Claims", value=claims_info, inline=False)
        
        # Timing stats
        if stats_data['successful_claims'] > 0:
            timing_info = (
                f"• Average: {stats_data['timing']['average']}\n"
                f"• Fastest: {stats_data['timing']['fastest']}\n"
                f"• Slowest: {stats_data['timing']['slowest']}"
            )
            embed.add_field(name="Timing", value=timing_info, inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="clearkeys", description="🗑️ Remove all keys")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def clearkeys(self, interaction: discord.Interaction):
        """Remove all keys from the server"""
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return
        
        key_count = len(guild_config.main_store)
        guild_config.main_store.clear()
        await key_cache.invalidate(guild_id)
        await save_config()
        
        # Reset stats and save them
        stats.reset_guild_stats(guild_id)
        await stats.save_stats()
        
        await interaction.response.send_message(
            f"✅ Cleared {key_count} keys!",
            ephemeral=True
        )

    @app_commands.command(name="removekey", description="🗑️ Remove a specific key")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def removekey(self, interaction: discord.Interaction, key: str):
        """Remove a specific key"""
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        # Validate UUID format
        try:
            uuid_obj = uuid.UUID(key, version=4)
            if str(uuid_obj) != key.lower():
                raise ValueError("Not a valid UUIDv4")
        except ValueError:
            await interaction.response.send_message("❌ Invalid UUID format!", ephemeral=True)
            return

        # Find and remove key
        removed = False
        for full_hash in list(guild_config.main_store):
            if KeySecurity.verify_key(key, full_hash)[0]:
                await guild_config.remove_key(full_hash, guild_id)
                removed = True
                break

        if removed:
            await save_config()
            await interaction.response.send_message("✅ Key removed!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Key not found!", ephemeral=True)

    @app_commands.command(name="removekeys", description="🗑️ Remove multiple keys")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def removekeys(self, interaction: discord.Interaction):
        """Remove multiple keys"""
        await interaction.response.send_modal(RemoveKeysModal())

    @app_commands.command(name="sync", description="🔄 Force sync commands")
    @app_commands.default_permissions(administrator=True)
    @admin_cooldown()
    async def sync_guild_commands(self, interaction: discord.Interaction):
        """Force sync commands with this guild"""
        try:
            await interaction.response.defer(ephemeral=True)
            bot.tree.copy_global_to(guild=interaction.guild)
            await bot.tree.sync(guild=interaction.guild)
            await interaction.followup.send("✅ Commands synced!", ephemeral=True)
        except Exception as e:
            logging.error(f"Sync error: {str(e)}")
            await interaction.followup.send("❌ Sync failed!", ephemeral=True)

    @app_commands.command(name="grimoire", description="📚 View command documentation")
    @admin_cooldown()
    async def grimoire(self, interaction: discord.Interaction):
        """View detailed command documentation"""
        embed = discord.Embed(
            title="📚 Realm Keeper's Grimoire",
            description="A guide to the mystical arts",
            color=discord.Color.purple()
        )
        
        # Admin commands with aliases
        admin_cmds = []
        for cmd_name in [
            'setup', 'addkey', 'addkeys', 'removekey', 'removekeys',
            'clearkeys', 'keys', 'sync', 'metrics'
        ]:
            aliases = COMMAND_ALIASES.get(cmd_name, [])
            cmd_str = f"`/{cmd_name}`"
            if aliases:
                cmd_str += f" (aliases: {', '.join(f'`/{a}`' for a in aliases)})"
            admin_cmds.append(cmd_str)
        
        admin = "\n".join(admin_cmds)
        embed.add_field(name="🔧 Admin Commands", value=admin, inline=False)
        
        # User commands
        if (guild_config := config.get(interaction.guild.id)):
            user = f"`/{guild_config.command}` - Claim your role with a key"
            embed.add_field(name="✨ User Commands", value=user, inline=False)
        
        # Usage examples
        examples = (
            "• `/setup` - First time setup\n"
            "• `/addkey <uuid> [expires_in]` - Add key with optional expiry\n"
            "• `/addkeys` - Bulk add keys\n"
            f"• `/{config.get(interaction.guild.id, GuildConfig(0, set())).command} <key>` - Claim role"
        )
        embed.add_field(name="📝 Examples", value=examples, inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

class RemoveKeysModal(discord.ui.Modal, title="Remove Multiple Keys"):
    
    keys = discord.ui.TextInput(
        label="Enter keys to remove (one per line)",
        style=discord.TextStyle.long,
        placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
        required=True,
        max_length=2000
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            guild_id = interaction.guild.id
            if (guild_config := config.get(guild_id)) is None:
                await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

            # Validate all keys first
            key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
            for key in key_list:
                try:
                    uuid_obj = uuid.UUID(key, version=4)
                    if str(uuid_obj) != key.lower():
                        raise ValueError()
                except ValueError:
                    await interaction.response.send_message(
                        f"❌ Invalid UUID format: {key[:8]}...",
                        ephemeral=True
                    )
            return

            # Remove valid keys
            removed = 0
            for key in key_list:
                for full_hash in list(guild_config.main_store):
                    if KeySecurity.verify_key(key, full_hash)[0]:
                        await guild_config.remove_key(full_hash, guild_id)
                        removed += 1
                        break

            await save_config()
            await interaction.response.send_message(
                f"✅ Removed {removed} keys!\n• Not found: {len(key_list)-removed}",
                ephemeral=True
            )

        except Exception as e:
            logging.error(f"Key removal error: {str(e)}")
        await interaction.response.send_message(
                "❌ Failed to remove keys!",
            ephemeral=True
        )

@tasks.loop(hours=1)
async def cleanup_task():
    """Cleanup expired keys periodically"""
    try:
        logging.info("Starting scheduled key cleanup")
        for guild_id, guild_config in config.items():
            expired = set()
            invalid = set()
            
            # Check each key
            for full_hash in guild_config.main_store:
                if KeySecurity.DELIMITER in full_hash:
                    try:
                        _, meta = full_hash.split(KeySecurity.DELIMITER, 1)
                        meta_data = json.loads(meta)
                        
                        # Check expiration
                        if meta_data.get('exp', float('inf')) < time.time():
                            expired.add(full_hash)
                            
                    except json.JSONDecodeError:
                        invalid.add(full_hash)
            
            # Remove expired and invalid keys
            if expired or invalid:
                guild_config.main_store -= (expired | invalid)
                await key_cache.invalidate(guild_id)
                
    except Exception as e:
        logging.error(f"Cleanup error: {str(e)}")

@cleanup_task.before_loop
async def before_cleanup():
    """Wait for bot to be ready before starting cleanup"""
    await bot.wait_until_ready()

class KeySecurity:
    DELIMITER = "||"
    HASH_PREFIX_LENGTH = 7
    HASH_ROUNDS = 10
    
    def __init__(self):
        self.salt = self._load_or_generate_salt()
        self.worker_pool = ThreadPoolExecutor(
            max_workers=max(8, os.cpu_count() * 2),
            thread_name_prefix="key-verify"
        )
        self.metrics = defaultdict(lambda: {
            'hashes': 0,
            'verifications': 0,
            'failures': 0,
            'avg_verify_time': 0.0
        })
    
    def _load_or_generate_salt(self) -> bytes:
        """Load or generate salt with proper error handling"""
        try:
            # Try to load from environment
            if salt := os.getenv('HASH_SALT'):
                return base64.b64decode(salt)
            
            # Generate new salt
            new_salt = secrets.token_bytes(16)
            encoded = base64.b64encode(new_salt).decode()
            
            # Save to .env file
            with open('.env', 'a') as f:
                f.write(f"\nHASH_SALT={encoded}")
            
            logging.warning("Generated new HASH_SALT")
            return new_salt
            
        except Exception as e:
            logging.error(f"Salt initialization error: {str(e)}")
            # Use fallback salt in emergency
            return secrets.token_bytes(16)
    
    def hash_key(self, key: str, expiry_seconds: Optional[int] = None) -> str:
        """Hash a key with metadata"""
        try:
            # Prepare metadata
            meta = {}
            if expiry_seconds:
                meta['exp'] = time.time() + expiry_seconds
            
            # Add salt and hash
            salted_key = f"{key}{self.salt.hex()}"
            hash_str = bcrypt_sha256.using(rounds=self.HASH_ROUNDS).hash(salted_key)
            
            # Add metadata if needed
            if meta:
                return f"{hash_str}{self.DELIMITER}{json.dumps(meta)}"
            return hash_str
            
        except Exception as e:
            logging.error(f"Hash error: {str(e)}")
            raise

class KeyCleanup:
    def __init__(self, bot):
        self.bot = bot
        self.cleanup_task = None
        self.stats = defaultdict(lambda: {
            'expired': 0,
            'invalid': 0,
            'last_run': 0,
            'duration': 0
        })
        self._lock = asyncio.Lock()
    
    async def start(self):
        """Start cleanup task"""
        if not self.cleanup_task:
            self.cleanup_task = self.bot.loop.create_task(self._cleanup_loop())
            logging.info("Started key cleanup task")
    
    async def stop(self):
        """Stop cleanup task"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
            self.cleanup_task = None
    
    async def _cleanup_loop(self):
        """Main cleanup loop"""
        while True:
            try:
                await self._cleanup_expired_keys()
                await asyncio.sleep(3600)  # Run every hour
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Cleanup error: {str(e)}")
                await asyncio.sleep(300)  # Retry after 5 minutes on error
    
    async def _cleanup_expired_keys(self):
        """Clean expired and invalid keys"""
        start_time = time.time()
        logging.info("Starting key cleanup")
        
        async with self._lock:
            for guild_id, guild_config in self.bot.config.guilds.items():
                expired = set()
                invalid = set()
                
                # Check each key
                for full_hash in guild_config.main_store:
                    if KeySecurity.DELIMITER in full_hash:
                        try:
                            hash_part, meta = full_hash.split(KeySecurity.DELIMITER, 1)
                            meta_data = json.loads(meta)
                            
                            # Check expiration
                            if meta_data.get('exp', float('inf')) < time.time():
                                expired.add(full_hash)
                                continue
                                
                            # Check uses
                            if meta_data.get('uses', 1) <= 0:
                                expired.add(full_hash)
                                continue
                                
                        except (json.JSONDecodeError, ValueError):
                            invalid.add(full_hash)
                
                # Remove expired and invalid keys
                if expired or invalid:
                    guild_config.main_store -= (expired | invalid)
                    guild_config._rebuild_bloom()  # Update Bloom filter
                    await key_cache.invalidate(guild_id)
                    
                    # Update stats
                    self.stats[guild_id]['expired'] += len(expired)
                    self.stats[guild_id]['invalid'] += len(invalid)
                    self.stats[guild_id]['last_run'] = time.time()
                    
                    logging.info(
                        f"Guild {guild_id} cleanup:\n"
                        f"• Expired: {len(expired)}\n"
                        f"• Invalid: {len(invalid)}\n"
                        f"• Remaining: {len(guild_config.main_store)}"
                    )
            
            # Save changes
            await self.bot.config.save()
        
        # Update duration stat
        duration = time.time() - start_time
        for stats in self.stats.values():
            stats['duration'] = duration
        
        logging.info(f"Cleanup completed in {duration:.2f}s")
    
    def get_stats(self, guild_id: int) -> dict:
        """Get cleanup stats for a guild"""
        return self.stats[guild_id]

class KeyLocks:
    def __init__(self, shards: int = 64):
        """Initialize lock manager with sharding"""
        self.shards = shards
        self.locks = defaultdict(lambda: {
            shard: asyncio.Lock() for shard in range(shards)
        })
        self.global_locks = defaultdict(asyncio.Lock)
        self._cleanup_task = None
        self._active_locks = defaultdict(int)
    
    def get_shard(self, key: str) -> int:
        """Get shard for a key"""
        return mmh3.hash(key.encode(), signed=False) % self.shards
    
    async def acquire(self, guild_id: int, key: str) -> bool:
        """Acquire lock for key verification"""
        try:
            shard = self.get_shard(key)
            self._active_locks[guild_id] += 1
            await self.locks[guild_id][shard].acquire()
            return True
        except Exception as e:
            logging.error(f"Lock acquisition failed: {str(e)}")
            return False
    
    async def release(self, guild_id: int, key: str):
        """Release lock after verification"""
        try:
            shard = self.get_shard(key)
            self.locks[guild_id][shard].release()
            self._active_locks[guild_id] -= 1
        except Exception as e:
            logging.error(f"Lock release failed: {str(e)}")
    
    async def acquire_global(self, guild_id: int) -> bool:
        """Acquire global lock for guild operations"""
        try:
            await self.global_locks[guild_id].acquire()
            return True
        except Exception as e:
            logging.error(f"Global lock acquisition failed: {str(e)}")
            return False
    
    async def release_global(self, guild_id: int):
        """Release global lock"""
        try:
            self.global_locks[guild_id].release()
        except Exception as e:
            logging.error(f"Global lock release failed: {str(e)}")
    
    def get_metrics(self, guild_id: int) -> dict:
        """Get lock metrics for a guild"""
        return {
            'active_locks': self._active_locks[guild_id],
            'shard_count': self.shards,
            'global_locked': self.global_locks[guild_id].locked()
        }

# Initialize lock manager
key_locks = KeyLocks()

class ArcaneGatewayModal(discord.ui.Modal, title="Enter Mystical Key"):
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Validate key format first
            is_valid, error = await self.bot.key_validator.validate_key(
                self.key.value.strip(),
                interaction.guild_id
            )
            
            if not is_valid:
                await interaction.response.send_message(
                    f"❌ {error}",
                    ephemeral=True
                )
                return
                
            # Continue with key verification...
            
            # Acquire lock for key verification
            if not await key_locks.acquire(guild_id, key_value):
                await progress_msg.edit(content="❌ System busy, please try again!")
                return
                
            try:
                # Verify key
                is_valid, updated_hash = await key_security.verify_keys_batch(
                    key_value, 
                    possible_hashes,
                    chunk_size=20
                )
                
                if is_valid:
                    # Update key storage under lock
                    for full_hash in possible_hashes:
                        if await key_security.verify_key(key_value, full_hash)[0]:
                            await guild_config.remove_key(full_hash, guild_id)
                            if updated_hash:
                                await guild_config.add_key(updated_hash, guild_id)
                            break
                    
                    await save_config()
                    
                    # Grant role and send success message
                    role = interaction.guild.get_role(guild_config.role_id)
                    await interaction.user.add_roles(role)
                    
                    success_msg = random.choice(guild_config.success_msgs)
                    await progress_msg.edit(content=success_msg.format(
                        user=interaction.user.mention,
                        role=role.mention
                    ))
                else:
                    await progress_msg.edit(content="❌ Invalid key!")
                    
            finally:
                # Always release lock
                await key_locks.release(guild_id, key_value)
                
        except Exception as e:
            logging.error(f"Claim error: {str(e)}")
            await progress_msg.edit(content="❌ An error occurred!")

class KeyValidator:
    def __init__(self, bot):
        self.bot = bot
        self.worker_pool = ThreadPoolExecutor(max_workers=4)
        self.validation_stats = defaultdict(lambda: {
            'total': 0,
            'valid': 0,
            'invalid': 0,
            'errors': 0
        })
    
    async def validate_key(self, key: str, guild_id: int) -> tuple[bool, Optional[str]]:
        """Validate key format and update stats"""
        try:
            self.validation_stats[guild_id]['total'] += 1
            
            # Basic format check
            if not isinstance(key, str) or not key:
                self.validation_stats[guild_id]['invalid'] += 1
                return False, "Key must be a non-empty string"
            
            # Length check
            if len(key) != 36:  # Standard UUID length
                self.validation_stats[guild_id]['invalid'] += 1
                return False, "Invalid key length"
            
            # Async UUID validation
            try:
                is_valid = await self.bot.loop.run_in_executor(
                    self.worker_pool,
                    self._validate_uuid,
                    key
                )
                
                if is_valid:
                    self.validation_stats[guild_id]['valid'] += 1
                    return True, None
                else:
                    self.validation_stats[guild_id]['invalid'] += 1
                    return False, "Invalid key format"
                    
            except Exception as e:
                self.validation_stats[guild_id]['errors'] += 1
                logging.error(f"UUID validation error: {str(e)}")
                return False, "Validation error"
                
        except Exception as e:
            self.validation_stats[guild_id]['errors'] += 1
            logging.error(f"Key validation error: {str(e)}")
            return False, "Internal error"
    
    def _validate_uuid(self, key: str) -> bool:
        """Synchronous UUID validation"""
        try:
            return str(uuid.UUID(key, version=4)) == key.lower()
        except ValueError:
            return False
    
    def get_stats(self, guild_id: int) -> dict:
        """Get validation stats for a guild"""
        return self.validation_stats[guild_id]

@tasks.loop(minutes=5)
async def save_stats_task():
    """Save bot statistics periodically"""
    try:
        stats = {
            'system': {
                'memory': psutil.Process().memory_info().rss,
                'cpu': psutil.cpu_percent(),
                'uptime': time.time() - start_time
            },
            'discord': {
                'latency': round(bot.latency * 1000, 2),
                'guilds': len(bot.guilds),
                'users': sum(g.member_count for g in bot.guilds)
            },
            'keys': {
                'total': sum(len(cfg.main_store) for cfg in bot.config.guilds.values()),
                'guilds': len(bot.config.guilds)
            }
        }
        
        async with aiofiles.open('stats.json', 'w') as f:
            await f.write(json.dumps(stats, indent=4))
            
    except Exception as e:
        logging.error(f"Stats save error: {str(e)}")

@tasks.loop(minutes=15)
async def memory_check():
    """Monitor memory usage and cleanup if needed"""
    try:
        memory = psutil.Process().memory_info()
        if memory.rss > 1024 * 1024 * 1024:  # 1GB
            logging.warning("High memory usage, running garbage collection")
            import gc
            gc.collect()
    except Exception as e:
        logging.error(f"Memory check error: {str(e)}")

@tasks.loop(minutes=5)
async def monitor_workers():
    """Monitor and adjust worker pools"""
    try:
        for guild_id, guild_config in config.items():
            queue_size = worker_pool.pool._work_queue.qsize()
            active_workers = len(worker_pool.pool._threads)
            
            if queue_size > 50:
                worker_pool.scale_up()
            elif queue_size < 10:
                worker_pool.scale_down()
                
            logging.info(
                f"Worker pool stats for {guild_id}:\n"
                f"• Queue size: {queue_size}\n"
                f"• Active workers: {active_workers}"
            )
    except Exception as e:
        logging.error(f"Worker monitor error: {str(e)}")

# Add before_loop for each task
@cleanup_task.before_loop
@save_stats_task.before_loop
@memory_check.before_loop
@monitor_workers.before_loop
async def before_task():
    """Wait for bot to be ready before starting tasks"""
    await bot.wait_until_ready()

# Start time for uptime tracking
start_time = time.time()

# Add near other modal classes
class SetupModal(discord.ui.Modal, title="Realm Setup"):
    role = discord.ui.TextInput(
        label="Role ID to grant (right-click role -> Copy ID)",
        placeholder="Enter role ID...",
        min_length=17,
        max_length=20
    )
    
    command = discord.ui.TextInput(
        label="Command name (without /)",
        placeholder="claim",
        default="claim",
        min_length=1,
        max_length=32
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Validate role ID
            try:
                role_id = int(self.role.value)
                if not (role := interaction.guild.get_role(role_id)):
                    await interaction.response.send_message(
                        "❌ Invalid role ID!", 
                        ephemeral=True
                    )
                    return
            except ValueError:
                await interaction.response.send_message(
                    "❌ Role ID must be a number!", 
                    ephemeral=True
                )
                return
            
            # Create guild config
            guild_id = interaction.guild.id
            bot.config.guilds[guild_id] = GuildConfig(
                role_id=role_id,
                valid_keys=set(),
                command=self.command.value.lower()
            )
            
            # Save config
            await bot.config.save()
            
            # Create dynamic command
            await create_dynamic_command(self.command.value.lower(), guild_id)
            
            await interaction.response.send_message(
                f"✅ Setup complete!\n"
                f"• Role: {role.mention}\n"
                f"• Command: /{self.command.value}\n"
                f"• Add keys with /addkey or /addkeys",
                ephemeral=True
            )
            
        except Exception as e:
            logging.error(f"Setup error: {str(e)}")
            await interaction.response.send_message(
                "❌ An error occurred during setup!",
                ephemeral=True
            )

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    # Run main
    asyncio.run(main())