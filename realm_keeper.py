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

# Initialize worker pool
class AdaptiveWorkerPool:
    def __init__(self, min_workers: int = 4, max_workers: int = 32):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.pool = ThreadPoolExecutor(
            max_workers=min_workers,
            thread_name_prefix="worker"
        )
        
    def scale_up(self):
        """Increase worker count"""
        current = len(self.pool._threads)
        if current < self.max_workers:
            new_size = min(current + 2, self.max_workers)
            self.pool._max_workers = new_size
            
    def scale_down(self):
        """Decrease worker count"""
        current = len(self.pool._threads)
        if current > self.min_workers:
            new_size = max(current - 1, self.min_workers)
            self.pool._max_workers = new_size

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
        self.connector = None
        self.realm_keeper = None
        self.config = Config()
        self.key_cleanup = KeyCleanup(self)
        self.command_sync = CommandSync(self)
        self.key_validator = KeyValidator(self)
    
    async def setup_hook(self):
        """Initialize bot systems"""
        await self.config.load()
        await self.key_cleanup.start()
        await self.command_sync.sync_all()
        
        # Create connector in async context
        self.connector = TCPConnector(
            limit=MAX_CONNECTIONS,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )
        self.http._HTTPClient__session._connector = self.connector
        
        # Add realm keeper cog
        self.realm_keeper = RealmKeeper(self)
        await self.add_cog(self.realm_keeper)
    
    async def close(self):
        """Cleanup on shutdown"""
        await self.key_cleanup.stop()
        if self.connector:
            await self.connector.close()
        await super().close()

async def main():
    """Main entry point"""
    async with RealmBot() as bot:
        await bot.start(TOKEN)

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
        
    async def load(self):
        """Load configuration with error handling and backup"""
        try:
            async with aiofiles.open('config.json', 'r') as f:
                data = json.loads(await f.read())
                
            # Create backup before processing
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
            
            # Warm caches with error handling
            warm_tasks = []
            for guild_id, guild_config in self.guilds.items():
                task = asyncio.create_task(
                    key_cache.warm_cache(guild_id, guild_config),
                    name=f"warm-{guild_id}"
                )
                task.add_done_callback(
                    lambda t: logging.error(f"Cache warm failed: {t.exception()}") 
                    if t.exception() else None
                )
                warm_tasks.append(task)
            
            await asyncio.gather(*warm_tasks, return_exceptions=True)
                
        except FileNotFoundError:
            logging.warning("Config file not found, starting fresh")
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
                
                # Atomic rename
                await aiofiles.os.rename(temp_path, 'config.json')
                
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
        self.sync_lock = asyncio.Lock()
        self.last_sync = defaultdict(float)
        self.sync_stats = defaultdict(lambda: {
            'success': 0,
            'failures': 0,
            'rate_limits': 0,
            'last_error': None
        })
        
        # Rate limit settings
        self.GUILD_BATCH_SIZE = 5
        self.BATCH_DELAY = 1.0
        self.MAX_RETRIES = 3
        self.BASE_RETRY_DELAY = 2.0
    
    async def sync_all(self):
        """Sync commands to all guilds with rate limiting"""
        async with self.sync_lock:
            try:
                # Sync global commands first
                global_commands = await self.bot.tree.sync()
                logging.info(f"Synced {len(global_commands)} global commands")
                
                # Group guilds into batches
                guilds = list(self.bot.guilds)
                batches = [
                    guilds[i:i + self.GUILD_BATCH_SIZE] 
                    for i in range(0, len(guilds), self.GUILD_BATCH_SIZE)
                ]
                
                # Process batches with rate limiting
                for batch in batches:
                    tasks = [
                        self._sync_guild(guild) 
                        for guild in batch
                    ]
                    
                    # Wait for batch to complete
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Handle results
                    for guild, result in zip(batch, results):
                        if isinstance(result, Exception):
                            self.sync_stats[guild.id]['failures'] += 1
                            self.sync_stats[guild.id]['last_error'] = str(result)
                            logging.error(f"Sync failed for {guild.name}: {result}")
                        else:
                            self.sync_stats[guild.id]['success'] += 1
                    
                    # Rate limit delay between batches
                    await asyncio.sleep(self.BATCH_DELAY)
                
                # Log completion
                total_guilds = len(guilds)
                success = sum(s['success'] for s in self.sync_stats.values())
                logging.info(
                    f"Command sync complete:\n"
                    f"• Total guilds: {total_guilds}\n"
                    f"• Successful: {success}\n"
                    f"• Failed: {total_guilds - success}"
                )
                
            except Exception as e:
                logging.error(f"Critical sync error: {str(e)}")
                raise
    
    async def _sync_guild(self, guild: discord.Guild) -> None:
        """Sync commands to a single guild with retries"""
        for attempt in range(self.MAX_RETRIES):
            try:
                # Check rate limit cooldown
                if time.time() - self.last_sync[guild.id] < self.BATCH_DELAY:
                    await asyncio.sleep(self.BATCH_DELAY)
                
                # Copy and sync commands
                self.bot.tree.copy_global_to(guild=guild)
                await self.bot.tree.sync(guild=guild)
                
                # Update timestamp
                self.last_sync[guild.id] = time.time()
                logging.info(f"Synced commands with {guild.name}")
                return
                
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    self.sync_stats[guild.id]['rate_limits'] += 1
                    retry_after = e.retry_after or self.BASE_RETRY_DELAY * (attempt + 1)
                    logging.warning(f"Rate limited for {guild.name}, retry in {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                raise
            
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.BASE_RETRY_DELAY * (attempt + 1)
                    logging.warning(f"Sync attempt {attempt + 1} failed for {guild.name}, retry in {delay}s")
                    await asyncio.sleep(delay)
                    continue
                raise
    
    def get_stats(self, guild_id: int) -> dict:
        """Get sync stats for a guild"""
        return self.sync_stats[guild_id]

def admin_cooldown():
    """Custom cooldown for admin commands"""
    async def predicate(interaction: discord.Interaction):
        if interaction.user.guild_permissions.administrator:
            return True
        if not hasattr(interaction.command, '_buckets'):
            interaction.command._buckets = commands.CooldownMapping.from_cooldown(
                1, 300, commands.BucketType.user
            )
        bucket = interaction.command._buckets.get_bucket(interaction)
        retry_after = bucket.update_rate_limit()
        if retry_after:
            raise commands.CommandOnCooldown(
                bucket, retry_after, commands.BucketType.user
            )
        return True
    return app_commands.check(predicate)

class RealmKeeper(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.worker_pool = AdaptiveWorkerPool()
        self.key_security = KeySecurity()
        self.monitor_task = None
        self.command_sync = CommandSync(self.bot)

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
                    self.key_security.worker_pool,
                    KeySecurity.hash_key,
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
            self.worker_pool.start()
            
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

@tasks.loop(seconds=30)
async def monitor_performance():
    """Monitor bot performance metrics"""
    try:
        # System metrics
        cpu_percent = psutil.cpu_percent()
        memory = psutil.Process().memory_info()
        
        # Discord metrics
        latency = round(bot.latency * 1000, 2)
        event_loop = asyncio.get_event_loop()
        pending_tasks = len([t for t in asyncio.all_tasks(event_loop) if not t.done()])
        
        # Worker metrics
        queue_size = worker_pool.pool._work_queue.qsize()
        active_workers = len(worker_pool.pool._threads)
        
        logging.info(
            f"Performance Metrics:\n"
            f"• System:\n"
            f"  - CPU: {cpu_percent}%\n"
            f"  - Memory: {memory.rss / 1024 / 1024:.1f}MB\n"
            f"• Discord:\n"
            f"  - Latency: {latency}ms\n"
            f"  - Pending Tasks: {pending_tasks}\n"
            f"• Workers:\n"
            f"  - Queue Size: {queue_size}\n"
            f"  - Active Workers: {active_workers}"
        )
        
        # Scale workers if needed
        if queue_size > 50 and cpu_percent < 90:
            worker_pool.scale_up()
        elif queue_size < 10:
            worker_pool.scale_down()
            
    except Exception as e:
        logging.error(f"Monitoring error: {str(e)}")

# Start monitoring when bot is ready
@monitor_performance.before_loop
async def before_monitor():
    await bot.wait_until_ready()

class KeySecurity:
    DELIMITER = "||"
    HASH_PREFIX_LENGTH = 7
    HASH_ROUNDS = 10  # Reduced bcrypt rounds for better performance
    
    def __init__(self):
        """Initialize security with environment configuration"""
        # Load salt from environment or generate
        self.salt = self._load_or_generate_salt()
        
        # Configure worker pool
        self.worker_pool = ThreadPoolExecutor(
            max_workers=max(8, os.cpu_count() * 2),
            thread_name_prefix="key-verify"
        )
        
        # Track security metrics
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
    
    async def hash_key(self, key: str, expiry_seconds: Optional[int] = None, 
                      max_uses: Optional[int] = None, guild_id: int = 0) -> str:
        """Hash a key with metadata and salt"""
        try:
            # Prepare metadata
            meta = {}
            if expiry_seconds:
                meta['exp'] = time.time() + expiry_seconds
            if max_uses:
                meta['uses'] = max_uses
            
            # Add salt and hash
            salted_key = f"{key}{self.salt.hex()}"
            hash_str = await self.bot.loop.run_in_executor(
                self.worker_pool,
                lambda: bcrypt_sha256.using(rounds=self.HASH_ROUNDS).hash(salted_key)
            )
            
            # Update metrics
            self.metrics[guild_id]['hashes'] += 1
            
            # Add metadata if needed
            if meta:
                return f"{hash_str}{self.DELIMITER}{json.dumps(meta)}"
            return hash_str
            
        except Exception as e:
            logging.error(f"Hash error: {str(e)}")
            raise
    
    async def verify_key(self, key: str, full_hash: str, guild_id: int = 0) -> tuple[bool, Optional[str]]:
        """Verify a key with metrics"""
        start_time = time.time()
        try:
            # Split hash and metadata
            if self.DELIMITER in full_hash:
                hash_part, meta_part = full_hash.split(self.DELIMITER, 1)
                try:
                    metadata = json.loads(meta_part)
                except json.JSONDecodeError:
                    self.metrics[guild_id]['failures'] += 1
                    return False, None
                
                # Check expiry first
                if metadata.get('exp', float('inf')) < time.time():
                    self.metrics[guild_id]['failures'] += 1
                    return False, None
                
                # Check uses
                uses = metadata.get('uses', 1)
                if uses <= 0:
                    self.metrics[guild_id]['failures'] += 1
                    return False, None
                
                # Update uses if valid
                if uses > 0:
                    metadata['uses'] = uses - 1
            else:
                hash_part = full_hash
                metadata = {}
            
            # Add salt and verify
            salted_key = f"{key}{self.salt.hex()}"
            is_valid = await self.bot.loop.run_in_executor(
                self.worker_pool,
                lambda: bcrypt_sha256.verify(salted_key, hash_part)
            )
            
            # Update metrics
            verify_time = time.time() - start_time
            self.metrics[guild_id]['verifications'] += 1
            if not is_valid:
                self.metrics[guild_id]['failures'] += 1
            
            # Update average verification time
            m = self.metrics[guild_id]
            m['avg_verify_time'] = (
                (m['avg_verify_time'] * (m['verifications'] - 1) + verify_time) / 
                m['verifications']
            )
            
            # Return result with updated metadata if needed
            if is_valid and metadata.get('uses', 1) > 0:
                return True, f"{hash_part}{self.DELIMITER}{json.dumps(metadata)}"
            return is_valid, None
            
        except Exception as e:
            logging.error(f"Verification error: {str(e)}")
            self.metrics[guild_id]['failures'] += 1
            return False, None
    
    def get_metrics(self, guild_id: int) -> dict:
        """Get security metrics for a guild"""
        return self.metrics[guild_id]

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

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    # Run main
    asyncio.run(main())