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
from contextlib import AsyncExitStack as asyncio_timeout  # For Python 3.10 compatibility

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

# Default messages for successful key claims
DEFAULT_SUCCESS_MESSAGES = [
    # Magical/Fantasy themed
    "✨ {user} has unlocked the {role} role!",
    "🎉 Welcome to {role}, {user}!",
    "🌟 {user} has joined the {role} ranks!",
    "🔓 Access granted! {user} now has the {role} role!",
    "⚡ {user} has activated their {role} powers!",
    "🎭 The {role} welcomes their newest member, {user}!",
    "🔮 {user} has discovered the secrets of {role}!",
    "⚔️ {user} has proven worthy of the {role} rank!",
    "🏰 The gates of {role} open for {user}!",
    "🌈 {user} has crossed the bridge to {role}!",
    
    # Tech/Hacker themed
    "🚀 {user} has deployed to {role}!",
    "💻 {user} has gained root access to {role}!",
    "🔑 Authentication successful! Welcome to {role}, {user}!",
    "🌐 {user} has connected to the {role} network!",
    "⚡ System upgrade complete: {user} → {role}!",
    
    # Adventure themed
    "🗺️ {user} has discovered the {role} realm!",
    "🎯 {user} has reached their destination: {role}!",
    "🌟 A new star rises in {role}: Welcome {user}!",
    "🎪 {user} has joined the {role} circus!",
    "🌈 {user} found the treasure at the end of {role}!",
    
    # Game themed
    "🎮 {user} has unlocked the {role} achievement!",
    "🏆 {user} has earned the {role} trophy!",
    "⬆️ {user} has leveled up to {role}!",
    "🎲 {user} rolled a natural 20 and gained {role}!",
    "🎯 Critical hit! {user} joins {role}!"
]

# Stats tracking
class Stats:
    def __init__(self):
        self.stats = defaultdict(lambda: {
            'keys_added': 0,
            'keys_removed': 0,
            'total_claims': 0,
            'successful_claims': 0,
            'failed_claims': 0,
            'timing': {
                'average': 0,
                'fastest': float('inf'),
                'slowest': 0
            }
        })
        self._lock = asyncio.Lock()
    
    def log_keys_added(self, guild_id: int, count: int):
        """Log key additions"""
        self.stats[guild_id]['keys_added'] += count
    
    def log_keys_removed(self, guild_id: int, count: int):
        """Log key removals"""
        self.stats[guild_id]['keys_removed'] += count
    
    def log_claim(self, guild_id: int, success: bool, duration: float = None):
        """Log claim attempt"""
        stats = self.stats[guild_id]
        stats['total_claims'] += 1
        if success:
            stats['successful_claims'] += 1
            if duration:
                timing = stats['timing']
                # Update timing stats
                timing['fastest'] = min(timing['fastest'], duration)
                timing['slowest'] = max(timing['slowest'], duration)
                # Update running average
                prev_avg = timing['average']
                timing['average'] = prev_avg + (duration - prev_avg) / stats['successful_claims']
        else:
            stats['failed_claims'] += 1
    
    def reset_guild_stats(self, guild_id: int):
        """Reset stats for a guild"""
        if guild_id in self.stats:
            del self.stats[guild_id]
    
    def get_stats(self, guild_id: int) -> dict:
        """Get stats for a guild"""
        return self.stats[guild_id]
    
    async def save_stats(self):
        """Save stats to file"""
        async with self._lock:
            try:
                async with aiofiles.open('stats.json', 'w') as f:
                    await f.write(json.dumps(self.stats, indent=4))
            except Exception as e:
                logging.error(f"Failed to save stats: {str(e)}")

# Audit logging
class AuditLogger:
    def __init__(self):
        self.audit_file = 'audit.log'
    
    async def log_key_add(self, interaction: discord.Interaction, count: int):
        """Log key addition"""
        await self._log_event(interaction, f"Added {count} keys")
    
    async def log_key_remove(self, interaction: discord.Interaction, count: int):
        """Log key removal"""
        await self._log_event(interaction, f"Removed {count} keys")
    
    async def log_claim(self, interaction: discord.Interaction, success: bool):
        """Log claim attempt"""
        await self._log_event(interaction, f"Key claim {'succeeded' if success else 'failed'}")
    
    async def _log_event(self, interaction: discord.Interaction, event: str):
        """Log an audit event"""
        try:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            user = f"{interaction.user} ({interaction.user.id})"
            guild = f"{interaction.guild} ({interaction.guild.id})"
            log_line = f"{timestamp} | {user} | {guild} | {event}\n"
            
            async with aiofiles.open(self.audit_file, 'a') as f:
                await f.write(log_line)
        except Exception as e:
            logging.error(f"Audit logging failed: {str(e)}")

# Initialize stats and audit
stats = Stats()
audit = AuditLogger()

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
        super().__init__(
            command_prefix="!",
            intents=intents,
            case_insensitive=True,
            max_messages=10000,
            timeout=HTTP_TIMEOUT,
            http_retry_count=MAX_RETRIES
        )
        
        # Initialize components
        self.session = None
        self.connector = None
        self.realm_keeper = None
        self.config = Config()
        self.ready = asyncio.Event()
        self.cleanup = None
    
    async def setup_hook(self):
        """Initialize bot systems"""
        try:
            # Create session
            self.connector = TCPConnector(
                limit=MAX_CONNECTIONS,
                ttl_dns_cache=300,
                force_close=False,
                enable_cleanup_closed=True
            )
            self.session = aiohttp.ClientSession(connector=self.connector)
            
            # Add cog
            self.realm_keeper = RealmKeeper(self)
            await self.add_cog(self.realm_keeper)
            
            # Initialize systems
            await self.config.load()
            
            # Start cleanup task
            self.cleanup = KeyCleanup(self)
            await self.cleanup.start()
            
        except Exception as e:
            logging.error(f"Setup error: {e}")
            raise
    
    @commands.Cog.listener()
    async def on_ready(self):
        """Called when bot is ready"""
        try:
            # Sync commands after bot is ready
            await self.tree.sync()
            logging.info("✅ Commands synced globally")
            
            # Set ready event
            self.ready.set()
            
            logging.info(f"✅ Bot ready as {self.user}")
        except Exception as e:
            logging.error(f"Ready event error: {e}")
            raise
    
    async def close(self):
        """Cleanup on shutdown"""
        try:
            if self.cleanup:
                await self.cleanup.stop()
            if self.session:
                await self.session.close()
            if self.connector:
                await self.connector.close()
            await super().close()
        except Exception as e:
            logging.error(f"Shutdown error: {e}")

# Add near the top with other globals
bot = None

# Update main function
async def main():
    """Main entry point"""
    global bot
    
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
    __slots__ = ('role_id', 'main_store', 'command', 'success_msgs', 'bloom')
    
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
        # Create new filter instead of clearing
        self.bloom = ScalableBloomFilter(
            initial_capacity=max(1000, len(self.main_store) * 2),
            error_rate=0.001,
            mode=ScalableBloomFilter.SMALL_SET_GROWTH
        )
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
    
    # Alias for compatibility
    add_keys = bulk_add_keys

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

async def create_dynamic_command(command_name: str, guild_id: int, client: discord.Client):
    """Create a dynamic claim command for a guild"""
    try:
        guild = client.get_guild(guild_id)
        if not guild:
            logging.error(f"Guild {guild_id} not found")
            return False

        # Create the command
        @app_commands.command(name=command_name, description="✨ Claim your role with a mystical key")
        @app_commands.guild_only()
        async def dynamic_claim(interaction: discord.Interaction):
            """Dynamic claim command"""
            if interaction.guild_id != guild_id:
                return
            await interaction.response.send_modal(ArcaneGatewayModal())

        # Remove existing command if it exists
        try:
            existing = client.tree.get_command(command_name, guild=guild)
            if existing:
                client.tree.remove_command(command_name, guild=guild)
        except:
            pass

        # Add and sync command
        client.tree.add_command(dynamic_claim, guild=guild)
        await client.tree.sync(guild=guild)
        logging.info(f"Created command /{command_name} in guild {guild_id}")
        return True

    except Exception as e:
        logging.error(f"Failed to create command {command_name} in guild {guild_id}: {e}")
        return False

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
    """Main cog for key management"""
    def __init__(self, bot):
        self.bot = bot
        self.worker_pool = AdaptiveWorkerPool(min_workers=4, max_workers=16)
        self.interaction_timeout = 15.0
    
    async def sync_commands_to_guild(self, guild_id: int) -> bool:
        """Sync commands to a specific guild"""
        try:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return False
            
            await self.bot.tree.sync(guild=guild)
            logging.info(f"Synced commands to guild {guild_id}")
            return True
            
        except Exception as e:
            logging.error(f"Guild sync error: {str(e)}")
            return False
    
    async def handle_interaction_timeout(self, interaction: discord.Interaction):
        """Handle interaction timeout gracefully"""
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "⌛ Operation timed out, please try again.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "⌛ Operation timed out, please try again.",
                    ephemeral=True
                )
        except Exception as e:
            logging.error(f"Timeout handler error: {e}")
    
    async def handle_interaction_error(self, interaction: discord.Interaction, error: Exception, message: str = None):
        """Handle interaction errors gracefully"""
        try:
            error_msg = message or "An error occurred!"
            logging.error(f"Interaction error: {error}", exc_info=error)
            
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ {error_msg}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"❌ {error_msg}",
                    ephemeral=True
                )
        except Exception as e:
            logging.error(f"Error handler failed: {e}", exc_info=e)
    
    @app_commands.command(name="setup", description="⚙️ Initial server setup")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def setup(self, interaction: discord.Interaction):
        """Initial server setup"""
        try:
            # Use asyncio_timeout instead of asyncio.timeout
            async with asyncio_timeout() as timeout:
                timeout.timeout = self.interaction_timeout
                
                # Check bot permissions
                if not interaction.guild.me.guild_permissions.manage_roles:
                    await interaction.response.send_message(
                        "❌ Bot needs 'Manage Roles' permission!",
                        ephemeral=True
                    )
                    return

                await interaction.response.send_modal(SetupModal())

        except TimeoutError:
            await self.handle_interaction_timeout(interaction)
        except Exception as e:
            await self.handle_interaction_error(interaction, e, "Setup failed!")
    
    @app_commands.command(name="sync", description="🔄 Sync commands to this server")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def sync(self, interaction: discord.Interaction):
        """Sync commands to this server"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            if await self.sync_commands_to_guild(interaction.guild_id):
                await interaction.followup.send(
                    "✅ Commands synced to server!",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ Failed to sync commands!",
                    ephemeral=True
                )
                
        except Exception as e:
            await self.handle_interaction_error(interaction, e, "Sync failed!")
    
    @app_commands.command(name="addkey", description="🔑 Add a single key")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def addkey(self, interaction: discord.Interaction, key: str):
        """Add a single key"""
        guild_id = interaction.guild.id
        if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
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
        await interaction.client.config.save()
        stats.log_keys_added(guild_id, 1)
        await audit.log_key_add(interaction, 1)
        
        msg = "✅ Key added!"
        if expiry_seconds:
            msg += f"\n• Expires in: {expiry_seconds} seconds"
        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="addkeys", description="🔑 Add multiple keys")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def addkeys(self, interaction: discord.Interaction):
        """Add multiple keys"""
        await interaction.response.send_modal(BulkKeyModal())

    @app_commands.command(name="removekey", description="🗑️ Remove a single key")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def removekey(self, interaction: discord.Interaction, key: str):
        """Remove a single key"""
        guild_id = interaction.guild.id
        if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
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
            await interaction.client.config.save()
            await interaction.response.send_message("✅ Key removed!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Key not found!", ephemeral=True)

    @app_commands.command(name="removekeys", description="🗑️ Remove multiple keys")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def removekeys(self, interaction: discord.Interaction):
        """Remove multiple keys"""
        await interaction.response.send_modal(RemoveKeysModal())

    @app_commands.command(name="clearkeys", description="🗑️ Remove all keys")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def clearkeys(self, interaction: discord.Interaction):
        """Remove all keys"""
        guild_id = interaction.guild.id
        if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        key_count = len(guild_config.main_store)
        guild_config.main_store.clear()
        await key_cache.invalidate(guild_id)
        await interaction.client.config.save()
        
        # Reset stats and save them
        stats.reset_guild_stats(guild_id)
        await stats.save_stats()
        
        await interaction.response.send_message(
            f"✅ Cleared {key_count} keys!",
            ephemeral=True
        )

    @app_commands.command(name="keys", description="📊 View key statistics")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def keys(self, interaction: discord.Interaction):
        """View key statistics"""
        guild_id = interaction.guild.id
        if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
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

    @app_commands.command(name="grimoire", description="📖 View available commands")
    @app_commands.guild_only()
    async def grimoire(self, interaction: discord.Interaction):
        """View available commands"""
        embed = discord.Embed(
            title="📖 Realm Keeper Commands",
            description="Here are the mystical commands at your disposal:",
            color=discord.Color.blue()
        )
        
        # Admin commands
        admin_cmds = (
            "`/setup` - Initial server setup\n"
            "`/sync` - Sync commands to server\n"
            "`/addkey` - Add a single key\n"
            "`/addkeys` - Add multiple keys\n"
            "`/removekey` - Remove a single key\n"
            "`/removekeys` - Remove multiple keys\n"
            "`/clearkeys` - Remove all keys\n"
            "`/keys` - View key statistics\n"
            "`/customize` - Customize success messages\n"
            "`/cooldown` - Manage claim cooldowns"
        )
        embed.add_field(name="🛡️ Admin Commands", value=admin_cmds, inline=False)
        
        # User commands
        guild_config = interaction.client.config.guilds.get(interaction.guild.id)
        if guild_config:
            user_cmds = f"`/{guild_config.command}` - Claim your role with a key"
            embed.add_field(name="✨ User Commands", value=user_cmds, inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="customize", description="✏️ Customize success messages")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def customize(self, interaction: discord.Interaction):
        """Customize success messages"""
        await interaction.response.send_modal(CustomizeModal())

    @app_commands.command(name="cooldown", description="⏲️ Manage claim cooldowns")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def cooldown(self, interaction: discord.Interaction, minutes: int):
        """Set claim cooldown duration"""
        try:
            if minutes < 0:
                await interaction.response.send_message(
                    "❌ Cooldown must be 0 or more minutes!",
                    ephemeral=True
                )
                return
            
            guild_id = interaction.guild.id
            if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
                await interaction.response.send_message(
                    "❌ Run /setup first!",
                    ephemeral=True
                )
                return
            
            # Update cooldown
            claim_cooldown.cooldown.per = minutes * 60
            
            # Clear existing cooldowns
            if guild_id in claim_cooldown._cooldowns:
                claim_cooldown._cooldowns[guild_id].clear()
            
            msg = "✅ Claim cooldown removed!" if minutes == 0 else f"✅ Claim cooldown set to {minutes} minutes!"
            await interaction.response.send_message(msg, ephemeral=True)
            
        except Exception as e:
            logging.error(f"Cooldown error: {e}")
            await interaction.response.send_message(
                "❌ Failed to update cooldown!",
                ephemeral=True
            )

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
            # Defer response to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
                await interaction.followup.send(
                    "❌ Run /setup first!", 
                    ephemeral=True
                )
                return

            # Validate all keys first
            key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
            if not key_list:
                await interaction.followup.send(
                    "❌ No keys provided!", 
                    ephemeral=True
                )
                return

            invalid_format = []
            valid_keys = []
            for key in key_list:
                try:
                    uuid_obj = uuid.UUID(key, version=4)
                    if str(uuid_obj) != key.lower():
                        invalid_format.append(key)
                    else:
                        valid_keys.append(key)
                except ValueError:
                    invalid_format.append(key)

            if not valid_keys:
                msg = ["❌ No valid keys to remove!"]
                if invalid_format:
                    msg.append(f"• Invalid format: {len(invalid_format)}")
                await interaction.followup.send(
                    "\n".join(msg),
                    ephemeral=True
                )
                return

            # Remove valid keys
            removed = 0
            not_found = []
            for key in valid_keys:
                found = False
                for full_hash in list(guild_config.main_store):
                    if KeySecurity.verify_key(key, full_hash)[0]:
                        await guild_config.remove_key(full_hash, guild_id)
                        removed += 1
                        found = True
                        break
                if not found:
                    not_found.append(key)

            await interaction.client.config.save()
            stats.log_keys_removed(guild_id, removed)
            await audit.log_key_remove(interaction, removed)
            
            # Build response message
            msg = [f"✅ Removed {removed} keys!"]
            if not_found:
                msg.append(f"• Not found: {len(not_found)}")
            if invalid_format:
                msg.append(f"• Invalid format: {len(invalid_format)}")
            
            await interaction.followup.send(
                "\n".join(msg),
                ephemeral=True
            )

        except Exception as e:
            logging.error(f"Key removal error: {str(e)}")
            try:
                await interaction.followup.send(
                    "❌ Failed to remove keys!",
                    ephemeral=True
                )
            except:
                pass

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
    _salt = None
    _metrics = defaultdict(lambda: {
        'hashes': 0,
        'verifications': 0,
        'failures': 0,
        'avg_verify_time': 0.0
    })
    
    @classmethod
    def _get_salt(cls) -> bytes:
        """Get or generate salt with proper error handling"""
        if cls._salt is None:
            try:
                # Try to load from environment
                if salt := os.getenv('HASH_SALT'):
                    cls._salt = base64.b64decode(salt)
                else:
                    # Generate new salt
                    cls._salt = secrets.token_bytes(16)
                    encoded = base64.b64encode(cls._salt).decode()
                    
                    # Save to .env file
                    with open('.env', 'a') as f:
                        f.write(f"\nHASH_SALT={encoded}")
                    
                    logging.warning("Generated new HASH_SALT")
            except Exception as e:
                logging.error(f"Salt initialization error: {str(e)}")
                cls._salt = secrets.token_bytes(16)  # Use fallback salt
        return cls._salt
    
    @classmethod
    def hash_key(cls, key: str, expiry_seconds: Optional[int] = None) -> str:
        """Hash a key with metadata"""
        try:
            # Prepare metadata
            meta = {}
            if expiry_seconds:
                meta['exp'] = time.time() + expiry_seconds
            
            # Add salt and hash
            salted_key = f"{key}{cls._get_salt().hex()}"
            hash_str = bcrypt_sha256.hash(salted_key, rounds=cls.HASH_ROUNDS)
            
            # Add metadata if needed
            if meta:
                return f"{hash_str}{cls.DELIMITER}{json.dumps(meta)}"
            return hash_str
            
        except Exception as e:
            logging.error(f"Hash error: {str(e)}")
            raise
    
    @classmethod
    def verify_key(cls, key: str, full_hash: str) -> tuple[bool, Optional[str]]:
        """Verify a key against a hash, returns (is_valid, updated_hash)"""
        try:
            # Split hash and metadata
            hash_str = full_hash.split(cls.DELIMITER)[0]
            
            # Check expiration if present
            if cls.DELIMITER in full_hash:
                try:
                    meta = json.loads(full_hash.split(cls.DELIMITER)[1])
                    if meta.get('exp', float('inf')) < time.time():
                        return False, None
                except json.JSONDecodeError:
                    return False, None
            
            # Verify hash
            salted_key = f"{key}{cls._get_salt().hex()}"
            try:
                if bcrypt_sha256.verify(salted_key, hash_str):
                    return True, full_hash
            except ValueError:
                pass
            
            return False, None
            
        except Exception as e:
            logging.error(f"Verification error: {str(e)}")
            return False, None

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

class ArcaneGatewayModal(discord.ui.Modal, title="Key Verification"):
    key = discord.ui.TextInput(
        label="Enter your key",
        placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
        min_length=36,
        max_length=36,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Defer response to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
                await interaction.followup.send(
                    "❌ This server is not setup!", 
                    ephemeral=True
                )
                return

            # Check cooldown
            user_id = interaction.user.id
            if stats.is_on_cooldown(guild_id, user_id):
                await interaction.followup.send(
                    "❌ You're on cooldown! Try again later.", 
                    ephemeral=True
                )
                return

            # Validate key format
            key_value = self.key.value.strip()
            try:
                uuid_obj = uuid.UUID(key_value, version=4)
                if str(uuid_obj) != key_value.lower():
                    raise ValueError()
            except ValueError:
                await interaction.followup.send(
                    "❌ Invalid key format!", 
                    ephemeral=True
                )
                return

            # Check if key exists and is valid
            key_found = False
            for full_hash in list(guild_config.main_store):
                valid, _ = KeySecurity.verify_key(key_value, full_hash)
                if valid:
                    key_found = True
                    # Remove key and grant role
                    await guild_config.remove_key(full_hash, guild_id)
                    await interaction.client.config.save()
                    
                    # Get role
                    role = interaction.guild.get_role(guild_config.role_id)
                    if not role:
                        await interaction.followup.send(
                            "❌ Role not found! Please contact an admin.", 
                            ephemeral=True
                        )
                        return
                    
                    # Add role
                    try:
                        await interaction.user.add_roles(role)
                    except discord.Forbidden:
                        await interaction.followup.send(
                            "❌ I don't have permission to give you that role!", 
                            ephemeral=True
                        )
                        return
                    except Exception as e:
                        logging.error(f"Role grant error: {str(e)}")
                        await interaction.followup.send(
                            "❌ Failed to give you the role!", 
                            ephemeral=True
                        )
                        return
                    
                    # Log success
                    stats.log_claim(guild_id, user_id)
                    await audit.log_claim(interaction)
                    
                    # Send success message
                    success_msg = random.choice(guild_config.success_msgs)
                    await interaction.followup.send(
                        success_msg.format(
                            user=interaction.user.mention,
                            role=role.mention
                        ),
                        ephemeral=True
                    )
                    return

            if not key_found:
                stats.log_failed_claim(guild_id, user_id)
                await interaction.followup.send(
                    "❌ Invalid key!", 
                    ephemeral=True
                )

        except Exception as e:
            logging.error(f"Claim error: {str(e)}")
            try:
                await interaction.followup.send(
                    "❌ Failed to process your key!", 
                    ephemeral=True
                )
            except:
                pass

class CustomizeModal(discord.ui.Modal, title="Customize Messages"):
    messages = discord.ui.TextInput(
        label="Success Messages (one per line)",
        style=discord.TextStyle.long,
        placeholder="✨ {user} has unlocked the {role} role!",
        required=True,
        max_length=2000
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Defer response to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
                await interaction.followup.send(
                    "❌ Run /setup first!", 
                    ephemeral=True
                )
                return

            # Parse and validate messages
            messages = [m.strip() for m in self.messages.value.split("\n") if m.strip()]
            if not messages:
                await interaction.followup.send(
                    "❌ No messages provided!", 
                    ephemeral=True
                )
                return

            # Validate message format
            invalid_format = []
            valid_messages = []
            for msg in messages:
                try:
                    # Test format with dummy values
                    msg.format(user="test", role="test")
                    valid_messages.append(msg)
                except (KeyError, ValueError):
                    invalid_format.append(msg)

            if not valid_messages:
                msg = ["❌ No valid messages found!"]
                if invalid_format:
                    msg.append(f"• Invalid format: {len(invalid_format)}")
                await interaction.followup.send(
                    "\n".join(msg),
                    ephemeral=True
                )
                return

            # Update guild config
            guild_config.success_msgs = valid_messages
            await interaction.client.config.save()
            
            # Build response message
            msg = [f"✅ Updated success messages! ({len(valid_messages)} total)"]
            if invalid_format:
                msg.append(f"• Invalid format: {len(invalid_format)}")
            
            await interaction.followup.send(
                "\n".join(msg),
                ephemeral=True
            )

        except Exception as e:
            logging.error(f"Customize error: {str(e)}")
            try:
                await interaction.followup.send(
                    "❌ Failed to update messages!",
                    ephemeral=True
                )
            except:
                pass

class BulkKeyModal(discord.ui.Modal, title="Add Multiple Keys"):
    keys = discord.ui.TextInput(
        label="Enter keys (one per line)",
        style=discord.TextStyle.long,
        placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
        required=True,
        max_length=2000
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Defer response to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            if (guild_config := interaction.client.config.guilds.get(guild_id)) is None:
                await interaction.followup.send(
                    "❌ Run /setup first!", 
                    ephemeral=True
                )
                return

            # Parse and validate keys
            key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
            if not key_list:
                await interaction.followup.send(
                    "❌ No keys provided!", 
                    ephemeral=True
                )
                return

            # Validate key format
            invalid_format = []
            valid_keys = []
            for key in key_list:
                try:
                    uuid_obj = uuid.UUID(key, version=4)
                    if str(uuid_obj) != key.lower():
                        invalid_format.append(key)
                    else:
                        valid_keys.append(key)
                except ValueError:
                    invalid_format.append(key)

            if not valid_keys:
                msg = ["❌ No valid keys found!"]
                if invalid_format:
                    msg.append(f"• Invalid format: {len(invalid_format)}")
                await interaction.followup.send(
                    "\n".join(msg),
                    ephemeral=True
                )
                return

            # Add valid keys
            added = 0
            duplicates = []
            for key in valid_keys:
                # Check if key already exists
                exists = False
                for full_hash in list(guild_config.main_store):
                    if KeySecurity.verify_key(key, full_hash)[0]:
                        duplicates.append(key)
                        exists = True
                        break
                
                if not exists:
                    # Add new key
                    full_hash = KeySecurity.hash_key(key)
                    await guild_config.add_key(full_hash, guild_id)
                    added += 1

            await interaction.client.config.save()
            stats.log_keys_added(guild_id, added)
            await audit.log_key_add(interaction, added)
            
            # Build response message
            msg = [f"✅ Added {added} keys!"]
            if duplicates:
                msg.append(f"• Duplicates: {len(duplicates)}")
            if invalid_format:
                msg.append(f"• Invalid format: {len(invalid_format)}")
            
            await interaction.followup.send(
                "\n".join(msg),
                ephemeral=True
            )

        except Exception as e:
            logging.error(f"Key addition error: {str(e)}")
            try:
                await interaction.followup.send(
                    "❌ Failed to add keys!",
                    ephemeral=True
                )
            except:
                pass

class SetupModal(discord.ui.Modal, title="Realm Setup"):
    def __init__(self):
        super().__init__()
        self.add_item(discord.ui.TextInput(
            label="Role Name",
            placeholder="Enter the exact role name",
            min_length=1,
            max_length=100,
            required=True
        ))
        self.add_item(discord.ui.TextInput(
            label="Command Name",
            placeholder="Enter command name (e.g. claim, verify, redeem)",
            default="claim",
            min_length=1,
            max_length=32,
            required=True
        ))
        self.add_item(discord.ui.TextInput(
            label="Initial Keys (optional, one per line)",
            style=discord.TextStyle.paragraph,
            placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
            required=False,
            max_length=2000
        ))
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Defer response to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            # Get values from inputs
            role_name = self.children[0].value
            command_name = self.children[1].value.lower()
            initial_keys = self.children[2].value.strip().split('\n') if self.children[2].value.strip() else []

            # Check if command name is valid
            if command_name in RESERVED_NAMES:
                await interaction.followup.send(
                    "❌ That command name is reserved! Please choose another.",
                    ephemeral=True
                )
                return

            # Find role by name
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if not role:
                await interaction.followup.send(
                    "❌ Role not found! Please enter the exact role name.",
                    ephemeral=True
                )
                return

            # Validate bot permissions
            bot_member = interaction.guild.me
            if not bot_member.guild_permissions.manage_roles:
                await interaction.followup.send(
                    "❌ Bot needs 'Manage Roles' permission!",
                    ephemeral=True
                )
                return

            # Check if bot can manage the role
            if role >= bot_member.top_role:
                await interaction.followup.send(
                    "❌ Bot's highest role must be above the target role!",
                    ephemeral=True
                )
                return

            # Check if role is managed by integration
            if role.managed:
                await interaction.followup.send(
                    "❌ Cannot use integration-managed roles!",
                    ephemeral=True
                )
                return

            # Check if role is @everyone
            if role.is_default():
                await interaction.followup.send(
                    "❌ Cannot use @everyone role!",
                    ephemeral=True
                )
                return

            # Create config
            guild_id = interaction.guild.id
            interaction.client.config.guilds[guild_id] = GuildConfig(
                role_id=role.id,
                valid_keys=set(),  # Initialize with empty set
                command=command_name
            )
            
            # Process initial keys if provided
            valid_keys = []
            invalid_format = []
            if initial_keys:
                for key in initial_keys:
                    key = key.strip()
                    try:
                        uuid_obj = uuid.UUID(key, version=4)
                        if str(uuid_obj) == key.lower():
                            valid_keys.append(key)
                        else:
                            invalid_format.append(key)
                    except ValueError:
                        invalid_format.append(key)

            # Add valid keys if any
            if valid_keys:
                guild_config = interaction.client.config.guilds[guild_id]
                for key in valid_keys:
                    hashed = KeySecurity.hash_key(key)
                    await guild_config.add_key(hashed, guild_id)
                stats.log_keys_added(guild_id, len(valid_keys))
                await audit.log_key_add(interaction, len(valid_keys))
            
            # Save and sync
            await interaction.client.config.save()
            success = await create_dynamic_command(command_name, guild_id, interaction.client)
            
            # Build response message
            response = [
                "✅ Setup complete!",
                f"• Role: {role.mention}",
                f"• Command: /{command_name}"
            ]
            if valid_keys:
                response.append(f"• Added {len(valid_keys)} keys")
                if invalid_format:
                    response.append(f"• Skipped {len(invalid_format)} invalid keys")
            else:
                response.append("• Add keys with /addkey or /addkeys")
            
            if not success:
                response.append("⚠️ Failed to create command, use /sync to retry")
            
            await interaction.followup.send(
                "\n".join(response),
                ephemeral=True
            )
            
        except Exception as e:
            logging.error(f"Setup error: {e}")
            try:
                await interaction.followup.send(
                    "❌ Setup failed!",
                    ephemeral=True
                )
            except:
                pass

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    # Run main
    asyncio.run(main())