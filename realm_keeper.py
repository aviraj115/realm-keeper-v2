import discord
import json
import os
import logging
import aiofiles
import aiofiles.os
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv
from typing import Dict, Set, Optional, List
import random
from discord.ext.commands import Cooldown, BucketType
from collections import defaultdict, deque
import asyncio
from passlib.hash import bcrypt_sha256
import time
import uuid
import hashlib
from concurrent.futures import ThreadPoolExecutor
import psutil
import mmh3
from aiohttp import TCPConnector, ClientTimeout
from discord import HTTPException, GatewayNotFound
import backoff
import sys
import platform

# Set event loop policy for Windows if needed
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Create event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# Configuration and logging setup
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)

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
        super().__init__(
            command_prefix="!",
            intents=intents,
            case_insensitive=True,
            max_messages=10000,
            timeout=HTTP_TIMEOUT,
            http_retry_count=MAX_RETRIES
        )
        self.connector = None
        self.realm_keeper = None
        
        # Set up command tree
        self.tree = app_commands.CommandTree(self)
    
    async def setup_hook(self):
        """Initialize bot systems"""
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
        
        # Sync commands
        await self.tree.sync()
    
    async def close(self):
        """Cleanup on shutdown"""
        if self.connector:
            await self.connector.close()
        await super().close()

async def main():
    """Main entry point"""
    async with RealmBot() as bot:
        await bot.start(TOKEN)

# Configuration handling
class GuildConfig:
    __slots__ = ('role_id', 'main_store', 'quick_lookup', 'command', 'success_msgs')
    
    def __init__(self, role_id: int, valid_keys: Set[str], command: str = "claim", 
                 success_msgs: list = None):
        self.role_id = role_id
        self.main_store = valid_keys
        self.quick_lookup = defaultdict(set)
        self.command = command
        self.success_msgs = success_msgs or DEFAULT_SUCCESS_MESSAGES.copy()
        self._rebuild_quick_lookup()
    
    def _rebuild_quick_lookup(self):
        """Rebuild quick lookup cache"""
        self.quick_lookup.clear()
        for full_hash in self.main_store:
            hash_part = full_hash.split(KeySecurity.DELIMITER)[0] if KeySecurity.DELIMITER in full_hash else full_hash
            self.quick_lookup[hash_part[:KeySecurity.HASH_PREFIX_LENGTH]].add(full_hash)
    
    async def add_key(self, full_hash: str, guild_id: int):
        """Add a key with cache invalidation"""
        self.main_store.add(full_hash)
        self._rebuild_quick_lookup()
        await key_cache.invalidate(guild_id)
    
    async def remove_key(self, full_hash: str, guild_id: int):
        """Remove a key with cache invalidation"""
        self.main_store.discard(full_hash)
        self._rebuild_quick_lookup()
        await key_cache.invalidate(guild_id)
    
    async def bulk_add_keys(self, hashes: Set[str], guild_id: int):
        """Add multiple keys efficiently"""
        self.main_store.update(hashes)
        self._rebuild_quick_lookup()
        await key_cache.invalidate(guild_id)

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
    def __init__(self):
        self.quick_lookup = defaultdict(dict)  # {guild_id: {quick_hash: set(full_hashes)}}
        self.last_update = defaultdict(float)
        self.cache_hits = defaultdict(int)
        self.cache_misses = defaultdict(int)
        self.cache_size = 1000  # Max keys per guild to cache
    
    async def warm_cache(self, guild_id: int, guild_config: GuildConfig):
        """Pre-compute quick hashes for a guild with optimized storage"""
        try:
            quick_lookup = defaultdict(set)
            
            # Process in chunks to avoid memory spikes
            chunk_size = 100
            hashes = list(guild_config.main_store)
            
            for i in range(0, len(hashes), chunk_size):
                chunk = hashes[i:i + chunk_size]
                for full_hash in chunk:
                    # Handle metadata in hash
                    if KeySecurity.DELIMITER in full_hash:
                        hash_part = full_hash.split(KeySecurity.DELIMITER)[0]
                    else:
                        hash_part = full_hash
                        
                    # Use faster hash function for lookup
                    quick_hash = mmh3.hash(hash_part.encode(), signed=False) & 0xFFFFFFFF
                    quick_lookup[quick_hash].add(full_hash)
                    
                # Allow other tasks to run
                await asyncio.sleep(0)
            
            async with self._lock:
                # Limit cache size
                if len(quick_lookup) > self.cache_size:
                    # Keep most recently added keys
                    keys_to_keep = list(quick_lookup.keys())[-self.cache_size:]
                    quick_lookup = {k: quick_lookup[k] for k in keys_to_keep}
                
                self.quick_lookup[guild_id] = quick_lookup
                self.last_update[guild_id] = time.time()
                
        except Exception as e:
            logging.error(f"Cache warmup failed: {str(e)}")
            # Invalidate cache on error
            await self.invalidate(guild_id)
    
    async def get_possible_hashes(self, guild_id: int, key: str) -> Optional[Set[str]]:
        """Get possible hash matches with metrics"""
        try:
            quick_hash = mmh3.hash(key.encode(), signed=False) & 0xFFFFFFFF
            matches = self.quick_lookup[guild_id].get(quick_hash)
            
            if matches:
                self.cache_hits[guild_id] += 1
                return matches
            
            self.cache_misses[guild_id] += 1
            return None
            
        except Exception:
            return None
    
    async def invalidate(self, guild_id: int):
        """Force cache invalidation for a guild"""
        async with self._lock:
            self.quick_lookup.pop(guild_id, None)
            self.last_update.pop(guild_id, None)
            self.cache_hits.pop(guild_id, None)
            self.cache_misses.pop(guild_id, None)
    
    def get_metrics(self, guild_id: int) -> dict:
        """Get cache performance metrics"""
        hits = self.cache_hits[guild_id]
        misses = self.cache_misses[guild_id]
        total = hits + misses
        
        return {
            'hits': hits,
            'misses': misses,
            'hit_rate': hits / total if total > 0 else 0,
            'size': len(self.quick_lookup.get(guild_id, {})),
            'last_update': self.last_update.get(guild_id, 0)
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

async def sync_commands():
    try:
        global_commands = await bot.tree.sync()
        logging.info(f"Synced {len(global_commands)} global commands")
        
        for guild in bot.guilds:
            try:
                bot.tree.copy_global_to(guild=guild)
                await bot.tree.sync(guild=guild)
                logging.info(f"Synced commands with {guild.name} ({guild.id})")
            except discord.Forbidden:
                logging.warning(f"Missing permissions in {guild.name}")
            except Exception as e:
                logging.error(f"Failed to sync with {guild.name}: {str(e)}")
                
    except Exception as e:
        logging.error(f"Critical sync error: {str(e)}")

class RealmKeeper(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.worker_pool = AdaptiveWorkerPool()
        self.key_security = KeySecurity()
        self.monitor_task = None

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
            logging.info("�� Pre-warming caches...")
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
            await sync_commands()
            
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
    async def setup(self, interaction: discord.Interaction):
        """Initial server setup"""
        await interaction.response.send_modal(SetupModal())

    @app_commands.command(name="keys", description="📊 View key statistics")
    @app_commands.default_permissions(administrator=True)
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
    async def clearkeys(self, interaction: discord.Interaction):
        """Remove all keys from the server"""
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return
        
        key_count = len(guild_config.main_store)
        guild_config.main_store.clear()
        guild_config.quick_lookup.clear()
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

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    # Run main
    asyncio.run(main())