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
bot = commands.AutoShardedBot(
    command_prefix="!",
    intents=intents,
    case_insensitive=True,
    max_messages=10000,
    connector=TCPConnector(
        limit=MAX_CONNECTIONS,
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True
    ),
    timeout=HTTP_TIMEOUT,
    http_retry_count=MAX_RETRIES,
    loop=loop  # Pass the event loop
)

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

@bot.event
async def on_ready():
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
                key_security.worker_pool,
                KeySecurity.hash_key,
                fake_key
            )
        
        # Pre-warm connection pool
        logging.info("🌐 Pre-warming connections...")
        async with bot.session.get(
            "https://discord.com/api/v9/gateway",
            timeout=HTTP_TIMEOUT
        ) as resp:
            await resp.read()
            
        # Pre-warm caches
        logging.info("💾 Pre-warming caches...")
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
        worker_pool.start()
        
        # Sync commands
        await sync_commands()
        
        # Update presence
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="⚡ for magical keys"
            )
        )
        
        # Log ready state
        guild_count = len(bot.guilds)
        key_count = sum(len(cfg.main_store) for cfg in config.values())
        logging.info(
            f"✅ Bot ready!\n"
            f"• Guilds: {guild_count}\n"
            f"• Total keys: {key_count}\n"
            f"• Workers: {len(key_security.worker_pool._threads)}\n"
            f"• Connections: {len(bot.http._HTTPClient__session.connector._conns)}\n"
            f"• Commands restored: {restored}"
        )
        
    except Exception as e:
        logging.error(f"Startup error: {str(e)}")
        # Continue with basic functionality

@bot.event
async def on_guild_join(guild):
    try:
        await bot.tree.sync(guild=guild)
        logging.info(f"Auto-synced commands to {guild.name}")
    except Exception as e:
        logging.error(f"Failed to sync new guild: {str(e)}")

@tasks.loop(hours=24)
async def cleanup_task():
    logging.info("Running daily cleanup")
    current_guilds = {guild.id for guild in bot.guilds}
    removed = 0
    
    for guild_id in list(config.keys()):
        if guild_id not in current_guilds:
            del config[guild_id]
            removed += 1
            
    if removed:
        await save_config()
        logging.info(f"Removed {removed} inactive guilds")

@tasks.loop(minutes=5)
async def save_stats_task():
    """Periodically save stats to file"""
    try:
        await stats.save_stats()
    except Exception as e:
        logging.error(f"Failed to save stats: {str(e)}")

@tasks.loop(hours=1)
async def cleanup_expired_keys():
    try:
        now = time.time()
        for guild_id, guild_config in config.items():
            async with key_locks[guild_id]:
                original_count = len(guild_config.main_store)
                valid_keys = set()
                
                for full_hash in guild_config.main_store:
                    try:
                        if KeySecurity.DELIMITER in full_hash:
                            hash_part, meta_part = full_hash.split(KeySecurity.DELIMITER, 1)
                            metadata = json.loads(meta_part)
                            if metadata.get('exp') and metadata['exp'] < now:
                                continue
                            if metadata.get('uses') and metadata['uses'] <= 0:
                                continue
                        valid_keys.add(full_hash)
                    except:
                        valid_keys.add(full_hash)
                
                removed = original_count - len(valid_keys)
                if removed > 0:
                    guild_config.main_store = valid_keys
                    await save_config()
                    stats.log_keys_removed(guild_id, removed)
                    logging.info(f"Removed {removed} expired/used keys from guild {guild_id}")
                    
    except Exception as e:
        logging.error(f"Error in key cleanup: {str(e)}")

@tasks.loop(minutes=1)
async def memory_check():
    try:
        usage = psutil.Process().memory_info().rss / 1024 / 1024
        percent = psutil.virtual_memory().percent
        
        if percent > 90:
            logging.warning(f"⚠️ High memory usage: {percent}% ({usage:.1f}MB)")
            await key_cache.cleanup()
            
        if percent > 95:
            logging.error(f"🚨 Critical memory: {percent}% ({usage:.1f}MB)")
            
    except Exception as e:
        logging.error(f"Memory check error: {str(e)}")

DEFAULT_SUCCESS_MESSAGES = [
    "✨ {user} has unlocked the {role}! ✨",
    "🌟 The ancient runes accept {user} into {role}!",
    "🔮 {user} has been granted the power of {role}!",
    "⚡ The portal opens, welcoming {user} to {role}!",
    "🎭 {user} has proven worthy of {role}!",
    "🌌 The stars align as {user} joins {role}!",
    "🎋 Ancient spirits welcome {user} to {role}!",
    "🔱 The sacred gates open for {user} to enter {role}!",
    "💫 {user} has been chosen by the {role} spirits!",
    "🌠 The mystical energies embrace {user} in {role}!",
    "🏮 {user} lights the eternal flame of {role}!",
    "🌸 The sacred blossoms welcome {user} to {role}!",
    "⭐ {user} has awakened the power of {role}!",
    "🌙 The moon blesses {user} with {role}!",
    "🎆 The realms rejoice as {user} joins {role}!"
]

class SetupModal(discord.ui.Modal, title="⚙️ Server Configuration"):
    role_name = discord.ui.TextInput(
        label="Role Name (exact match)",
        placeholder="Realm Traveler",
        style=discord.TextStyle.short,
        required=True
    )
    
    command_name = discord.ui.TextInput(
        label="Activation Command",
        placeholder="openportal",
        style=discord.TextStyle.short,
        required=True,
        max_length=20
    )
    
    success_message = discord.ui.TextInput(
        label="Success Messages (one per line)",
        placeholder="✨ {user} unlocked {role}!\n🌟 {user} joined {role}!",
        style=discord.TextStyle.long,
        required=False
    )
    
    initial_keys = discord.ui.TextInput(
        label="Initial Keys (one per line)",
        style=discord.TextStyle.long,
        placeholder="key1\nkey2\nkey3",
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            progress_msg = await interaction.followup.send(
                "⚙️ Setting up...", 
                ephemeral=True,
                wait=True
            )

            # Validate role
            role = discord.utils.get(interaction.guild.roles, name=self.role_name.value)
            if not role:
                await progress_msg.edit(content="❌ Role not found! Create it first.")
                return

            # Validate command name
            command = self.command_name.value.lower().strip()
            if not command or command in RESERVED_NAMES:
                await progress_msg.edit(content="❌ Invalid command name!")
                return

            # Process success messages
            success_msgs = []
            if self.success_message.value.strip():
                success_msgs = [
                    msg.strip() for msg in self.success_message.value.split('\n')
                    if msg.strip()
                ]

            # Process initial keys
            initial_key_set = set()
            if self.initial_keys.value.strip():
                await progress_msg.edit(content="🔑 Validating keys...")
                for key in self.initial_keys.value.split('\n'):
                    key = key.strip()
                    if not key:
                        continue
                    try:
                        uuid_obj = uuid.UUID(key, version=4)
                        if str(uuid_obj) != key.lower():
                            raise ValueError()
                        initial_key_set.add(KeySecurity.hash_key(key))
                    except ValueError:
                        await progress_msg.edit(
                            content=f"❌ Invalid key format: {key[:8]}...\nKeys must be UUIDv4!"
                        )
                        return

            # Create config
            guild_id = interaction.guild.id
            config[guild_id] = GuildConfig(
                role.id,
                initial_key_set,
                command,
                success_msgs if success_msgs else None
            )

            await progress_msg.edit(content="⚡ Creating command...")
            await create_dynamic_command(command, guild_id)
            await save_config()

            await progress_msg.edit(content=(
                f"✅ Setup complete!\n• Command: `/{command}`\n"
                f"• Success messages: {len(success_msgs) or len(DEFAULT_SUCCESS_MESSAGES)}\n"
                f"• Initial keys: {len(initial_key_set)}"
            ))

        except Exception as e:
            logging.error(f"Setup error: {str(e)}")
            try:
                await progress_msg.edit(content=f"❌ Setup failed: {str(e)}")
            except:
                await interaction.followup.send("❌ Setup failed!", ephemeral=True)

class ArcaneGatewayModal(discord.ui.Modal, title="Enter Mystical Key"):
    key = discord.ui.TextInput(
        label="Enter your mystical key",
        placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
        min_length=36,
        max_length=36
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            progress_msg = await interaction.followup.send(
                "🔮 Channeling mystical energies...", 
                ephemeral=True,
                wait=True
            )
            
            start_time = time.time()
            guild_id = interaction.guild.id
            user = interaction.user
            key_value = self.key.value.strip()
            
            # Basic validation
            try:
                uuid_obj = uuid.UUID(key_value, version=4)
                if str(uuid_obj) != key_value.lower():
                    raise ValueError()
            except ValueError:
                await progress_msg.edit(content="❌ Invalid key format!")
                return

            # Get guild config
            if (guild_config := config.get(guild_id)) is None:
                await progress_msg.edit(content="❌ Server not configured!")
                return

            # Check cooldown
            if retry_after := claim_cooldown.get_retry_after(interaction):
                await progress_msg.edit(
                    content=f"⏳ Please wait {int(retry_after)} seconds before trying again!"
                )
                return

            # Get possible matches using quick lookup
            quick_hash = mmh3.hash(key_value.encode(), signed=False) & 0xFFFFFFFF
            possible_hashes = guild_config.quick_lookup.get(quick_hash, set())
            
            if not possible_hashes:
                stats.log_claim(guild_id, False)
                await audit.log_claim(interaction, key_value[:8], False)
                await progress_msg.edit(content="❌ Invalid key or already claimed!")
                return

            # Verify in parallel batches
            async with key_locks[guild_id][get_shard(user.id)]:
                is_valid, updated_hash = await key_security.verify_keys_batch(
                    key_value, 
                    possible_hashes,
                    chunk_size=20  # Larger chunks for better throughput
                )

                if not is_valid:
                    stats.log_claim(guild_id, False)
                    await audit.log_claim(interaction, key_value[:8], False)
                    await progress_msg.edit(content="❌ Invalid key or already claimed!")
                    return

                # Update key storage
                for full_hash in possible_hashes:
                    if await key_security.verify_key(key_value, full_hash)[0]:
                        await guild_config.remove_key(full_hash, guild_id)
                        if updated_hash:  # Key still has uses
                            await guild_config.add_key(updated_hash, guild_id)
                        break

                await save_config()

            # Grant role
            role = interaction.guild.get_role(guild_config.role_id)
            await user.add_roles(role)
            
            # Log success
            claim_time = time.time() - start_time
            stats.log_claim(guild_id, True, claim_time)
            await audit.log_claim(interaction, key_value[:8], True)
            
            # Send success message
            success_msg = random.choice(guild_config.success_msgs)
            await progress_msg.edit(
                content=success_msg.format(
                    user=user.mention,
                    role=f"<@&{role.id}>"
                )
            )

        except Exception as e:
            logging.error(f"Claim error: {str(e)}")
            await progress_msg.edit(content="❌ An error occurred!")

class KeySecurity:
    DELIMITER = "||"
    HASH_PREFIX_LENGTH = 7
    HASH_ROUNDS = 10  # Reduced bcrypt rounds for better performance
    
    def __init__(self):
        self.worker_pool = ThreadPoolExecutor(
            max_workers=max(8, os.cpu_count() * 2),  # More workers for parallel processing
            thread_name_prefix="key-verify"
        )
    
    @staticmethod
    def hash_key(key: str, expiry_seconds: Optional[int] = None, max_uses: Optional[int] = None) -> str:
        """Hash a key with optional metadata"""
        # Use optimized bcrypt settings
        hash_str = bcrypt_sha256.using(rounds=KeySecurity.HASH_ROUNDS).hash(key)
        if expiry_seconds or max_uses:
            metadata = {}
            if expiry_seconds:
                metadata['exp'] = time.time() + expiry_seconds
            if max_uses:
                metadata['uses'] = max_uses
            return f"{hash_str}{KeySecurity.DELIMITER}{json.dumps(metadata)}"
        return hash_str

    @staticmethod
    def verify_key(key: str, full_hash: str) -> tuple[bool, Optional[str]]:
        """Verify key and handle metadata"""
        try:
            if KeySecurity.DELIMITER in full_hash:
                hash_part, meta_part = full_hash.split(KeySecurity.DELIMITER, 1)
                # Fast-fail if metadata is invalid
                try:
                    metadata = json.loads(meta_part)
                except:
                    return False, None
                
                # Check expiry first (fastest check)
                if metadata.get('exp') and metadata['exp'] < time.time():
                    return False, None
                
                # Only verify hash if metadata checks pass
                if not bcrypt_sha256.verify(key, hash_part):
                    return False, None
                
                # Handle uses
                if 'uses' in metadata:
                    if metadata['uses'] <= 0:
                        return False, None
                    metadata['uses'] -= 1
                    if metadata['uses'] > 0:
                        return True, f"{hash_part}{KeySecurity.DELIMITER}{json.dumps(metadata)}"
                    return True, None
                    
                return True, full_hash
            else:
                # Simple hash verification
                return bcrypt_sha256.verify(key, full_hash), full_hash
                
        except Exception:
            return False, None

    async def verify_keys_batch(self, key: str, hashes: Set[str], chunk_size: int = 20) -> tuple[bool, Optional[str]]:
        """Verify key against multiple hashes in parallel batches"""
        if not hashes:
            return False, None
            
        # Larger chunks for better throughput
        hash_chunks = [list(hashes)[i:i + chunk_size] for i in range(0, len(hashes), chunk_size)]
        
        for chunk in hash_chunks:
            # Process chunk in parallel
            tasks = [
                asyncio.get_event_loop().run_in_executor(
                    self.worker_pool,
                    KeySecurity.verify_key,
                    key,
                    full_hash
                )
                for full_hash in chunk
            ]
            
            # Wait for all verifications in chunk with timeout
            try:
                results = await asyncio.gather(*tasks, timeout=5.0)
                
                # Check results
                for i, (is_valid, updated_hash) in enumerate(results):
                    if is_valid:
                        return True, updated_hash or chunk[i]
            except asyncio.TimeoutError:
                # Cancel remaining tasks on timeout
                for task in tasks:
                    task.cancel()
                    
        return False, None

class AdaptiveWorkerPool:
    def __init__(self, min_workers: int = 4, max_workers: int = 32):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.pool = ThreadPoolExecutor(
            max_workers=min_workers,
            thread_name_prefix="worker"
        )
        self._monitor_task = None
    
    def start(self):
        """Start the worker pool with pre-warming"""
        # Pre-warm threads
        futures = []
        for _ in range(self.min_workers):
            futures.append(self.pool.submit(lambda: None))
        
        # Wait for threads to start
        for future in futures:
            future.result()

worker_pool = AdaptiveWorkerPool()

@tasks.loop(minutes=1)
async def monitor_workers():
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        queue_size = worker_pool.pool._work_queue.qsize()
        
        await worker_pool.adjust_workers(queue_size, cpu_percent)
        
        logging.info(
            f"Worker stats: Active={worker_pool.current_workers}, "
            f"Queue={queue_size}, CPU={cpu_percent}%"
        )
            
    except Exception as e:
        logging.error(f"Worker monitoring error: {str(e)}")

class Stats:
    def __init__(self):
        # Persistent stats (survive restarts)
        self.persistent_stats = defaultdict(lambda: {
            'total_keys_added': 0,
            'total_keys_removed': 0,
            'total_claims_all_time': 0,
            'successful_claims_all_time': 0
        })
        
        # Session stats (reset on restart)
        self.guild_stats = defaultdict(lambda: {
            'total_claims': 0,
            'successful_claims': 0,
            'failed_claims': 0,
            'keys_added': 0,
            'keys_removed': 0,
            'total_claim_time': 0.0,
            'fastest_claim': float('inf'),
            'slowest_claim': 0.0,
            'last_claim': 0,
            'session_start': time.time()
        })
        self.load_stats()

    async def save_stats(self):
        """Save persistent stats to file"""
        try:
            async with aiofiles.open('stats.json', 'w') as f:
                await f.write(json.dumps({
                    str(guild_id): stats
                    for guild_id, stats in self.persistent_stats.items()
                }, indent=4))
        except Exception as e:
            logging.error(f"Failed to save stats: {str(e)}")

    def load_stats(self):
        """Load persistent stats from file"""
        try:
            with open('stats.json', 'r') as f:
                data = json.loads(f.read())
                for guild_id, stats in data.items():
                    self.persistent_stats[int(guild_id)].update(stats)
        except FileNotFoundError:
            pass
        except Exception as e:
            logging.error(f"Failed to load stats: {str(e)}")

    def log_claim(self, guild_id: int, success: bool, time_taken: float = None):
        """Log a claim attempt with timing"""
        # Update session stats
        stats = self.guild_stats[guild_id]
        stats['total_claims'] += 1
        stats['last_claim'] = time.time()
        
        if success:
            stats['successful_claims'] += 1
            if time_taken is not None:
                stats['total_claim_time'] += time_taken
                stats['fastest_claim'] = min(stats['fastest_claim'], time_taken)
                stats['slowest_claim'] = max(stats['slowest_claim'], time_taken)
        else:
            stats['failed_claims'] += 1
            
        # Update persistent stats
        p_stats = self.persistent_stats[guild_id]
        p_stats['total_claims_all_time'] += 1
        if success:
            p_stats['successful_claims_all_time'] += 1

    def log_keys_added(self, guild_id: int, count: int):
        """Log keys being added"""
        self.guild_stats[guild_id]['keys_added'] += count
        self.persistent_stats[guild_id]['total_keys_added'] += count

    def log_keys_removed(self, guild_id: int, count: int):
        """Log keys being removed"""
        self.guild_stats[guild_id]['keys_removed'] += count
        self.persistent_stats[guild_id]['total_keys_removed'] += count

    def get_stats(self, guild_id: int) -> dict:
        """Get formatted stats for display"""
        session = self.guild_stats[guild_id]
        persistent = self.persistent_stats[guild_id]
        
        # Calculate average claim time
        successful_claims = session['successful_claims']
        avg_time = session['total_claim_time'] / successful_claims if successful_claims > 0 else 0
        
        return {
            'total_claims': session['total_claims'],
            'successful_claims': session['successful_claims'],
            'failed_claims': session['failed_claims'],
            'keys_added': persistent['total_keys_added'],
            'keys_removed': persistent['total_keys_removed'],
            'timing': {
                'average': f"{avg_time:.2f}s",
                'fastest': f"{session['fastest_claim']:.2f}s" if session['fastest_claim'] != float('inf') else "N/A",
                'slowest': f"{session['slowest_claim']:.2f}s" if session['slowest_claim'] > 0 else "N/A"
            }
        }

    def reset_guild_stats(self, guild_id: int):
        """Reset both session and persistent stats for a guild"""
        # Reset session stats
        self.guild_stats[guild_id] = {
            'total_claims': 0,
            'successful_claims': 0,
            'failed_claims': 0,
            'keys_added': 0,
            'keys_removed': 0,
            'total_claim_time': 0.0,
            'fastest_claim': float('inf'),
            'slowest_claim': 0.0,
            'last_claim': 0,
            'session_start': time.time()
        }
        
        # Reset persistent stats
        self.persistent_stats[guild_id] = {
            'total_keys_added': 0,
            'total_keys_removed': 0,
            'total_claims_all_time': 0,
            'successful_claims_all_time': 0
        }

# Initialize stats
stats = Stats()

class AuditLogger:
    async def log_claim(self, interaction: discord.Interaction, key_prefix: str, success: bool):
        logging.info(
            f"Claim attempt by {interaction.user} ({interaction.user.id}) "
            f"in {interaction.guild.name} ({interaction.guild.id}) "
            f"with key {key_prefix}... - {'✅' if success else '❌'}"
        )
    
    async def log_key_add(self, interaction: discord.Interaction, count: int):
        logging.info(
            f"Added {count} keys in {interaction.guild.name} ({interaction.guild.id}) "
            f"by {interaction.user} ({interaction.user.id})"
        )

# Initialize audit logger
audit = AuditLogger()

def admin_cooldown():
    """Cooldown for admin commands"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.guild_permissions.administrator:
            return True
        retry_after = admin_cd.get_retry_after(interaction)
        if retry_after:
            await interaction.response.send_message(
                f"⏳ Command cooldown. Try again in {int(retry_after)} seconds.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

# Initialize admin cooldown (3 commands per minute)
admin_cd = CustomCooldown(3, 60)

@bot.tree.command(name="addkey", description="Add single key")
@app_commands.default_permissions(administrator=True)
@admin_cooldown()
async def addkey(interaction: discord.Interaction, key: str, expires_in: Optional[int] = None):
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

    expiry_seconds = expires_in * 3600 if expires_in else None
    
    if any(KeySecurity.verify_key(key, h)[0] for h in guild_config.main_store):
        await interaction.response.send_message("❌ Key exists!", ephemeral=True)
        return

    hashed = KeySecurity.hash_key(key, expiry_seconds)
    await guild_config.add_key(hashed, guild_id)
    await save_config()
    stats.log_keys_added(guild_id, 1)
    await audit.log_key_add(interaction, 1)
    
    msg = "✅ Key added!"
    if expires_in:
        msg += f"\n• Expires in: {expires_in} hours"
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="setup", description="🔧 Configure the bot for your server")
@app_commands.default_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    """Initial bot setup for a server"""
    await interaction.response.send_modal(SetupModal())

@bot.tree.command(name="addkeys", description="📥 Add multiple keys at once")
@app_commands.default_permissions(administrator=True)
@admin_cooldown()
async def addkeys(interaction: discord.Interaction):
    """Add multiple keys in bulk"""
    await interaction.response.send_modal(BulkKeysModal())

@bot.tree.command(name="keys", description="📊 View key statistics")
@app_commands.default_permissions(administrator=True)
async def keys(interaction: discord.Interaction):
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

@bot.tree.command(name="clearkeys", description="🗑️ Remove all keys")
@app_commands.default_permissions(administrator=True)
async def clearkeys(interaction: discord.Interaction):
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

@bot.tree.command(name="removekey", description="🗑️ Remove a specific key")
@app_commands.default_permissions(administrator=True)
@admin_cooldown()
async def removekey(interaction: discord.Interaction, key: str):
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

@bot.tree.command(name="removekeys", description="🗑️ Remove multiple keys")
@app_commands.default_permissions(administrator=True)
@admin_cooldown()
async def removekeys(interaction: discord.Interaction):
    """Remove multiple keys"""
    await interaction.response.send_modal(RemoveKeysModal())

@bot.tree.command(name="sync", description="🔄 Force sync commands")
@app_commands.default_permissions(administrator=True)
async def sync_guild_commands(interaction: discord.Interaction):
    """Force sync commands with this guild"""
    try:
        await interaction.response.defer(ephemeral=True)
        bot.tree.copy_global_to(guild=interaction.guild)
        await bot.tree.sync(guild=interaction.guild)
        await interaction.followup.send("✅ Commands synced!", ephemeral=True)
    except Exception as e:
        logging.error(f"Sync error: {str(e)}")
        await interaction.followup.send("❌ Sync failed!", ephemeral=True)

@bot.tree.command(name="grimoire", description="📚 View command documentation")
async def grimoire(interaction: discord.Interaction):
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

@bot.tree.command(name="metrics", description="📊 View detailed performance metrics (Admin only)")
@app_commands.default_permissions(administrator=True)
async def view_metrics(interaction: discord.Interaction):
    """View detailed performance metrics"""
    guild_id = interaction.guild.id
    if (guild_config := config.get(guild_id)) is None:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return
        
    queue_stats = queue_metrics.get_metrics(guild_id)
    
    embed = discord.Embed(
        title="🔍 Performance Metrics",
        color=discord.Color.blue(),
        timestamp=discord.utils.utcnow()
    )
    
    # Queue stats
    queue_info = (
        f"• Processed: {queue_stats['processed']}\n"
        f"• Errors: {queue_stats['errors']}\n"
        f"• Current queue: {queue_stats['requests_waiting']}\n"
        f"• Peak queue: {queue_stats['peak_queue_size']}"
    )
    embed.add_field(name="📋 Queue Status", value=queue_info, inline=False)
    
    # Timing stats
    if queue_stats['processed'] > 0:
        timing = (
            f"• Average wait: {queue_stats['avg_wait_time']:.2f}s\n"
            f"• Total wait: {queue_stats['total_wait_time']:.1f}s\n"
            f"• Last processed: {format_time_ago(time.time() - queue_stats['last_processed'])}"
        )
        embed.add_field(name="⏱️ Timing", value=timing, inline=False)
    
    # System stats
    cpu_percent = psutil.cpu_percent()
    memory = psutil.Process().memory_info()
    sys_stats = (
        f"• CPU Usage: {cpu_percent}%\n"
        f"• Memory: {memory.rss / 1024 / 1024:.1f}MB\n"
        f"• Active workers: {len(worker_pool.pool._threads)}\n"
        f"• Queue size: {worker_pool.pool._work_queue.qsize()}"
    )
    embed.add_field(name="🖥️ System", value=sys_stats, inline=False)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

def format_time_ago(seconds: float) -> str:
    """Format time difference into human readable string"""
    if seconds < 60:
        return f"{int(seconds)}s ago"
    elif seconds < 3600:
        return f"{int(seconds/60)}m ago"
    elif seconds < 86400:
        return f"{int(seconds/3600)}h ago"
    else:
        return f"{int(seconds/86400)}d ago"

# Update COMMAND_ALIASES to avoid conflicts with actual commands
COMMAND_ALIASES = {
    'addkey': ['newkey', 'createkey', 'genkey'],
    'addkeys': ['newkeys', 'createkeys', 'genkeys'],
    'removekey': ['delkey', 'deletekey', 'rmkey'],
    'removekeys': ['delkeys', 'deletekeys', 'rmkeys'],
    'clearkeys': ['purgekeys', 'resetkeys', 'wipekeys'],
    'grimoire': ['guide', 'manual', 'help'],
    'metrics': ['performance', 'status', 'health']
}

class BulkKeysModal(discord.ui.Modal, title="Add Multiple Keys"):
    keys = discord.ui.TextInput(
        label="Enter keys (one per line)",
        style=discord.TextStyle.long,
        placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
        required=True,
        max_length=2000
    )
    
    expires_in = discord.ui.TextInput(
        label="Expiry time in hours (optional)",
        style=discord.TextStyle.short,
        required=False,
        placeholder="24"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            progress_msg = await interaction.followup.send(
                "🔮 Processing keys...", 
                ephemeral=True,
                wait=True
            )
            
            guild_id = interaction.guild.id
            if (guild_config := config.get(guild_id)) is None:
                await progress_msg.edit(content="❌ Run /setup first!")
                return

            # Parse expiry time
            expiry_seconds = None
            if self.expires_in.value:
                try:
                    hours = float(self.expires_in.value)
                    expiry_seconds = int(hours * 3600)
                except ValueError:
                    await progress_msg.edit(content="❌ Invalid expiry time!")
                    return

            # Validate and hash keys
            key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
            valid_hashes = set()
            
            for key in key_list:
                try:
                    uuid_obj = uuid.UUID(key, version=4)
                    if str(uuid_obj) != key.lower():
                        raise ValueError()
                    valid_hashes.add(KeySecurity.hash_key(key, expiry_seconds))
                except ValueError:
                    await progress_msg.edit(
                        content=f"❌ Invalid UUID format: {key[:8]}...\nKeys must be UUIDv4 format!"
                    )
                    return

            # Add valid keys
            await guild_config.bulk_add_keys(valid_hashes, guild_id)
            await save_config()
            stats.log_keys_added(guild_id, len(valid_hashes))
            await audit.log_key_add(interaction, len(valid_hashes))
            
            msg = f"✅ Added {len(valid_hashes)} keys!"
            if expiry_seconds:
                msg += f"\n• Expires in: {self.expires_in.value} hours"
            await progress_msg.edit(content=msg)
            
        except Exception as e:
            logging.error(f"Bulk key add error: {str(e)}")
            await progress_msg.edit(content="❌ Failed to add keys!")

class QueueMetrics:
    def __init__(self):
        self.metrics = defaultdict(lambda: {
            'processed': 0,
            'errors': 0,
            'avg_wait_time': 0.0,
            'total_wait_time': 0.0,
            'requests_waiting': 0,
            'peak_queue_size': 0,
            'last_processed': 0
        })
        self._lock = asyncio.Lock()
    
    async def update(self, guild_id: int, wait_time: float = None, error: bool = False):
        """Update metrics for a guild"""
        async with self._lock:
            m = self.metrics[guild_id]
            m['processed'] += 1
            m['errors'] += int(error)
            m['last_processed'] = time.time()
            
            if wait_time is not None:
                total = m['avg_wait_time'] * (m['processed'] - 1)
                m['total_wait_time'] += wait_time
                m['avg_wait_time'] = (total + wait_time) / m['processed']
    
    async def update_queue_size(self, guild_id: int, size: int):
        """Update queue size metrics"""
        async with self._lock:
            m = self.metrics[guild_id]
            m['requests_waiting'] = size
            m['peak_queue_size'] = max(m['peak_queue_size'], size)
    
    def get_metrics(self, guild_id: int) -> dict:
        """Get current metrics for a guild"""
        return self.metrics[guild_id].copy()

# Initialize queue metrics
queue_metrics = QueueMetrics()

# Initialize key security (near the top with other initializations)
key_security = KeySecurity()

# Add connection error handling
@bot.event
async def on_error(event, *args, **kwargs):
    """Handle connection errors gracefully"""
    error = sys.exc_info()[1]
    if isinstance(error, (HTTPException, GatewayNotFound)):
        logging.error(f"Discord API error in {event}: {str(error)}")
        # Implement exponential backoff for retries
        await backoff.expo(
            bot.connect,
            max_tries=MAX_RETRIES,
            max_time=60
        )
    else:
        logging.error(f"Error in {event}: {str(error)}")

@bot.event
async def on_connect():
    """Log successful connections"""
    logging.info(f"Connected to Discord API with {bot.shard_count} shards")
    logging.info(f"Active connections: {len(bot.http._HTTPClient__session.connector._conns)}")

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
        
    try:
        # Start monitoring
        monitor_performance.start()
        
        # Run bot
        loop.run_until_complete(bot.start(TOKEN))
    except KeyboardInterrupt:
        loop.run_until_complete(bot.close())
    finally:
        monitor_performance.cancel()
        loop.close()