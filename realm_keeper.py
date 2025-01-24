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

bot = commands.AutoShardedBot(
    command_prefix="!",
    intents=intents,
    case_insensitive=True,
    max_messages=10000
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
        
        # Initialize quick lookup
        for h in valid_keys:
            quick_hash = hashlib.sha256(h.encode()).hexdigest()[:8]
            self.quick_lookup[quick_hash].add(h)
    
    async def add_key(self, full_hash: str, guild_id: int):
        """Add a key with cache invalidation and warmup"""
        self.main_store.add(full_hash)
        quick_hash = hashlib.sha256(full_hash.encode()).hexdigest()[:8]
        self.quick_lookup[quick_hash].add(full_hash)
        
        # Invalidate and rewarm cache
        await key_cache.invalidate(guild_id)
        await key_cache.warm_cache(guild_id, self)
    
    async def remove_key(self, full_hash: str, guild_id: int):
        """Remove a key with cache invalidation and warmup"""
        self.main_store.discard(full_hash)
        quick_hash = hashlib.sha256(full_hash.encode()).hexdigest()[:8]
        self.quick_lookup[quick_hash].discard(full_hash)
        
        # Invalidate and rewarm cache
        await key_cache.invalidate(guild_id)
        await key_cache.warm_cache(guild_id, self)
    
    async def bulk_add_keys(self, hashes: Set[str], guild_id: int):
        """Add multiple keys with single cache update"""
        for full_hash in hashes:
            quick_hash = hashlib.sha256(full_hash.encode()).hexdigest()[:8]
            self.quick_lookup[quick_hash].add(full_hash)
        self.main_store.update(hashes)
        
        # Invalidate and rewarm cache once
        await key_cache.invalidate(guild_id)
        await key_cache.warm_cache(guild_id, self)

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
        self._lock = asyncio.Lock()
    
    async def warm_cache(self, guild_id: int, guild_config: GuildConfig):
        """Pre-compute quick hashes for a guild"""
        quick_lookup = defaultdict(set)
        for full_hash in guild_config.main_store:
            # Handle metadata in hash
            if KeySecurity.DELIMITER in full_hash:
                hash_part = full_hash.split(KeySecurity.DELIMITER)[0]
            else:
                hash_part = full_hash
            quick_hash = hashlib.sha256(hash_part.encode()).hexdigest()[:8]
            quick_lookup[quick_hash].add(full_hash)
            
        async with self._lock:
            self.quick_lookup[guild_id] = quick_lookup
            self.last_update[guild_id] = time.time()
    
    async def invalidate(self, guild_id: int):
        """Force cache invalidation for a guild"""
        async with self._lock:
            self.quick_lookup.pop(guild_id, None)

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

async def create_dynamic_command(command_name: str, guild_id: int):
    guild = bot.get_guild(guild_id)
    if not guild:
        return

    @app_commands.command(name=command_name, description="Claim your role with a key")
    @app_commands.describe(key="Your mystical key")
    async def dynamic_claim(interaction: discord.Interaction, key: str):
        await interaction.response.send_modal(ArcaneGatewayModal(key))

    bot.tree.add_command(dynamic_claim, guild=guild)

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
    await load_config()
    cleanup_task.start()
    save_stats_task.start()
    memory_check.start()
    monitor_workers.start()
    
    logging.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    
    restored = 0
    for guild_id, guild_config in config.items():
        try:
            await create_dynamic_command(guild_config.command, guild_id)
            restored += 1
        except Exception as e:
            logging.error(f"Failed to restore command for guild {guild_id}: {e}")
    
    logging.info(f"Restored {restored} custom commands")
    
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="for /claim commands"
        )
    )
    
    await sync_commands()
    logging.info(f"Connected to {len(bot.guilds)} servers")

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
            progress_msg = await interaction.followup.send("🔮 Setting up your realm...", ephemeral=True, wait=True)
            
            guild_id = interaction.guild.id
            role_name = str(self.role_name)
            command = str(self.command_name).lower().strip()
            
            if command in RESERVED_NAMES:
                await progress_msg.edit(content="❌ That command name is reserved!")
                return
            
            if not command.isalnum():
                await progress_msg.edit(content="❌ Command name must be alphanumeric!")
                return
            
            await progress_msg.edit(content="🔍 Validating role...")
            roles = [r for r in interaction.guild.roles if r.name == role_name]
            
            if len(roles) > 1:
                await progress_msg.edit(content="❌ Multiple roles with this name exist!")
                return
            
            if not roles:
                await progress_msg.edit(content="❌ Role not found! Create it first.")
                return
                
            target_role = roles[0]
            bot_role = interaction.guild.me.top_role
            
            if target_role >= bot_role:
                await progress_msg.edit(
                    content=f"❌ Bot's role must be higher than the target role!\n• Bot's highest role: {bot_role.mention}\n• Target role: {target_role.mention}"
                )
                return
                
            if not interaction.user.guild_permissions.administrator and target_role >= interaction.user.top_role:
                await progress_msg.edit(content="❌ Your highest role must be above the target role!")
                return
            
            await progress_msg.edit(content="📝 Processing configuration...")
            success_msgs = []
            if self.success_message.value:
                success_msgs = [msg.strip() for msg in self.success_message.value.split("\n") if msg.strip()]
            
            initial_key_set = set()
            if self.initial_keys.value:
                key_list = [k.strip() for k in self.initial_keys.value.split("\n") if k.strip()]
                for key in key_list:
                    try:
                        uuid_obj = uuid.UUID(key, version=4)
                        if str(uuid_obj) == key.lower():
                            initial_key_set.add(KeySecurity.hash_key(key))
                    except ValueError:
                        await progress_msg.edit(content=f"❌ Invalid UUID format: {key[:8]}...\nKeys must be UUIDv4 format!")
                        return
            
            config[guild_id] = GuildConfig(
                target_role.id,
                initial_key_set,
                command,
                success_msgs
            )
            await save_config()
            
            if initial_key_set:
                stats.log_keys_added(guild_id, len(initial_key_set))
                await audit.log_key_add(interaction, len(initial_key_set))
            
            await progress_msg.edit(content="⚡ Creating command...")
            await create_dynamic_command(command, guild_id)
            
            await progress_msg.edit(content=(
                f"✅ Setup complete!\n• Command: `/{command}`\n• Success messages: {len(success_msgs) or len(DEFAULT_SUCCESS_MESSAGES)}\n• Initial keys: {len(initial_key_set)}"
            ))
            
        except Exception as e:
            try:
                await progress_msg.edit(content=f"❌ Setup failed: {str(e)}")
            except Exception:
                logging.error(f"Failed to send setup error message: {str(e)}", exc_info=True)

class ArcaneGatewayModal(discord.ui.Modal, title="Enter Mystical Key"):
    key = discord.ui.TextInput(
        label="Arcane Key",
        placeholder="xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx",
        style=discord.TextStyle.short,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            progress_msg = await interaction.followup.send("🔮 Channeling mystical energies...", ephemeral=True, wait=True)
            
            guild_id = interaction.guild.id
            user = interaction.user
            key_value = self.key.value.strip()
            
            guild_config = config.get(guild_id)
            if not guild_config:
                await progress_msg.edit(content="❌ Server not configured!")
                return

            retry_after = claim_cooldown.get_retry_after(interaction)
            if retry_after:
                await progress_msg.edit(content=f"⏳ Cooldown active. Try again in {int(retry_after)} seconds.")
                return

            start_time = time.time()
            is_valid = False
            
            async with key_locks[guild_id][get_shard(user.id)]:
                for full_hash in guild_config.main_store:
                    if await KeySecurity.verify_key_async(key_value, full_hash, guild_config):
                        await guild_config.remove_key(full_hash, guild_id)
                        is_valid = True
                        break

            if not is_valid:
                stats.log_claim(guild_id, False)
                await audit.log_claim(interaction, key_value, False)
                await progress_msg.edit(content="❌ Invalid key or already claimed!")
                return

            role = interaction.guild.get_role(guild_config.role_id)
            try:
                await user.add_roles(role)
            except discord.Forbidden:
                await progress_msg.edit(content="❌ Bot lacks permissions to assign role!")
                return

            msg_template = random.choice(guild_config.success_msgs or DEFAULT_SUCCESS_MESSAGES)
            await progress_msg.edit(content=msg_template.format(user=user.mention, role=role.name))

            claim_time = time.time() - start_time
            stats.log_claim(guild_id, True, claim_time)
            await audit.log_claim(interaction, key_value, True)

        except Exception as e:
            logging.error(f"Claim error: {str(e)}")
            await progress_msg.edit(content="❌ A mystical disturbance prevents this action!")

class KeySecurity:
    DELIMITER = "◆"
    
    @staticmethod
    def hash_key(key: str, expiry: int = None, max_uses: int = None) -> str:
        try:
            uuid_obj = uuid.UUID(key, version=4)
            if str(uuid_obj) != key.lower():
                raise ValueError("Not a valid UUIDv4")
        except ValueError as e:
            raise ValueError(f"Invalid UUID format: {str(e)}")
            
        metadata = {}
        if expiry:
            metadata['exp'] = int(time.time()) + expiry
        if max_uses:
            metadata['uses'] = max_uses
        
        hash_str = bcrypt_sha256.hash(key)
        return f"{hash_str}{KeySecurity.DELIMITER}{json.dumps(metadata)}" if metadata else hash_str

    @staticmethod
    def verify_key(key: str, full_hash: str, guild_config: GuildConfig = None) -> bool:
        try:
            uuid_obj = uuid.UUID(key, version=4)
            if str(uuid_obj) != key.lower():
                return False
                
            if KeySecurity.DELIMITER in full_hash:
                hash_part, meta_part = full_hash.split(KeySecurity.DELIMITER, 1)
                metadata = json.loads(meta_part)
                
                if metadata.get('exp') and metadata['exp'] < time.time():
                    return False
                
                if 'uses' in metadata:
                    if metadata['uses'] <= 0:
                        return False
                    if guild_config:
                        metadata['uses'] -= 1
                        new_hash = f"{hash_part}{KeySecurity.DELIMITER}{json.dumps(metadata)}"
                        guild_config.main_store.discard(full_hash)
                        if metadata['uses'] > 0:
                            guild_config.main_store.add(new_hash)
                
                return bcrypt_sha256.verify(key, hash_part)
            else:
                return bcrypt_sha256.verify(key, full_hash)
        except (ValueError, Exception):
            return False

    @staticmethod
    async def verify_key_async(key: str, full_hash: str, guild_config: GuildConfig = None) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            worker_pool.pool,
            KeySecurity.verify_key,
            key,
            full_hash,
            guild_config
        )

class AdaptiveWorkerPool:
    def __init__(self, min_workers: int = 4, max_workers: int = 32):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.current_workers = min_workers
        self.pool = ThreadPoolExecutor(
            max_workers=min_workers,
            thread_name_prefix="bcrypt_worker"
        )
        self.queue_high = 50
        self.queue_low = 10
        self._lock = asyncio.Lock()
    
    async def adjust_workers(self, queue_size: int, cpu_percent: float):
        async with self._lock:
            if queue_size > self.queue_high and cpu_percent < 90:
                if self.current_workers < self.max_workers:
                    self.current_workers = min(self.max_workers, self.current_workers + 2)
                    self.pool._max_workers = self.current_workers
                    logging.info(f"Scaled up workers to {self.current_workers}")
                    
            elif queue_size < self.queue_low and self.current_workers > self.min_workers:
                self.current_workers = max(self.min_workers, self.current_workers - 1)
                self.pool._max_workers = self.current_workers
                logging.info(f"Scaled down workers to {self.current_workers}")
    
    def submit(self, fn, *args):
        return self.pool.submit(fn, *args)

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

class StatsTracker:
    def __init__(self):
        self.stats = defaultdict(lambda: {
            'total_claims': 0,
            'successful_claims': 0,
            'failed_claims': 0,
            'keys_added': 0,
            'keys_removed': 0,
            'last_claim': 0,
            'last_key_add': 0,
            'fastest_claim': float('inf'),
            'slowest_claim': 0,
            'total_claim_time': 0,
            'claim_count': 0
        })
        self.load_stats()
    
    async def save_stats(self):
        async with aiofiles.open('stats.json', 'w') as f:
            await f.write(json.dumps({
                str(guild_id): data 
                for guild_id, data in self.stats.items()
            }, indent=4))
    
    def load_stats(self):
        try:
            with open('stats.json', 'r') as f:
                data = json.loads(f.read())
                self.stats.update({
                    int(guild_id): stats_data
                    for guild_id, stats_data in data.items()
                })
        except FileNotFoundError:
            pass
    
    def log_claim(self, guild_id: int, success: bool, claim_time: float = None):
        now = time.time()
        guild_stats = self.stats[guild_id]
        guild_stats['total_claims'] += 1
        guild_stats['last_claim'] = now
        
        if success:
            guild_stats['successful_claims'] += 1
            if claim_time:
                guild_stats['claim_count'] += 1
                guild_stats['total_claim_time'] += claim_time
                guild_stats['fastest_claim'] = min(guild_stats['fastest_claim'], claim_time)
                guild_stats['slowest_claim'] = max(guild_stats['slowest_claim'], claim_time)
        else:
            guild_stats['failed_claims'] += 1
    
    def log_keys_added(self, guild_id: int, count: int):
        guild_stats = self.stats[guild_id]
        guild_stats['keys_added'] += count
        guild_stats['last_key_add'] = time.time()

stats = StatsTracker()

class AuditLogger:
    def __init__(self):
        self.logger = logging.getLogger('audit')
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('audit.log')
        handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
        self.logger.addHandler(handler)
    
    async def log_claim(self, interaction: discord.Interaction, key: str, success: bool):
        self.logger.info(
            f"CLAIM | User: {interaction.user} ({interaction.user.id}) | "
            f"Guild: {interaction.guild.name} ({interaction.guild.id}) | "
            f"Key: {key[:3]}... | Success: {success}"
        )
    
    async def log_key_add(self, interaction: discord.Interaction, count: int):
        self.logger.info(
            f"ADD_KEYS | Admin: {interaction.user} ({interaction.user.id}) | "
            f"Guild: {interaction.guild.name} ({interaction.guild.id}) | "
            f"Count: {count}"
        )

audit = AuditLogger()

@bot.tree.command(name="addkey", description="Add single key")
@app_commands.default_permissions(administrator=True)
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
    
    if any(key_security.verify_key(key, h) for h in guild_config.main_store):
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
        
    total_keys = len(guild_config.main_store)
    expired = sum(1 for h in guild_config.main_store 
                 if KeySecurity.DELIMITER in h and 
                 json.loads(h.split(KeySecurity.DELIMITER)[1]).get('exp', 0) < time.time())
    
    guild_stats = stats.stats[guild_id]
    embed = discord.Embed(
        title="🔑 Key Statistics",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="Keys",
        value=f"• Total: {total_keys}\n• Expired: {expired}\n• Added: {guild_stats['keys_added']}"
    )
    embed.add_field(
        name="Claims",
        value=f"• Total: {guild_stats['total_claims']}\n• Successful: {guild_stats['successful_claims']}\n• Failed: {guild_stats['failed_claims']}"
    )
    if guild_stats['claim_count'] > 0:
        avg_time = guild_stats['total_claim_time'] / guild_stats['claim_count']
        embed.add_field(
            name="Timing",
            value=f"• Average: {avg_time:.2f}s\n• Fastest: {guild_stats['fastest_claim']:.2f}s\n• Slowest: {guild_stats['slowest_claim']:.2f}s"
        )
    
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
    
    await interaction.response.send_message(
        f"✅ Cleared {key_count} keys!",
        ephemeral=True
    )

@bot.tree.command(name="removekey", description="🗑️ Remove a specific key")
@app_commands.default_permissions(administrator=True)
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
        if key_security.verify_key(key, full_hash):
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
    
    # Admin commands
    admin = (
        "`/setup` - Configure the bot for your server\n"
        "`/addkey` - Add a single key\n"
        "`/addkeys` - Add multiple keys at once\n"
        "`/removekey` - Remove a specific key\n"
        "`/removekeys` - Remove multiple keys\n"
        "`/clearkeys` - Remove all keys\n"
        "`/keys` - View key statistics\n"
        "`/sync` - Force sync commands\n"
        "`/metrics` - View performance metrics"
    )
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
                    if key_security.verify_key(key, full_hash):
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

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    bot.run(TOKEN)