import discord
import json
import os
import logging
import aiofiles
import aiofiles.os
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv
from typing import Dict, Set, Optional
import random
from discord.ext.commands import Cooldown, BucketType
from collections import defaultdict
import asyncio
from passlib.hash import bcrypt_sha256
import time
import uuid

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
    __slots__ = ('role_id', 'valid_keys', 'command', 'success_msgs')
    
    def __init__(self, role_id: int, valid_keys: Set[str], command: str = "claim", 
                 success_msgs: list = None):
        self.role_id = role_id
        self.valid_keys = valid_keys
        self.command = command
        self.success_msgs = success_msgs or DEFAULT_SUCCESS_MESSAGES.copy()

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

# Replace the old cooldown with our new one
claim_cooldown = CustomCooldown(1, 300)  # 1 attempt per 300 seconds (5 minutes)

key_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

async def save_config():
    try:
        # Save main config
        async with aiofiles.open('config.json', 'w') as f:
            serialized = {
                str(guild_id): {
                    "role_id": cfg.role_id,
                    "valid_keys": list(cfg.valid_keys),
                    "command": cfg.command,
                    "success_msgs": cfg.success_msgs
                }
                for guild_id, cfg in config.items()
            }
            await f.write(json.dumps(serialized, indent=4))
        
        # Safely rotate backups
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

async def load_config():
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
                for guild_id, cfg in data.items()  # Removed ["guilds"]
            }
    except FileNotFoundError:
        config = {}

# Command synchronization system
async def sync_commands():
    try:
        # Global sync first
        global_commands = await bot.tree.sync()
        logging.info(f"Synced {len(global_commands)} global commands")
        
        # Sync with all existing guilds
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
    
    logging.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
    
    # Restore guild-specific commands
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

# Maintenance tasks
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
    """Remove expired keys periodically"""
    try:
        now = time.time()
        for guild_id, guild_config in config.items():
            original_count = len(guild_config.valid_keys)
            valid_keys = set()
            
            for full_hash in guild_config.valid_keys:
                try:
                    if '$' in full_hash:
                        _, meta_part = full_hash.rsplit('$', 1)
                        metadata = json.loads(meta_part)
                        if metadata.get('exp') and metadata['exp'] < now:
                            continue  # Skip expired
                        if metadata.get('uses') and metadata['uses'] <= 0:
                            continue  # Skip used up
                    valid_keys.add(full_hash)
                except:
                    valid_keys.add(full_hash)  # Keep malformed hashes
            
            removed = original_count - len(valid_keys)
            if removed > 0:
                guild_config.valid_keys = valid_keys
                await save_config()
                logging.info(f"Removed {removed} expired/used keys from guild {guild_id}")
                
    except Exception as e:
        logging.error(f"Error in key cleanup: {str(e)}")

# Modals
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
            # Defer the response immediately to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            # Send initial status
            progress_msg = await interaction.followup.send(
                "🔮 Setting up your realm...",
                ephemeral=True,
                wait=True
            )
            
            guild_id = interaction.guild.id
            role_name = str(self.role_name)
            command = str(self.command_name).lower().strip()
            
            # Validate command name
            if command in RESERVED_NAMES:
                await progress_msg.edit(content="❌ That command name is reserved!")
                return
            
            if not command.isalnum():
                await progress_msg.edit(content="❌ Command name must be alphanumeric!")
                return
            
            # Update progress
            await progress_msg.edit(content="🔍 Validating role...")
            
            # Find role and check hierarchy
            roles = [r for r in interaction.guild.roles if r.name == role_name]
            if len(roles) > 1:
                await progress_msg.edit(content="❌ Multiple roles with this name exist!")
                return
            
            if not roles:
                await progress_msg.edit(content="❌ Role not found! Create it first.")
                return
                
            target_role = roles[0]
            bot_role = interaction.guild.me.top_role
            
            # Check bot's role position
            if target_role >= bot_role:
                await progress_msg.edit(
                    content="❌ Bot's role must be higher than the target role!\n"
                    f"• Bot's highest role: {bot_role.mention}\n"
                    f"• Target role: {target_role.mention}"
                )
                return
                
            # Check admin's role position
            if not interaction.user.guild_permissions.administrator and target_role >= interaction.user.top_role:
                await progress_msg.edit(
                    content="❌ Your highest role must be above the target role!"
                )
                return
            
            # Update progress
            await progress_msg.edit(content="📝 Processing configuration...")

            # Parse success messages and keys
            success_msgs = []
            if self.success_message.value:
                success_msgs = [msg.strip() for msg in self.success_message.value.split("\n") if msg.strip()]
            
            initial_key_set = set()
            if self.initial_keys.value:
                initial_key_set = {
                    key_security.hash_key(k.strip())
                    for k in self.initial_keys.value.split("\n") 
                    if k.strip()
                }
            
            # Store configuration
            config[guild_id] = GuildConfig(
                target_role.id,
                initial_key_set,
                command,
                success_msgs
            )
            await save_config()
            
            # Update progress
            await progress_msg.edit(content="⚡ Creating command...")
            
            # Create guild-specific command
            await create_dynamic_command(command, guild_id)
            
            # Final success message
            await progress_msg.edit(content=(
                f"✅ Setup complete!\n"
                f"• Command: `/{command}`\n"
                f"• Success messages: {len(success_msgs) or len(DEFAULT_SUCCESS_MESSAGES)}\n"
                f"• Initial keys: {len(initial_key_set)}"
            ))
            
        except Exception as e:
            try:
                await progress_msg.edit(content=f"❌ Setup failed: {str(e)}")
            except Exception:
                logging.error(f"Failed to send setup error message: {str(e)}", exc_info=True)

class BulkKeysModal(discord.ui.Modal, title="Add Multiple Keys"):
    keys = discord.ui.TextInput(
        label="Enter keys (one per line)",
        style=discord.TextStyle.long,
        placeholder="key1\nkey2\nkey3",
        required=True,
        max_length=2000
    )
    
    expires_in = discord.ui.TextInput(
        label="Expiry time (hours, optional)",
        style=discord.TextStyle.short,
        required=False,
        placeholder="24"
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            guild_id = interaction.guild.id
            if (guild_config := config.get(guild_id)) is None:
                await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
                return

            # Parse expiry
            expiry_seconds = None
            if self.expires_in.value:
                try:
                    hours = float(self.expires_in.value)
                    expiry_seconds = int(hours * 3600)
                except ValueError:
                    await interaction.response.send_message("❌ Invalid expiry time!", ephemeral=True)
                    return

            # Hash and add new keys
            key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
            new_hashes = set()
            for key in key_list:
                if not any(key_security.verify_key(key, h) for h in guild_config.valid_keys):
                    new_hashes.add(key_security.hash_key(key, expiry_seconds))
            
            guild_config.valid_keys.update(new_hashes)
            await save_config()
            stats.log_keys_added(guild_id, len(new_hashes))
            await audit.log_key_add(interaction, len(new_hashes))
            
            msg = f"✅ Added {len(new_hashes)} new keys!\n"
            msg += f"• Duplicates skipped: {len(key_list)-len(new_hashes)}\n"
            msg += f"• Total keys: {len(guild_config.valid_keys)}"
            if expiry_seconds:
                msg += f"\n• Expires in: {self.expires_in.value} hours"
            
            await interaction.response.send_message(msg, ephemeral=True)
                
        except Exception as e:
            logging.error(f"Key addition error: {str(e)}")
            await interaction.response.send_message(
                "❌ Failed to add keys!",
                ephemeral=True
            )

class RemoveKeysModal(discord.ui.Modal, title="Remove Keys"):
    keys = discord.ui.TextInput(
        label="Keys to remove (one per line)",
        style=discord.TextStyle.long,
        required=True,
        max_length=2000
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        removed = 0
        
        # Find and remove matching hashed keys
        for key in key_list:
            for hash_ in list(guild_config.valid_keys):  # Use list to avoid modification during iteration
                if key_security.verify_key(key, hash_):
                    guild_config.valid_keys.remove(hash_)
                    removed += 1
                    break
        
        await save_config()
        await audit.log_key_remove(interaction, removed)

        await interaction.response.send_message(
            f"✅ Removed {removed} keys!\n"
            f"• Not found: {len(key_list)-removed}\n"
            f"• Remaining: {len(guild_config.valid_keys)}",
            ephemeral=True
        )

class ArcaneGatewayModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="🔮 Arcane Gateway")
        
    key = discord.ui.TextInput(
        label="SPEAK THE ANCIENT RUNE",
        placeholder="Enter your mystical key...",
        style=discord.TextStyle.short,
        required=True,
        min_length=1,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        start_time = time.time()
        guild_id = interaction.guild.id
        
        try:
            await interaction.response.defer(ephemeral=True)
            
            progress_msg = await interaction.followup.send(
                "🔮 Channeling mystical energies...",
                ephemeral=True,
                wait=True
            )
            
            # Clean the key input
            key = str(self.key).strip()
            if not key:
                stats.log_claim(guild_id, success=False)
                await progress_msg.edit(content="❌ Invalid key format!")
                return
            
            # Check cooldown first
            if retry_after := claim_cooldown.get_retry_after(interaction):
                stats.log_claim(guild_id, success=False)
                await progress_msg.edit(
                    content=f"⏳ The portal is still cooling down... Try again in {retry_after:.1f}s"
                )
                return
                
            # Update progress while verifying
            await progress_msg.edit(content="✨ Verifying ancient runes...")
            
            # Process the claim
            if (guild_config := config.get(interaction.guild.id)) is None:
                await progress_msg.edit(content="🔧 Server not configured! Use /setup first")
                return
                
            # Thread-safe key operations
            async with key_locks[guild_id]:
                # Check role
                role = interaction.guild.get_role(guild_config.role_id)
                if not role:
                    await progress_msg.edit(content="👻 Role missing - contact admin!")
                    return
                    
                if role >= interaction.guild.me.top_role:
                    await progress_msg.edit(content="📛 Bot needs higher role position!")
                    return
                    
                if role in interaction.user.roles:
                    await progress_msg.edit(content="🎭 You already have this role!")
                    return
                
                # Update progress while checking key
                await progress_msg.edit(content="🔍 Consulting the ancient tomes...")
                
                # Verify key
                valid_hash = None
                for hash_ in guild_config.valid_keys:
                    if key_security.verify_key(key, hash_):
                        valid_hash = hash_
                        break
                        
                if not valid_hash:
                    await audit.log_claim(interaction, key[:3], success=False)
                    await progress_msg.edit(content="🔑 Invalid or expired key!")
                    return
                    
                # Update progress while granting role
                await progress_msg.edit(content="⚡ Channeling powers...")
                
                # Process claim
                await interaction.user.add_roles(role)
                guild_config.valid_keys.remove(valid_hash)
                await save_config()
                await audit.log_claim(interaction, key[:3], success=True)
                
                # Reset cooldown on successful claim
                claim_cooldown.reset_cooldown(interaction)
                
                # Get random success message
                template = random.choice(guild_config.success_msgs)
                formatted = template.format(
                    user=interaction.user.mention,
                    role=role.mention,
                    key=f"`{key[:3]}...`"
                )
                
                # Final success message
                await progress_msg.edit(content=f"✨ {formatted} ✨")
                
                # Calculate claim time and log success
                claim_time = time.time() - start_time
                stats.log_claim(guild_id, success=True, claim_time=claim_time)
                
        except Exception as e:
            claim_time = time.time() - start_time
            stats.log_claim(guild_id, success=False)
            try:
                await progress_msg.edit(content="❌ A mystical disturbance prevents this action!")
                logging.error(f"Claim error: {str(e)}", exc_info=True)
            except Exception:
                pass

# Add this helper
def require_setup():
    def decorator(func):
        async def wrapper(interaction: discord.Interaction):
            if (guild_config := config.get(interaction.guild.id)) is None:
                await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
                return
            return await func(interaction)
        wrapper.__name__ = func.__name__  # Preserve function name
        return wrapper  # Return wrapper instead of decorator
    return decorator

# Command descriptions
ADMIN_COMMANDS = {
    "setup": "⚙️ Initial server setup and configuration",
    "addkey": "🔑 Add a single key",
    "addkeys": "📥 Bulk add multiple keys",
    "removekey": "🗑️ Remove a single key",
    "removekeys": "📤 Bulk remove multiple keys",
    "clearkeys": "💣 Clear all keys",
    "keys": "📊 Check available keys",
    "sync": "♻️ Sync bot commands",
    "stats": "📊 View realm statistics (Admin only)"
}

MEMBER_COMMANDS = {
    # This will be dynamically added based on server config
    # "openportal": "🌀 Use your key to unlock the role"
}

# Commands
@bot.tree.command(name="sync", description="♻️ Sync commands (Admin only)")
@app_commands.default_permissions(administrator=True)
async def sync(interaction: discord.Interaction):
    """Manual command sync handler"""
    try:
        await sync_commands()
        await interaction.response.send_message(
            "✅ Commands synchronized successfully!",
            ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(
            f"❌ Sync failed: {str(e)}",
            ephemeral=True
        )

@bot.tree.command(name="setup", description="Initial server setup")
@app_commands.default_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    await interaction.response.send_modal(SetupModal())

@bot.tree.command(name="addkey", description="Add single key")
@app_commands.default_permissions(administrator=True)
async def addkey(interaction: discord.Interaction, key: str, expires_in: Optional[int] = None):
    """Add a key with optional expiry time in hours"""
    guild_id = interaction.guild.id
    if (guild_config := config.get(guild_id)) is None:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    # Convert hours to seconds if expiry provided
    expiry_seconds = expires_in * 3600 if expires_in else None
    
    # Check if key already exists
    if any(key_security.verify_key(key, h) for h in guild_config.valid_keys):
        await interaction.response.send_message("❌ Key exists!", ephemeral=True)
        return

    # Store hashed key with expiry
    hashed = key_security.hash_key(key, expiry_seconds)
    guild_config.valid_keys.add(hashed)
    await save_config()
    stats.log_keys_added(guild_id, 1)
    await audit.log_key_add(interaction, 1)
    
    msg = "✅ Key added!"
    if expires_in:
        msg += f"\n• Expires in: {expires_in} hours"
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="addkeys", description="Bulk add keys")
@app_commands.default_permissions(administrator=True)
async def addkeys(interaction: discord.Interaction):
    await interaction.response.send_modal(BulkKeysModal())

@bot.tree.command(name="removekey", description="Remove single key")
@app_commands.default_permissions(administrator=True)
async def removekey(interaction: discord.Interaction, key: str):
    guild_id = interaction.guild.id
    if (guild_config := config.get(guild_id)) is None:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    removed = False
    # Iterate through all hashes to find matching key
    for hash_ in list(guild_config.valid_keys):
        if key_security.verify_key(key, hash_):
            guild_config.valid_keys.discard(hash_)
            removed = True
            break
    
    if not removed:
        await interaction.response.send_message("❌ Key not found!", ephemeral=True)
        return

    await save_config()
    await audit.log_key_remove(interaction, 1)
    await interaction.response.send_message("✅ Key removed!", ephemeral=True)

@bot.tree.command(name="removekeys", description="Bulk remove keys")
@app_commands.default_permissions(administrator=True)
async def removekeys(interaction: discord.Interaction):
    await interaction.response.send_modal(RemoveKeysModal())

@bot.tree.command(name="clearkeys", description="Clear ALL valid keys (admin only)")
@app_commands.default_permissions(administrator=True)
async def clearkeys(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if (guild_config := config.get(guild_id)) is None:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    class ClearConfirmView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)

        @discord.ui.button(label="CONFIRM CLEAR ALL KEYS", style=discord.ButtonStyle.danger)
        async def confirm(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            if button_interaction.user != interaction.user:
                return
                
            count = len(guild_config.valid_keys)
            guild_config.valid_keys.clear()
            await save_config()
            await audit.log_key_remove(interaction, count)
            await button_interaction.response.edit_message(
                content=f"✅ Cleared {count} keys!",
                view=None
            )

    await interaction.response.send_message(
        "⚠️ This will delete ALL keys! Click to confirm:",
        view=ClearConfirmView(),
        ephemeral=True
    )

@bot.tree.command(name="keys", description="Check available keys (Admin only)")
@app_commands.default_permissions(administrator=True)
@require_setup()
async def keys(interaction: discord.Interaction):
    guild_config = config[interaction.guild.id]
    role = interaction.guild.get_role(guild_config.role_id)
    
    # Count expired keys
    now = time.time()
    expired = sum(1 for h in guild_config.valid_keys 
                 if not key_security.verify_key(str(now), h))
    
    msg = (
        f"🔑 **Key Status**\n"
        f"• Available keys: {len(guild_config.valid_keys)}\n"
        f"• Expired keys: {expired}\n"
        f"• Active keys: {len(guild_config.valid_keys) - expired}\n"
        f"• Target role: {role.mention if role else '❌ Role not found!'}"
    )
    
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="grimoire", description="📚 Reveal the ancient tomes of knowledge")
async def grimoire(interaction: discord.Interaction):
    is_admin = interaction.user.guild_permissions.administrator
    guild_id = interaction.guild.id
    
    embed = discord.Embed(
        title="🔮 Realm Keeper Commands",
        color=discord.Color.blurple()
    )
    
    if is_admin:
        admin_cmds = "\n".join(f"• `/{cmd}` - {desc}" for cmd, desc in ADMIN_COMMANDS.items())
        embed.add_field(
            name="🛡️ Admin Commands",
            value=admin_cmds,
            inline=False
        )
    
    # Show member commands (including custom command if configured)
    member_cmds = MEMBER_COMMANDS.copy()
    if guild_id in config:
        custom_cmd = config[guild_id].command
        member_cmds[custom_cmd] = "🌀 Use your key to unlock the role"
    
    if member_cmds:
        member_list = "\n".join(f"• `/{cmd}` - {desc}" for cmd, desc in member_cmds.items())
        embed.add_field(
            name="📜 Member Commands",
            value=member_list,
            inline=False
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="stats", description="📊 View realm statistics (Admin only)")
@app_commands.default_permissions(administrator=True)
@require_setup()
async def view_stats(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    guild_stats = stats.get_stats(guild_id)
    guild_config = config[guild_id]
    
    embed = discord.Embed(
        title="📊 Realm Statistics",
        color=discord.Color.blue()
    )
    
    # Claims stats
    claims = (
        f"• Total attempts: {guild_stats['total_claims']}\n"
        f"• Successful: {guild_stats['successful_claims']}\n"
        f"• Failed: {guild_stats['failed_claims']}\n"
        f"• Success rate: {guild_stats['success_rate']:.1f}%\n"
        f"• Last claim: {format_time_ago(guild_stats['time_since_last_claim'])}"
    )
    embed.add_field(name="🎯 Claims", value=claims, inline=False)
    
    # Performance stats
    if guild_stats['claim_count'] > 0:
        perf = (
            f"• Average time: {guild_stats['avg_claim_time']:.1f}s\n"
            f"• Fastest: {guild_stats['fastest_claim']:.1f}s\n"
            f"• Slowest: {guild_stats['slowest_claim']:.1f}s"
        )
        embed.add_field(name="⚡ Performance", value=perf, inline=False)
    
    # Keys stats
    keys = (
        f"• Available: {len(guild_config.valid_keys)}\n"
        f"• Added: {guild_stats['keys_added']}\n"
        f"• Removed: {guild_stats['keys_removed']}\n"
        f"• Last added: {format_time_ago(guild_stats['time_since_last_key'])}"
    )
    embed.add_field(name="🔑 Keys", value=keys, inline=False)
    
    # Active cooldowns
    cooldowns = claim_cooldown.get_cooldowns_for_guild(guild_id)
    embed.add_field(
        name="⏳ Cooldowns",
        value=f"• Active: {cooldowns}",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

def format_time_ago(seconds: float) -> str:
    """Format time difference as human readable string"""
    if not seconds:
        return "Never"
        
    if seconds < 60:
        return f"{seconds:.0f}s ago"
    elif seconds < 3600:
        return f"{seconds/60:.0f}m ago"
    elif seconds < 86400:
        return f"{seconds/3600:.0f}h ago"
    else:
        return f"{seconds/86400:.0f}d ago"

@bot.event
async def on_error(event, *args, **kwargs):
    logging.error(f"Error in {event}: {args} {kwargs}")

@bot.event
async def on_command_error(ctx, error):
    logging.error(f"Command error: {error}")

async def create_dynamic_command(name: str, guild_id: int):
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            raise ValueError(f"Guild {guild_id} not found")

        # Use persistent callback naming
        cmd_name = f"dynamic_cmd_{guild_id}"
        
        # Remove existing command if it exists
        old_command = bot.tree.get_command(name, guild=discord.Object(id=guild_id))
        if old_command:
            bot.tree.remove_command(name, guild=discord.Object(id=guild_id))
            logging.info(f"Removed old command /{name} for guild {guild_id}")
            
        @bot.tree.command(name=name, description="🌟 Unlock your mystical powers", guild=discord.Object(id=guild_id))
        async def dynamic_claim_wrapper(interaction: discord.Interaction):
            await interaction.response.send_modal(ArcaneGatewayModal())
        
        # Store reference to prevent garbage collection
        setattr(bot, cmd_name, dynamic_claim_wrapper)
            
        await bot.tree.sync(guild=discord.Object(id=guild_id))
        logging.info(f"Created command /{name} for guild {guild_id}")
        
    except Exception as e:
        logging.error(f"Failed to create command /{name}: {e}")
        raise

# Add after config class
class KeySecurity:
    @staticmethod
    def hash_key(key: str, expiry: int = None, max_uses: int = None) -> str:
        """Hash a key with metadata stored in the hash string"""
        metadata = {}
        if expiry:
            metadata['exp'] = int(time.time()) + expiry
        if max_uses:
            metadata['uses'] = max_uses
        
        # Store metadata as JSON in hash string
        hash_str = bcrypt_sha256.hash(key)
        return f"{hash_str}${json.dumps(metadata)}" if metadata else hash_str

    @staticmethod
    def verify_key(key: str, full_hash: str, guild_config: GuildConfig = None) -> bool:
        """Verify a key and handle metadata"""
        try:
            if '$' in full_hash:
                hash_part, meta_part = full_hash.rsplit('$', 1)
                metadata = json.loads(meta_part)
                
                # Check expiration
                if metadata.get('exp') and metadata['exp'] < time.time():
                    return False
                
                # Check and update uses
                if 'uses' in metadata:
                    if metadata['uses'] <= 0:
                        return False
                    if guild_config:
                        metadata['uses'] -= 1
                        new_hash = f"{hash_part}${json.dumps(metadata)}"
                        guild_config.valid_keys.remove(full_hash)
                        if metadata['uses'] > 0:
                            guild_config.valid_keys.add(new_hash)
                
                return bcrypt_sha256.verify(key, hash_part)
            else:
                # Legacy hash without metadata
                return bcrypt_sha256.verify(key, full_hash)
        except Exception:
            # Fallback for malformed hashes
            return bcrypt_sha256.verify(key, full_hash)

# Initialize security
key_security = KeySecurity()

# Add after imports
class AuditLogger:
    def __init__(self):
        self.logger = logging.getLogger('audit')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        handler = logging.FileHandler('audit.log')
        handler.setFormatter(
            logging.Formatter('%(asctime)s | %(message)s')
        )
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
    
    async def log_key_remove(self, interaction: discord.Interaction, count: int):
        self.logger.info(
            f"REMOVE_KEYS | Admin: {interaction.user} ({interaction.user.id}) | "
            f"Guild: {interaction.guild.name} ({interaction.guild.id}) | "
            f"Count: {count}"
        )

# Initialize logger
audit = AuditLogger()

# Add after AuditLogger
class StatsTracker:
    def __init__(self):
        self.stats: Dict[int, Dict[str, int]] = defaultdict(lambda: {
            'total_claims': 0,
            'successful_claims': 0,
            'failed_claims': 0,
            'keys_added': 0,
            'keys_removed': 0,
            'last_claim': 0,  # Timestamp of last claim
            'last_key_add': 0,  # Timestamp of last key addition
            'fastest_claim': float('inf'),  # Fastest successful claim time
            'slowest_claim': 0,  # Slowest successful claim time
            'total_claim_time': 0,  # Total time spent claiming
            'claim_count': 0  # Number of claims for averaging
        })
        self.load_stats()
    
    async def save_stats(self):
        """Save stats to file"""
        async with aiofiles.open('stats.json', 'w') as f:
            await f.write(json.dumps({
                str(guild_id): data 
                for guild_id, data in self.stats.items()
            }, indent=4))
    
    def load_stats(self):
        """Load stats from file"""
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
        """Log a claim attempt with timing"""
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
    
    def get_stats(self, guild_id: int) -> dict:
        stats = self.stats[guild_id]
        total_claims = stats['total_claims']
        
        # Calculate additional metrics
        success_rate = (stats['successful_claims'] / total_claims * 100) if total_claims else 0
        avg_claim_time = (stats['total_claim_time'] / stats['claim_count']) if stats['claim_count'] else 0
        
        return {
            **stats,
            'success_rate': success_rate,
            'avg_claim_time': avg_claim_time,
            'time_since_last_claim': time.time() - stats['last_claim'] if stats['last_claim'] else 0,
            'time_since_last_key': time.time() - stats['last_key_add'] if stats['last_key_add'] else 0
        }

# Initialize tracker
stats = StatsTracker()

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    bot.run(TOKEN)