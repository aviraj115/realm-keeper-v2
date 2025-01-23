import discord
import json
import os
import uuid
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
from threading import Lock
from cryptography.fernet import Fernet
import base64
from passlib.hash import bcrypt_sha256
import time

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

key_locks: Dict[int, Lock] = defaultdict(Lock)

async def save_config():
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
    
    # Rotate backups (keep last 3)
    for i in range(2, -1, -1):
        src = f'config.json{"." + str(i) if i else ""}'
        dest = f'config.json.{i+1}'
        if await aiofiles.os.path.exists(src):
            await aiofiles.os.replace(src, dest)

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
            
            # Find role
            roles = [r for r in interaction.guild.roles if r.name == role_name]
            if len(roles) > 1:
                await progress_msg.edit(content="❌ Multiple roles with this name exist!")
                return
            
            if not roles:
                await progress_msg.edit(content="❌ Role not found! Create it first.")
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
                roles[0].id,
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

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        # Hash and add new keys
        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        new_hashes = set()
        for key in key_list:
            if not any(key_security.verify_key(key, h) for h in guild_config.valid_keys):
                new_hashes.add(key_security.hash_key(key))
        
        guild_config.valid_keys.update(new_hashes)
        await save_config()
        await audit.log_key_add(interaction, len(new_hashes))

        await interaction.response.send_message(
            f"✅ Added {len(new_hashes)} new keys!\n"
            f"• Duplicates skipped: {len(key_list)-len(new_hashes)}\n"
            f"• Total keys: {len(guild_config.valid_keys)}",
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
        # Get custom title from config if available
        super().__init__(title="🔮 Arcane Gateway")
        
    key = discord.ui.TextInput(
        label="Speak the Ancient Rune",
        placeholder="Enter your mystical key...",
        style=discord.TextStyle.short,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await process_claim(interaction, str(self.key))

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
    "sync": "♻️ Sync bot commands"
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
async def addkey(interaction: discord.Interaction, key: str):
    guild_id = interaction.guild.id
    if (guild_config := config.get(guild_id)) is None:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    # Check if key already exists
    if any(key_security.verify_key(key, h) for h in guild_config.valid_keys):
        await interaction.response.send_message("❌ Key exists!", ephemeral=True)
        return

    # Store hashed key
    hashed = key_security.hash_key(key)
    guild_config.valid_keys.add(hashed)
    await save_config()
    await audit.log_key_add(interaction, 1)
    await interaction.response.send_message("✅ Key added!", ephemeral=True)

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
async def keys(interaction: discord.Interaction):  # Only takes interaction
    guild_config = config[interaction.guild.id]  # Get config inside function
    role = interaction.guild.get_role(guild_config.role_id)
    await interaction.response.send_message(
        f"🔑 **Key Status**\n"
        f"• Available keys: {len(guild_config.valid_keys)}\n"
        f"• Target role: {role.mention if role else '❌ Role not found!'}",
        ephemeral=True
    )

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

async def process_claim(interaction: discord.Interaction, key: str):
    try:
        # Check cooldown first (admins are exempt)
        if retry_after := claim_cooldown.get_retry_after(interaction):
            raise commands.CommandOnCooldown(None, retry_after, BucketType.user)

        if (guild_config := config.get(interaction.guild.id)) is None:
            raise ValueError("Server not configured")
            
        # Thread-safe key operations
        guild_id = interaction.guild.id
        with key_locks[guild_id]:
            # Check if user already has the role
            role = interaction.guild.get_role(guild_config.role_id)
            if not role:
                raise ValueError("Role not found")
                
            # Verify bot can manage the role
            if role >= interaction.guild.me.top_role:
                raise PermissionError("Bot role too low")
                
            if role in interaction.user.roles:
                raise ValueError("Already claimed")
            
            # Verify key with hashes
            valid_hash = None
            for hash_ in guild_config.valid_keys:
                if key_security.verify_key(key, hash_):
                    valid_hash = hash_
                    break
                    
            if not valid_hash:
                await audit.log_claim(interaction, key[:3], success=False)
                raise ValueError("Invalid key")
                
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
                key=f"`{valid_hash}`"
            )
            
            await interaction.response.send_message(
                f"✨ {formatted} ✨",
                ephemeral=True
            )
            
    except Exception as e:
        await handle_claim_error(interaction, e)

async def handle_claim_error(interaction: discord.Interaction, error: Exception):
    # Add detailed error logging
    logging.error(f"Claim Error: {str(error)}", exc_info=True)
    
    error_messages = {
        ValueError: {
            "Server not configured": "🔧 Server not configured! Use /setup first",
            "Invalid key": "🔑 Invalid or expired key!",
            "Role not found": "👻 Role missing - contact admin!",
            "Already claimed": "🎭 You already have this role!"
        },
        PermissionError: "📛 Bot needs higher role position!",
        commands.CommandOnCooldown: lambda e: f"⏳ Try again in {e.retry_after:.1f}s"
    }
    
    # Get original error message
    error_msg = str(error).split('\n')[0]
    
    if isinstance(error, ValueError):
        message = error_messages[ValueError].get(error_msg, "❌ Validation error")
    elif isinstance(error, PermissionError):
        message = error_messages[PermissionError]
    elif isinstance(error, commands.CommandOnCooldown):
        message = error_messages[commands.CommandOnCooldown](error)
    else:
        message = "❌ An unexpected error occurred"
        
    try:
        await interaction.response.send_message(message, ephemeral=True)
    except discord.InteractionResponded:
        await interaction.followup.send(message, ephemeral=True)

# Add after config class
class KeySecurity:
    @staticmethod
    def hash_key(key: str) -> str:
        return bcrypt_sha256.hash(key)
    
    @staticmethod
    def verify_key(key: str, hash_: str) -> bool:
        try:
            return bcrypt_sha256.verify(key, hash_)
        except Exception:
            return False

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

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    bot.run(TOKEN)