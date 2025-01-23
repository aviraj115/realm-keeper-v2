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
    __slots__ = ('role_id', 'valid_keys', 'command', 'success_msg')
    
    def __init__(self, role_id: int, valid_keys: Set[str], command: str = "claim", 
                 success_msg: str = "{user} has unlocked the {role}!"):
        self.role_id = role_id
        self.valid_keys = valid_keys
        self.command = command
        self.success_msg = success_msg

config: Dict[int, GuildConfig] = {}

async def save_config():
    async with aiofiles.open('config.json', 'w') as f:
        serialized = {
            str(guild_id): {
                "role_id": cfg.role_id,
                "valid_keys": list(cfg.valid_keys),
                "command": cfg.command,
                "success_msg": cfg.success_msg
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
                    cfg.get("success_msg", "{user} has unlocked the {role}!")
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
        label="Success Message (empty for random)",
        placeholder="{user} has unlocked the {role}!",
        style=discord.TextStyle.long,
        required=False
    )
    
    initial_keys = discord.ui.TextInput(
        label="Initial Keys (one per line, optional)",
        style=discord.TextStyle.long,
        placeholder="key1\nkey2\nkey3",
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            guild_id = interaction.guild.id
            role_name = str(self.role_name)
            command = str(self.command_name).lower().strip()
            
            # Validate command name
            if not command.isalnum():
                await interaction.response.send_message(
                    "❌ Command name must be alphanumeric!",
                    ephemeral=True
                )
                return
            
            # Find role
            roles = [r for r in interaction.guild.roles if r.name == role_name]
            if len(roles) > 1:
                await interaction.response.send_message(
                    "❌ Multiple roles with this name exist!",
                    ephemeral=True
                )
                return
            
            if not roles:
                await interaction.response.send_message(
                    "❌ Role not found! Create it first.",
                    ephemeral=True
                )
                return

            # Use random default message if none provided
            success_template = str(self.success_message) if self.success_message.value else random.choice(DEFAULT_SUCCESS_MESSAGES)
            
            # Parse initial keys if provided
            initial_key_set = set()
            if self.initial_keys.value:
                initial_key_set = {k.strip() for k in self.initial_keys.value.split("\n") if k.strip()}
            
            # Store configuration
            config[guild_id] = GuildConfig(
                roles[0].id,
                initial_key_set,  # Use the initial keys
                command,
                success_template
            )
            await save_config()
            
            # Create guild-specific command
            await create_dynamic_command(command, guild_id)
            
            await interaction.response.send_message(
                f"🔮 Configuration complete!\n"
                f"- Activation command: `/{command}`\n"
                f"- Success template: `{success_template}`\n"
                f"- Initial keys added: {len(initial_key_set)}",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Setup failed: {str(e)}",
                ephemeral=True
            )

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

        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        existing = guild_config.valid_keys
        new_keys = [k for k in key_list if k not in existing]
        
        guild_config.valid_keys.update(new_keys)
        await save_config()

        await interaction.response.send_message(
            f"✅ Added {len(new_keys)} new keys!\n"
            f"• Duplicates skipped: {len(key_list)-len(new_keys)}\n"
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
        
        removed = sum(1 for k in key_list if k in guild_config.valid_keys)
        guild_config.valid_keys -= set(key_list)
        await save_config()

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
            return await func(interaction, guild_config)
        return wrapper
    return decorator

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

    if key in guild_config.valid_keys:
        await interaction.response.send_message("❌ Key exists!", ephemeral=True)
        return

    guild_config.valid_keys.add(key)
    await save_config()
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

    if key not in guild_config.valid_keys:
        await interaction.response.send_message("❌ Key not found!", ephemeral=True)
        return

    guild_config.valid_keys.discard(key)
    await save_config()
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
                
            guild_config.valid_keys.clear()
            await save_config()
            await button_interaction.response.edit_message(
                content="✅ All keys cleared!",
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

        # Remove existing command if it exists
        old_command = bot.tree.get_command(name, guild=discord.Object(id=guild_id))
        if old_command:
            bot.tree.remove_command(name, guild=discord.Object(id=guild_id))
            
        @bot.tree.command(name=name, description="🌟 Unlock your mystical powers", guild=discord.Object(id=guild_id))
        async def dynamic_claim(interaction: discord.Interaction):
            await interaction.response.send_modal(ArcaneGatewayModal())
            
        await bot.tree.sync(guild=discord.Object(id=guild_id))
        logging.info(f"Created command /{name} for guild {guild_id}")
        
    except Exception as e:
        logging.error(f"Failed to create command /{name}: {e}")
        raise

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    bot.run(TOKEN)