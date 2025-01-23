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
from typing import Dict, Set

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
    __slots__ = ('role_id', 'valid_keys')
    
    def __init__(self, role_id: int, valid_keys: Set[str]):
        self.role_id = role_id
        self.valid_keys = valid_keys

config: Dict[int, GuildConfig] = {}

async def save_config():
    async with aiofiles.open('config.json', 'w') as f:
        serialized = {
            str(guild_id): {
                "role_id": cfg.role_id,
                "valid_keys": list(cfg.valid_keys)
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
                    set(cfg["valid_keys"])
                )
                for guild_id, cfg in data.items()
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
class SetupModal(discord.ui.Modal, title="Server Setup"):
    role_name = discord.ui.TextInput(
        label="Role Name (exact match)",
        placeholder="Realm Tester",
        required=True,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = interaction.guild.id
        roles = [r for r in interaction.guild.roles if r.name == str(self.role_name)]
        
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

        config[guild_id] = GuildConfig(roles[0].id, set())
        await save_config()
        
        await interaction.response.send_message(
            f"✅ Setup complete! Role set to {roles[0].mention}.",
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
        if guild_id not in config:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        existing = config[guild_id].valid_keys
        new_keys = [k for k in key_list if k not in existing]
        
        config[guild_id].valid_keys.update(new_keys)
        await save_config()

        await interaction.response.send_message(
            f"✅ Added {len(new_keys)} new keys!\n"
            f"• Duplicates skipped: {len(key_list)-len(new_keys)}\n"
            f"• Total keys: {len(config[guild_id].valid_keys)}",
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
        if guild_id not in config:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        guild_config = config[guild_id]
        
        removed = sum(1 for k in key_list if k in guild_config.valid_keys)
        guild_config.valid_keys -= set(key_list)
        await save_config()

        await interaction.response.send_message(
            f"✅ Removed {removed} keys!\n"
            f"• Not found: {len(key_list)-removed}\n"
            f"• Remaining: {len(guild_config.valid_keys)}",
            ephemeral=True
        )

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
    if guild_id not in config:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    if key in config[guild_id].valid_keys:
        await interaction.response.send_message("❌ Key exists!", ephemeral=True)
        return

    config[guild_id].valid_keys.add(key)
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
    if guild_id not in config:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    if key not in config[guild_id].valid_keys:
        await interaction.response.send_message("❌ Key not found!", ephemeral=True)
        return

    config[guild_id].valid_keys.discard(key)
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
    if guild_id not in config:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    class ClearConfirmView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)

        @discord.ui.button(label="CONFIRM CLEAR ALL KEYS", style=discord.ButtonStyle.danger)
        async def confirm(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            if button_interaction.user != interaction.user:
                return
                
            config[guild_id].valid_keys.clear()
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
async def keys(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in config:
        await interaction.response.send_message("❌ Server not setup!", ephemeral=True)
        return
    
    guild_config = config[guild_id]
    role = interaction.guild.get_role(guild_config.role_id)
    
    await interaction.response.send_message(
        f"🔑 **Key Status**\n"
        f"• Available keys: {len(guild_config.valid_keys)}\n"
        f"• Target role: {role.mention if role else '❌ Role not found!'}",
        ephemeral=True
    )

class ClaimSystem:
    _cooldown = commands.CooldownMapping.from_cooldown(1, 60, commands.BucketType.user)
    
    @staticmethod
    def is_valid_key(key: str) -> bool:
        try:
            return uuid.UUID(key).version == 4
        except ValueError:
            return False

@bot.tree.command(name="claim", description="Claim your role")
@app_commands.describe(key="Your activation key")
async def claim(interaction: discord.Interaction, key: str):
    try:
        # Validate key format
        if not ClaimSystem.is_valid_key(key):
            raise ValueError("Invalid key format")
            
        # Check cooldown
        bucket = ClaimSystem._cooldown.get_bucket(interaction)
        if retry_after := bucket.update_rate_limit():
            raise commands.CommandOnCooldown(bucket, retry_after)
            
        guild_id = interaction.guild.id
        if (guild_config := config.get(guild_id)) is None:
            raise ValueError("Server not configured")
            
        if key not in guild_config.valid_keys:
            raise ValueError("Invalid key")
            
        role = interaction.guild.get_role(guild_config.role_id)
        if not role:
            raise ValueError("Role not found")
            
        if role >= interaction.guild.me.top_role:
            raise PermissionError("Bot role too low")
            
        # Process claim
        await interaction.user.add_roles(role)
        guild_config.valid_keys.remove(key)
        await save_config()
        
        await interaction.response.send_message(
            f"🎉 {interaction.user.mention}, welcome!",
            ephemeral=True
        )
        
    except Exception as e:
        error_map = {
            "Invalid key format": "❌ Invalid key format!",
            "Server not configured": "❌ Server not setup!",
            "Invalid key": "❌ Invalid key!",
            "Role not found": "❌ Role missing!",
            "Bot role too low": "❌ Bot needs higher role position!",
            "CommandOnCooldown": lambda e: f"⏳ Try again in {e.retry_after:.1f} seconds"
        }
        
        message = error_map.get(type(e).__name__, "❌ An error occurred")
        if callable(message):
            message = message(e)
            
        await interaction.response.send_message(message, ephemeral=True)

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    bot.run(TOKEN)