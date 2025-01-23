import discord
import json
from discord.ext import commands
from discord import app_commands
import os
from dotenv import load_dotenv
import uuid
import fcntl
import shutil

# At the top of the file
load_dotenv()

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Load config
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    config = {"guilds": {}}

def save_config():
    # Create backup
    shutil.copy2('config.json', 'config.json.bak')
    
    with open('config.json', 'w') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            json.dump(config, f, indent=4)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

@bot.event
async def on_ready():
    print(f"{bot.user} has connected to Discord!")
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced {len(synced)} commands: {[cmd.name for cmd in synced]}")
    except Exception as e:
        print(f"❌ Sync failed: {e}")

@bot.event
async def on_guild_join(guild):
    try:
        await bot.tree.sync(guild=discord.Object(id=guild.id))
        print(f"✅ Commands synced to new guild: {guild.name}")
    except Exception as e:
        print(f"❌ Failed to sync to {guild.name}: {e}")

# Modals
class SetupModal(discord.ui.Modal, title="Server Setup"):
    role_name = discord.ui.TextInput(
        label="Role Name (exact match)",
        placeholder="Realm Tester",
        required=True,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild.id)
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

        config["guilds"][guild_id] = {
            "role_id": roles[0].id,
            "valid_keys": []
        }
        save_config()
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
        guild_id = str(interaction.guild.id)
        if guild_id not in config["guilds"]:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        existing = set(config["guilds"][guild_id]["valid_keys"])
        new_keys = [k for k in key_list if k not in existing]
        
        config["guilds"][guild_id]["valid_keys"].extend(new_keys)
        save_config()

        await interaction.response.send_message(
            f"✅ Added {len(new_keys)} new keys!\n"
            f"• Duplicates skipped: {len(key_list)-len(new_keys)}\n"
            f"• Total keys: {len(config['guilds'][guild_id]['valid_keys'])}",
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
        guild_id = str(interaction.guild.id)
        if guild_id not in config["guilds"]:
            await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
            return

        key_list = [k.strip() for k in self.keys.value.split("\n") if k.strip()]
        guild_config = config["guilds"][guild_id]
        
        removed = [k for k in key_list if k in guild_config["valid_keys"]]
        guild_config["valid_keys"] = [k for k in guild_config["valid_keys"] if k not in key_list]
        save_config()

        await interaction.response.send_message(
            f"✅ Removed {len(removed)} keys!\n"
            f"• Not found: {len(key_list)-len(removed)}\n"
            f"• Remaining: {len(guild_config['valid_keys'])}",
            ephemeral=True
        )

# Commands
@bot.tree.command(name="setup", description="Initial server setup")
@app_commands.default_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    await interaction.response.send_modal(SetupModal())

@bot.tree.command(name="addkey", description="Add single key")
@app_commands.default_permissions(administrator=True)
async def addkey(interaction: discord.Interaction, key: str):
    guild_id = str(interaction.guild.id)
    if guild_id not in config["guilds"]:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    if key in config["guilds"][guild_id]["valid_keys"]:
        await interaction.response.send_message("❌ Key exists!", ephemeral=True)
        return

    config["guilds"][guild_id]["valid_keys"].append(key)
    save_config()
    await interaction.response.send_message(f"✅ Key added!", ephemeral=True)

@bot.tree.command(name="addkeys", description="Bulk add keys")
@app_commands.default_permissions(administrator=True)
async def addkeys(interaction: discord.Interaction):
    await interaction.response.send_modal(BulkKeysModal())

@bot.tree.command(name="removekey", description="Remove single key")
@app_commands.default_permissions(administrator=True)
async def removekey(interaction: discord.Interaction, key: str):
    guild_id = str(interaction.guild.id)
    if guild_id not in config["guilds"]:
        await interaction.response.send_message("❌ Run /setup first!", ephemeral=True)
        return

    if key not in config["guilds"][guild_id]["valid_keys"]:
        await interaction.response.send_message("❌ Key not found!", ephemeral=True)
        return

    config["guilds"][guild_id]["valid_keys"].remove(key)
    save_config()
    await interaction.response.send_message("✅ Key removed!", ephemeral=True)

@bot.tree.command(name="removekeys", description="Bulk remove keys")
@app_commands.default_permissions(administrator=True)
async def removekeys(interaction: discord.Interaction):
    await interaction.response.send_modal(RemoveKeysModal())

@bot.tree.command(name="clearkeys", description="Clear ALL valid keys (admin only)")
@app_commands.default_permissions(administrator=True)
async def clearkeys(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    if guild_id not in config["guilds"]:
        await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
        return

    # Confirmation dialog with proper interaction handling
    class ClearConfirmView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)

        @discord.ui.button(label="CONFIRM CLEAR ALL KEYS", style=discord.ButtonStyle.danger)
        async def confirm(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            if button_interaction.user != interaction.user:
                return  # Only allow original user
                
            config["guilds"][guild_id]["valid_keys"] = []
            save_config()
            await button_interaction.response.edit_message(
                content="✅ **All keys have been cleared!**",
                view=None
            )

    # Send the confirmation message
    await interaction.response.send_message(
        "⚠️ **This will delete ALL keys!**\nClick to confirm:",
        view=ClearConfirmView(),
        ephemeral=True
    )
    
def is_valid_uuid(key):
    try:
        uuid.UUID(str(key))
        return True
    except ValueError:
        return False

@commands.cooldown(1, 60, commands.BucketType.user)  # 1 attempt per minute
@bot.tree.command(name="claim", description="Claim your role")
async def claim(interaction: discord.Interaction, key: str):
    if not is_valid_uuid(key):
        await interaction.response.send_message("❌ Invalid key format!", ephemeral=True)
        return

    try:
        guild_id = str(interaction.guild.id)
        if (guild_config := config["guilds"].get(guild_id)) is None:
            await interaction.response.send_message("❌ Server not setup!", ephemeral=True)
            return

        if key not in guild_config["valid_keys"]:
            await interaction.response.send_message("❌ Invalid key!", ephemeral=True)
            return

        role = interaction.guild.get_role(guild_config["role_id"])
        if not role:
            await interaction.response.send_message("❌ Role missing!", ephemeral=True)
            return

        # Check if bot can assign this role
        if role >= interaction.guild.me.top_role:
            await interaction.response.send_message(
                "❌ Bot needs higher role position!",
                ephemeral=True
            )
            return

        await interaction.user.add_roles(role)
        guild_config["valid_keys"].remove(key)
        save_config()
        await interaction.response.send_message(
            f"🎉 {interaction.user.mention}, welcome to the team!",
            ephemeral=True
        )

    except discord.Forbidden:
        await interaction.response.send_message(
            "❌ Missing permissions! Check bot role position.",
            ephemeral=True
        )

async def cleanup_unused_guilds():
    for guild_id in list(config["guilds"].keys()):
        if not bot.get_guild(int(guild_id)):
            del config["guilds"][guild_id]
    save_config()

@bot.tree.command(name="keys", description="Check number of available keys (Admin only)")
@app_commands.default_permissions(administrator=True)
async def keys(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    if (guild_config := config["guilds"].get(guild_id)) is None:
        await interaction.response.send_message("❌ Server not setup!", ephemeral=True)
        return
    
    num_keys = len(guild_config["valid_keys"])
    role = interaction.guild.get_role(guild_config["role_id"])
    
    await interaction.response.send_message(
        f"🔑 **Key Status**\n"
        f"• Available keys: {num_keys}\n"
        f"• Target role: {role.mention if role else '❌ Role not found!'}\n"
        f"• Server ID: {guild_id}",
        ephemeral=True
    )

if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("No Discord token found! Make sure to create a .env file with DISCORD_TOKEN=your_token_here")
    bot.run(TOKEN)