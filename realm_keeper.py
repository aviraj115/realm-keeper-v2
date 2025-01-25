import os
import uuid
import json
import hashlib
import logging
import asyncio
import discord
import random
import time
from discord.ext import commands
from discord import app_commands
from pybloom_live import ScalableBloomFilter
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.FileHandler('realm.log'), logging.StreamHandler()]
)

DRAMATIC_MESSAGES = [
    "🌟 {user} has proven worthy of {role}!",
    "⚔️ The sacred {role} mantle falls upon {user}!",
    # ... (keep all original messages)
]

class GuildConfig:
    __slots__ = ('role_id', 'command', 'key_filter', 'key_store', 
                'cooldowns', 'success_msgs', 'stats', 'custom_cooldown')
    
    def __init__(self, role_id: int):
        self.role_id = role_id
        self.command = "claim"
        self.key_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        self.key_store = set()
        self.cooldowns = dict()
        self.success_msgs = DRAMATIC_MESSAGES.copy()
        self.stats = defaultdict(int)
        self.custom_cooldown = 300  # Default 5 minutes

class RealmKeeper(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix='!', intents=intents)
        self.config = dict()
        self.locks = defaultdict(asyncio.Lock)
        self.registered_commands = set()

    async def setup_hook(self):
        await self.load_config()
        await self.register_commands()
        await self.tree.sync()
        logging.info("Realm Keeper initialized")

    async def load_config(self):
        try:
            with open('realms.json') as f:
                realms = json.load(f)
                for gid, data in realms.items():
                    guild_id = int(gid)
                    self.config[guild_id] = GuildConfig(data['role_id'])
                    cfg = self.config[guild_id]
                    cfg.command = data.get('command', 'claim')
                    cfg.key_store = set(data['keys'])
                    cfg.success_msgs = data.get('success_msgs', DRAMATIC_MESSAGES.copy())
                    cfg.custom_cooldown = data.get('custom_cooldown', 300)
                    for key in data['keys']:
                        cfg.key_filter.add(key)
                    self._create_dynamic_command(guild_id, cfg.command)
        except FileNotFoundError:
            logging.warning("No existing configuration found")

    def _create_dynamic_command(self, guild_id: int, command_name: str):
        """Create dynamic claim command for a guild"""
        if (guild_id, command_name) in self.registered_commands:
            return

        @app_commands.command(name=command_name, description="Claim your role with a mystical key")
        @app_commands.guild_only()
        async def claim_command(interaction: discord.Interaction, key: str):
            await self.process_claim(interaction, key)
        
        self.tree.add_command(claim_command, guild=discord.Object(id=guild_id))
        self.registered_commands.add((guild_id, command_name))

    async def save_config(self):
        data = {
            str(gid): {
                'role_id': cfg.role_id,
                'command': cfg.command,
                'keys': list(cfg.key_store),
                'success_msgs': cfg.success_msgs,
                'custom_cooldown': cfg.custom_cooldown
            }
            for gid, cfg in self.config.items()
        }
        with open('realms.json', 'w') as f:
            json.dump(data, f)

bot = RealmKeeper()

# Original Setup Command
@bot.tree.command(name="setup", description="Initialize a new realm")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    role="Role to grant",
    command_name="Claim command name (default: claim)"
)
async def setup(
    interaction: discord.Interaction,
    role: discord.Role,
    command_name: str = "claim"
):
    """Original setup functionality with performance enhancements"""
    guild_id = interaction.guild.id
    
    # Validate permissions
    if not interaction.guild.me.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "🔒 I need the 'Manage Roles' permission!",
            ephemeral=True
        )
        return

    # Validate command name
    if command_name in app_commands.tree._all_commands:
        await interaction.response.send_message(
            "⚠️ This command name is already in use!",
            ephemeral=True
        )
        return

    # Initialize realm configuration
    bot.config[guild_id] = GuildConfig(role.id)
    bot.config[guild_id].command = command_name.lower()
    
    # Create dynamic command
    bot._create_dynamic_command(guild_id, command_name)
    await bot.tree.sync(guild=discord.Object(id=guild_id))
    
    await interaction.response.send_message(
        f"✨ Realm initialized for {role.mention}!\n"
        f"Use `/{command_name}` with valid keys to claim your role.",
        ephemeral=True
    )

# Original Loadkeys Command
@bot.tree.command(name="loadkeys", description="Load keys from a text file")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    file="Text file containing keys (one per line)",
    overwrite="Clear existing keys? (default: False)"
)
async def loadkeys(
    interaction: discord.Interaction,
    file: discord.Attachment,
    overwrite: bool = False
):
    """File-based key loading with original functionality"""
    guild_id = interaction.guild.id
    cfg = bot.config.get(guild_id)
    
    if not cfg:
        await interaction.response.send_message(
            "❌ Run /setup first!",
            ephemeral=True
        )
        return

    # Validate file
    if not file.filename.endswith('.txt'):
        await interaction.response.send_message(
            "📄 Only text files are accepted!",
            ephemeral=True
        )
        return

    if file.size > 2 * 1024 * 1024:  # 2MB limit
        await interaction.response.send_message(
            "⚖️ File too large (max 2MB)!",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    
    try:
        content = await file.read()
        keys = [k.strip() for k in content.decode('utf-8').split('\n') if k.strip()]
    except Exception as e:
        await interaction.followup.send(
            "💥 Failed to read file!",
            ephemeral=True
        )
        return

    added = 0
    invalid = 0
    hashes = set()
    
    for key in keys:
        try:
            uuid.UUID(key)
            key_hash = hashlib.sha256(key.lower().encode()).hexdigest()
            hashes.add(key_hash)
            added += 1
        except ValueError:
            invalid += 1

    async with bot.locks[guild_id]:
        if overwrite:
            cfg.key_store.clear()
            cfg.key_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
            
        cfg.key_store.update(hashes)
        for h in hashes:
            cfg.key_filter.add(h)
        
        cfg.stats['keys_added'] += added

    await bot.save_config()
    await interaction.followup.send(
        f"📦 Loaded {added} keys ({invalid} invalid)",
        ephemeral=True
    )

# Keep other original commands (addkeys, removekeys, clearkeys, etc.) 
# with their original implementations from previous versions

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    bot.run(TOKEN)