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

    def add_key(self, key: str) -> bool:
        """Add a key to storage"""
        try:
            uuid_obj = uuid.UUID(key)
            key_normalized = str(uuid_obj).lower()
            self.key_store.add(key_normalized)
            self.key_filter.add(key_normalized)
            return True
        except ValueError:
            return False

    def remove_key(self, key: str) -> bool:
        """Remove a key from storage"""
        try:
            uuid_obj = uuid.UUID(key)
            key_normalized = str(uuid_obj).lower()
            if key_normalized in self.key_store:
                self.key_store.remove(key_normalized)
                if hasattr(self.key_filter, 'remove'):
                    self.key_filter.remove(key_normalized)
                return True
            return False
        except ValueError:
            return False

    def verify_key(self, key: str) -> bool:
        """Verify if a key is valid"""
        try:
            uuid_obj = uuid.UUID(key)
            key_normalized = str(uuid_obj).lower()
            return key_normalized in self.key_store
        except ValueError:
            return False

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

    async def process_claim(self, interaction: discord.Interaction, key: str):
        """Process a key claim attempt"""
        try:
            guild_id = interaction.guild.id
            cfg = self.config.get(guild_id)
            
            if not cfg:
                await interaction.response.send_message(
                    "🌌 The mystical gateway has not yet been established in this realm!",
                    ephemeral=True
                )
                return

            # Get role first to do early checks
            role = interaction.guild.get_role(cfg.role_id)
            if not role:
                await interaction.response.send_message(
                    "⚠️ The destined role has vanished from this realm! Seek the council of an elder.",
                    ephemeral=True
                )
                return

            # Check cooldown for non-admins
            if not interaction.user.guild_permissions.administrator:
                last_try = cfg.cooldowns.get(interaction.user.id, 0)
                if time.time() - last_try < cfg.custom_cooldown:
                    remaining = int(cfg.custom_cooldown - (time.time() - last_try))
                    minutes = remaining // 60
                    seconds = remaining % 60
                    await interaction.response.send_message(
                        f"⌛ The arcane energies must replenish... Return in {minutes}m {seconds}s.",
                        ephemeral=True
                    )
                    return

            # Verify and claim key
            async with self.locks[guild_id]:
                if cfg.verify_key(key):
                    # Remove key and grant role
                    cfg.remove_key(key)
                    await self.save_config()
                    
                    try:
                        await interaction.user.add_roles(role)
                        cfg.stats['successful_claims'] += 1
                        
                        success_msg = random.choice(cfg.success_msgs)
                        await interaction.response.send_message(
                            success_msg.format(
                                user=interaction.user.mention,
                                role=role.mention
                            ),
                            ephemeral=True
                        )
                    except discord.Forbidden:
                        await interaction.response.send_message(
                            "🔒 The mystical barriers prevent me from bestowing this power!",
                            ephemeral=True
                        )
                    except Exception as e:
                        logging.error(f"Role grant error: {str(e)}")
                        await interaction.response.send_message(
                            "💔 The ritual of bestowal has failed!",
                            ephemeral=True
                        )
                else:
                    cfg.stats['failed_claims'] += 1
                    cfg.cooldowns[interaction.user.id] = time.time()
                    await interaction.response.send_message(
                        "🌑 This key holds no power in these lands...",
                        ephemeral=True
                    )
                    
        except Exception as e:
            logging.error(f"Claim error: {str(e)}")
            try:
                await interaction.response.send_message(
                    "❌ Failed to process your key!",
                    ephemeral=True
                )
            except:
                pass

bot = RealmKeeper()

# Original Setup Command
@bot.tree.command(name="setup", description="Initialize a new realm")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    role="Role to grant",
    command_name="Claim command name (default: claim)",
    file="Optional: Text file containing initial keys (one per line)"
)
async def setup(
    interaction: discord.Interaction,
    role: discord.Role,
    command_name: str = "claim",
    file: discord.Attachment = None
):
    """Setup functionality with initial key support"""
    guild_id = interaction.guild.id
    
    # Validate permissions
    if not interaction.guild.me.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "🔒 I need the 'Manage Roles' permission!",
            ephemeral=True
        )
        return

    # Validate command name
    if command_name in bot.registered_commands:
        await interaction.response.send_message(
            "⚠️ This command name is already in use!",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    # Initialize realm configuration
    bot.config[guild_id] = GuildConfig(role.id)
    cfg = bot.config[guild_id]
    cfg.command = command_name.lower()
    
    # Process initial keys if provided
    if file:
        if not file.filename.endswith('.txt'):
            await interaction.followup.send(
                "📄 Only text files are accepted!",
                ephemeral=True
            )
            return

        if file.size > 2 * 1024 * 1024:  # 2MB limit
            await interaction.followup.send(
                "⚖️ File too large (max 2MB)!",
                ephemeral=True
            )
            return

        try:
            content = await file.read()
            keys = [k.strip() for k in content.decode('utf-8').split('\n') if k.strip()]
            
            added = 0
            invalid = 0
            
            for key in keys:
                if cfg.add_key(key):
                    added += 1
                else:
                    invalid += 1

            cfg.stats['keys_added'] = added
            
        except Exception as e:
            logging.error(f"Failed to process initial keys: {str(e)}")
            await interaction.followup.send(
                "⚠️ Failed to process some initial keys!",
                ephemeral=True
            )
    
    # Create dynamic command
    bot._create_dynamic_command(guild_id, command_name)
    await bot.tree.sync(guild=discord.Object(id=guild_id))
    await bot.save_config()
    
    response = [
        f"✨ Realm initialized for {role.mention}!",
        f"Use `/{command_name}` with valid keys to claim your role."
    ]
    
    if file:
        response.append(f"\n📦 Loaded {added} initial keys ({invalid} invalid)")
    
    await interaction.followup.send("\n".join(response), ephemeral=True)

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
    """File-based key loading with improved validation"""
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

    async with bot.locks[guild_id]:
        if overwrite:
            cfg.key_store.clear()
            cfg.key_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        
        added = 0
        invalid = 0
        
        for key in keys:
            if cfg.add_key(key):
                added += 1
            else:
                invalid += 1
        
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