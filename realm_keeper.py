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
    "🌟 The ancient scrolls have recognized {user} as a true {role}!",
    "⚔️ Through trials of valor, {user} ascends to the ranks of {role}!",
    "✨ The mystical gates of {role} part before {user}'s destined arrival!",
    "🔮 The oracles have foreseen it - {user} joins the sacred order of {role}!",
    "🏰 The grand halls of {role} echo with cheers as {user} takes their rightful place!",
    "⚡ The powers of {role} surge through {user}'s very being!",
    "🌈 A new light shines as {user} joins the {role} fellowship!",
    "🎭 The {role} welcomes their newest member, {user}!",
    "💫 {user} has proven worthy of the {role}'s ancient power!",
    "🔥 The flames of destiny mark {user} as a true {role}!"
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
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            cfg = self.config.get(guild_id)
            
            if not cfg:
                await interaction.followup.send(
                    "🌌 The mystical gateway has not yet been established in this realm!",
                    ephemeral=True
                )
                return

            # Get role first to do early checks
            role = interaction.guild.get_role(cfg.role_id)
            if not role:
                await interaction.followup.send(
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
                    await interaction.followup.send(
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
                        await interaction.followup.send(
                            success_msg.format(
                                user=interaction.user.mention,
                                role=role.mention
                            ),
                            ephemeral=True
                        )
                    except discord.Forbidden:
                        await interaction.followup.send(
                            "🔒 The mystical barriers prevent me from bestowing this power!",
                            ephemeral=True
                        )
                    except Exception as e:
                        logging.error(f"Role grant error: {str(e)}")
                        await interaction.followup.send(
                            "💔 The ritual of bestowal has failed!",
                            ephemeral=True
                        )
                else:
                    cfg.stats['failed_claims'] += 1
                    cfg.cooldowns[interaction.user.id] = time.time()
                    await interaction.followup.send(
                        "🌑 This key holds no power in these lands...",
                        ephemeral=True
                    )
                    
        except Exception as e:
            logging.error(f"Claim error: {str(e)}")
            try:
                await interaction.followup.send(
                    "❌ Failed to process your key!",
                    ephemeral=True
                )
            except:
                pass

bot = RealmKeeper()

# Setup Command
@bot.tree.command(name="setup", description="🏰 Initialize a new realm")
@app_commands.default_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    """Initialize a new realm"""
    # Validate permissions
    if not interaction.guild.me.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "🔒 I need the 'Manage Roles' permission!",
            ephemeral=True
        )
        return

    await interaction.response.send_modal(SetupModal())

# Add Keys Command
@bot.tree.command(name="addkeys", description="📚 Add multiple keys")
@app_commands.default_permissions(administrator=True)
async def addkeys(interaction: discord.Interaction):
    """Add multiple keys"""
    if interaction.guild_id not in bot.config:
        await interaction.response.send_message(
            "❌ Run /setup first!",
            ephemeral=True
        )
        return
        
    await interaction.response.send_modal(BulkKeyModal())

# Remove Keys Command
@bot.tree.command(name="removekeys", description="🗑️ Remove multiple keys")
@app_commands.default_permissions(administrator=True)
async def removekeys(interaction: discord.Interaction):
    """Remove multiple keys"""
    if interaction.guild_id not in bot.config:
        await interaction.response.send_message(
            "❌ Run /setup first!",
            ephemeral=True
        )
        return
        
    await interaction.response.send_modal(RemoveKeysModal())

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

class SetupModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="🏰 Realm Setup")
        self.add_item(discord.ui.TextInput(
            label="✨ Role Name",
            placeholder="Enter the exact role name to grant",
            min_length=1,
            max_length=100,
            required=True
        ))
        self.add_item(discord.ui.TextInput(
            label="🔮 Command Name",
            placeholder="Enter command name (e.g. claim, verify, redeem)",
            default="claim",
            min_length=1,
            max_length=32,
            required=True
        ))
        self.add_item(discord.ui.TextInput(
            label="📜 Initial Keys (Optional)",
            style=discord.TextStyle.paragraph,
            placeholder="Add initial keys here (one per line)",
            required=False,
            max_length=4000
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            
            # Get role by name
            role_name = self.children[0].value.strip()
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            
            if not role:
                await interaction.followup.send(
                    "❌ Role not found! Please enter the exact role name.",
                    ephemeral=True
                )
                return
            
            # Validate command name
            command_name = self.children[1].value.strip().lower()
            if command_name in bot.registered_commands:
                await interaction.followup.send(
                    "⚠️ This command name is already in use!",
                    ephemeral=True
                )
                return
            
            # Initialize realm configuration
            guild_id = interaction.guild.id
            bot.config[guild_id] = GuildConfig(role.id)
            cfg = bot.config[guild_id]
            cfg.command = command_name
            
            # Process initial keys if provided
            initial_keys = self.children[2].value.strip()
            if initial_keys:
                keys = [k.strip() for k in initial_keys.split('\n') if k.strip()]
                added = 0
                invalid = 0
                
                for key in keys:
                    if cfg.add_key(key):
                        added += 1
                    else:
                        invalid += 1
                
                cfg.stats['keys_added'] = added
            
            # Create dynamic command
            bot._create_dynamic_command(guild_id, command_name)
            await bot.tree.sync(guild=discord.Object(id=guild_id))
            await bot.save_config()
            
            # Build response
            response = [
                f"✨ Realm initialized for {role.mention}!",
                f"Use `/{command_name}` with valid keys to claim your role."
            ]
            
            if initial_keys:
                response.append(f"\n📦 Loaded {added} initial keys ({invalid} invalid)")
            
            await interaction.followup.send("\n".join(response), ephemeral=True)
            
        except Exception as e:
            logging.error(f"Setup error: {str(e)}")
            await interaction.followup.send(
                "💔 Failed to setup the realm!",
                ephemeral=True
            )

class BulkKeyModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="📚 Bulk Key Addition")
        self.add_item(discord.ui.TextInput(
            label="🔑 Keys",
            style=discord.TextStyle.paragraph,
            placeholder="Paste your keys here (one per line)",
            required=True,
            max_length=4000
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            cfg = bot.config.get(guild_id)
            
            if not cfg:
                await interaction.followup.send(
                    "❌ Run /setup first!",
                    ephemeral=True
                )
                return
            
            keys = [k.strip() for k in self.children[0].value.split('\n') if k.strip()]
            
            async with bot.locks[guild_id]:
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
                f"📦 Added {added} keys ({invalid} invalid)",
                ephemeral=True
            )
            
        except Exception as e:
            logging.error(f"Bulk key error: {str(e)}")
            await interaction.followup.send(
                "💔 Failed to process keys!",
                ephemeral=True
            )

class RemoveKeysModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="🗑️ Remove Keys")
        self.add_item(discord.ui.TextInput(
            label="🔑 Keys to Remove",
            style=discord.TextStyle.paragraph,
            placeholder="Enter keys to remove (one per line)",
            required=True,
            max_length=4000
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            cfg = bot.config.get(guild_id)
            
            if not cfg:
                await interaction.followup.send(
                    "❌ Run /setup first!",
                    ephemeral=True
                )
                return
            
            keys = [k.strip() for k in self.children[0].value.split('\n') if k.strip()]
            
            async with bot.locks[guild_id]:
                removed = 0
                not_found = 0
                
                for key in keys:
                    if cfg.remove_key(key):
                        removed += 1
                    else:
                        not_found += 1
                
                await bot.save_config()
            
            await interaction.followup.send(
                f"🗑️ Removed {removed} keys ({not_found} not found)",
                ephemeral=True
            )
            
        except Exception as e:
            logging.error(f"Remove keys error: {str(e)}")
            await interaction.followup.send(
                "💔 Failed to remove keys!",
                ephemeral=True
            )

class ArcaneGatewayModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="🔮 Mystical Gateway")
        self.add_item(discord.ui.TextInput(
            label="✨ Present Your Arcane Key",
            placeholder="Inscribe your mystical key (format: xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx)",
            min_length=36,
            max_length=36,
            required=True
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await bot.process_claim(interaction, self.children[0].value.strip())

class CustomizeModal(discord.ui.Modal):
    def __init__(self, current_messages):
        super().__init__(title="📜 Customize Success Messages")
        self.add_item(discord.ui.TextInput(
            label="✨ Success Messages (one per line)",
            style=discord.TextStyle.paragraph,
            placeholder="Use {user} for the user and {role} for the role\nExample: ✨ {user} has unlocked the {role} role!",
            default="\n".join(current_messages),
            required=True,
            max_length=4000
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            cfg = bot.config.get(guild_id)
            
            if not cfg:
                await interaction.followup.send(
                    "❌ Run /setup first!",
                    ephemeral=True
                )
                return
            
            # Parse and validate messages
            messages = [msg.strip() for msg in self.children[0].value.split('\n') if msg.strip()]
            
            if not messages:
                await interaction.followup.send(
                    "⚠️ Please provide at least one message!",
                    ephemeral=True
                )
                return
            
            # Validate message format
            invalid = []
            for msg in messages:
                if "{user}" not in msg or "{role}" not in msg:
                    invalid.append(msg)
            
            if invalid:
                await interaction.followup.send(
                    "⚠️ Some messages are missing {user} or {role} placeholders:\n" +
                    "\n".join(f"• {msg}" for msg in invalid[:3]) +
                    ("\n..." if len(invalid) > 3 else ""),
                    ephemeral=True
                )
                return
            
            # Update messages
            cfg.success_msgs = messages
            await bot.save_config()
            
            await interaction.followup.send(
                f"✨ Updated success messages! Added {len(messages)} messages.",
                ephemeral=True
            )
            
        except Exception as e:
            logging.error(f"Customize error: {str(e)}")
            await interaction.followup.send(
                "💔 Failed to update messages!",
                ephemeral=True
            )

# Add customize command
@bot.tree.command(name="customize", description="📜 Customize success messages")
@app_commands.default_permissions(administrator=True)
async def customize(interaction: discord.Interaction):
    """Customize success messages"""
    guild_id = interaction.guild.id
    cfg = bot.config.get(guild_id)
    
    if not cfg:
        await interaction.response.send_message(
            "❌ Run /setup first!",
            ephemeral=True
        )
        return
        
    await interaction.response.send_modal(CustomizeModal(cfg.success_msgs))

# Update claim command to use modal
@bot.tree.command(name="claim", description="✨ Claim your role with a mystical key")
async def claim(interaction: discord.Interaction):
    """Claim your role with a key"""
    if interaction.guild_id not in bot.config:
        await interaction.response.send_message(
            "🌌 The mystical gateway has not yet been established in this realm!",
            ephemeral=True
        )
        return
        
    await interaction.response.send_modal(ArcaneGatewayModal())

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    bot.run(TOKEN)