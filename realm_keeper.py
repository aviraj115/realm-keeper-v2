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
        self.stats = {
            'keys_added': 0,
            'keys_removed': 0,
            'successful_claims': 0,
            'failed_claims': 0,
            'last_claim_time': 0,
            'total_keys': 0
        }
        self.custom_cooldown = 300  # Default 5 minutes

    def add_key(self, key: str) -> bool:
        """Add a key to storage"""
        try:
            uuid_obj = uuid.UUID(key)
            key_normalized = str(uuid_obj).lower()
            if key_normalized not in self.key_store:
                self.key_store.add(key_normalized)
                self.key_filter.add(key_normalized)
                self.stats['keys_added'] += 1
                self.stats['total_keys'] = len(self.key_store)
                return True
            return False
        except ValueError:
            return False

    def remove_key(self, key: str) -> bool:
        """Remove a key from storage"""
        try:
            uuid_obj = uuid.UUID(key)
            key_normalized = str(uuid_obj).lower()
            if key_normalized in self.key_store:
                self.key_store.remove(key_normalized)
                self.stats['keys_removed'] += 1
                self.stats['total_keys'] = len(self.key_store)
                return True
            return False
        except ValueError:
            return False

    def verify_key(self, key: str) -> bool:
        """Verify if a key is valid"""
        try:
            uuid_obj = uuid.UUID(key)
            key_normalized = str(uuid_obj).lower()
            # First check Bloom filter for quick rejection
            if key_normalized not in self.key_filter:
                return False
            # Then check actual set
            return key_normalized in self.key_store
        except ValueError:
            return False

# --- Modals ---
# Placed before the Bot class because they are used by command functions,
# which are then referenced by the Bot class.
class ArcaneGatewayModal(discord.ui.Modal, title="🔮 Mystical Gateway"):
    key_input = discord.ui.TextInput(
        label="✨ Present Your Arcane Key",
        placeholder="Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        min_length=36,
        max_length=36
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.client.process_claim(interaction, self.key_input.value.strip())

class SetupModal(discord.ui.Modal, title="🏰 Realm Setup"):
    role_name_input = discord.ui.TextInput(
        label="✨ Role Name",
        placeholder="Enter the exact role name to grant",
        min_length=1,
        max_length=100
    )
    command_name_input = discord.ui.TextInput(
        label="🔮 Command Name",
        placeholder="e.g., claim, verify, redeem (no slash)",
        default="claim",
        min_length=1,
        max_length=32
    )
    initial_keys_input = discord.ui.TextInput(
        label="📜 Initial Keys (Optional)",
        style=discord.TextStyle.paragraph,
        placeholder="Add initial keys here, one per line",
        required=False,
        max_length=4000
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            bot = interaction.client # Use interaction.client to get the bot instance
            
            role_name = self.role_name_input.value.strip()
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            
            if not role:
                await interaction.followup.send(f"❌ Role '{role_name}' not found! Please enter the exact name (case-sensitive).", ephemeral=True)
                return
            
            command_name = self.command_name_input.value.strip().lower()
            reserved_names = [cmd.name for cmd in bot.tree.get_commands()]
            if command_name in reserved_names:
                await interaction.followup.send(f"⚠️ Command name `/{command_name}` is reserved! Choose another.", ephemeral=True)
                return
            
            guild_id = interaction.guild.id
            bot.config[guild_id] = GuildConfig(role.id)
            cfg = bot.config[guild_id]
            cfg.command = command_name
            
            added, invalid = 0, 0
            initial_keys = self.initial_keys_input.value.strip()
            if initial_keys:
                keys = [k.strip() for k in initial_keys.split('\n') if k.strip()]
                for key in keys:
                    if cfg.add_key(key):
                        added += 1
                    else:
                        invalid += 1
            
            success = await bot._create_dynamic_command(guild_id, command_name)
            if not success:
                await interaction.followup.send("⚠️ Failed to create the slash command. Please check my permissions and try again.", ephemeral=True)
                if guild_id in bot.config:
                    del bot.config[guild_id] # Clean up failed config
                return

            await bot.save_config()

            response = [
                f"✨ Realm initialized for {role.mention}!",
                f"Use `/{command_name}` to claim the role.",
                "⚠️ It may take a minute for Discord to show the new command."
            ]
            if initial_keys:
                response.append(f"\n📦 Loaded {added} initial keys ({invalid} were invalid or duplicates).")
            
            await interaction.followup.send("\n".join(response), ephemeral=True)
        except Exception as e:
            logging.error(f"Setup modal error: {str(e)}")
            await interaction.followup.send("💔 An unexpected error occurred during setup.", ephemeral=True)

class BulkKeyModal(discord.ui.Modal, title="📚 Bulk Key Addition"):
    keys_input = discord.ui.TextInput(
        label="🔑 Keys",
        style=discord.TextStyle.paragraph,
        placeholder="Paste your keys here, one per line",
        max_length=4000
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild.id
        bot = interaction.client
        cfg = bot.config.get(guild_id)
        
        if not cfg:
            await interaction.followup.send("❌ Run `/setup` first!", ephemeral=True)
            return
            
        keys = [k.strip() for k in self.keys_input.value.split('\n') if k.strip()]
        
        async with bot.locks[guild_id]:
            added, invalid = 0, 0
            for key in keys:
                if cfg.add_key(key):
                    added += 1
                else:
                    invalid += 1
            await bot.save_config()
        
        await interaction.followup.send(f"📦 Added {added} new keys. ({invalid} were invalid or duplicates).", ephemeral=True)

class RemoveKeysModal(discord.ui.Modal, title="🗑️ Remove Keys"):
    keys_input = discord.ui.TextInput(
        label="🔑 Keys to Remove",
        style=discord.TextStyle.paragraph,
        placeholder="Paste keys to remove, one per line",
        max_length=4000
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild.id
        bot = interaction.client
        cfg = bot.config.get(guild_id)
        
        if not cfg:
            await interaction.followup.send("❌ Run `/setup` first!", ephemeral=True)
            return
            
        keys = [k.strip() for k in self.keys_input.value.split('\n') if k.strip()]
        
        async with bot.locks[guild_id]:
            removed, not_found = 0, 0
            for key in keys:
                if cfg.remove_key(key):
                    removed += 1
                else:
                    not_found += 1
            await bot.save_config()
        
        await interaction.followup.send(f"🗑️ Removed {removed} keys. ({not_found} were not found).", ephemeral=True)

class CustomizeModal(discord.ui.Modal, title="📜 Customize Success Messages"):
    messages_input = discord.ui.TextInput(
        label="✨ Success Messages (one per line)",
        style=discord.TextStyle.paragraph,
        placeholder="Use {user} for user mention and {role} for role mention.",
        max_length=4000
    )
    def __init__(self, current_messages):
        super().__init__()
        self.messages_input.default = "\n".join(current_messages)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild.id
        bot = interaction.client
        cfg = bot.config.get(guild_id)
        
        if not cfg:
            await interaction.followup.send("❌ Run `/setup` first!", ephemeral=True)
            return

        messages = [msg.strip() for msg in self.messages_input.value.split('\n') if msg.strip()]
        if not messages:
            await interaction.followup.send("⚠️ Please provide at least one message!", ephemeral=True)
            return
        
        invalid_msgs = [msg for msg in messages if "{user}" not in msg or "{role}" not in msg]
        if invalid_msgs:
            await interaction.followup.send(
                "⚠️ Some messages are missing `{user}` or `{role}` placeholders:\n" +
                "\n".join(f"• `{msg}`" for msg in invalid_msgs[:3]),
                ephemeral=True
            )
            return
        
        cfg.success_msgs = messages
        await bot.save_config()
        
        await interaction.followup.send(f"✨ Success messages updated! There are now {len(messages)} unique messages.", ephemeral=True)

# --- Command Functions ---
# These are defined before the Bot class so they can be referenced during its initialization.
@app_commands.command(name="setup", description="🏰 Initialize or reconfigure the bot for this server.")
@app_commands.default_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    if not interaction.guild.me.guild_permissions.manage_roles:
        await interaction.response.send_message("🔒 I need the 'Manage Roles' permission to function!", ephemeral=True)
        return
    await interaction.response.send_modal(SetupModal())

@app_commands.command(name="addkeys", description="📚 Add multiple keys to the store.")
@app_commands.default_permissions(administrator=True)
async def addkeys(interaction: discord.Interaction):
    if interaction.guild_id not in interaction.client.config:
        await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
        return
    await interaction.response.send_modal(BulkKeyModal())

@app_commands.command(name="removekeys", description="🗑️ Remove multiple keys from the store.")
@app_commands.default_permissions(administrator=True)
async def removekeys(interaction: discord.Interaction):
    if interaction.guild_id not in interaction.client.config:
        await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
        return
    await interaction.response.send_modal(RemoveKeysModal())

@app_commands.command(name="loadkeys", description="📤 Load keys from a text file.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    file="The text file containing keys (one per line).",
    overwrite="Select True to remove all existing keys before adding new ones."
)
async def loadkeys(
    interaction: discord.Interaction,
    file: discord.Attachment,
    overwrite: bool = False
):
    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id
    bot = interaction.client
    cfg = bot.config.get(guild_id)
    
    if not cfg:
        await interaction.followup.send("❌ Run `/setup` first!", ephemeral=True)
        return

    if not file.filename.endswith('.txt'):
        await interaction.followup.send("📄 Invalid file type! Please upload a `.txt` file.", ephemeral=True)
        return

    if file.size > 2 * 1024 * 1024:  # 2MB limit
        await interaction.followup.send("⚖️ File is too large (max 2MB).", ephemeral=True)
        return
    
    try:
        content = await file.read()
        keys = [k.strip() for k in content.decode('utf-8').split('\n') if k.strip()]
    except Exception as e:
        logging.error(f"File read error: {e}")
        await interaction.followup.send("💥 Failed to read the file content.", ephemeral=True)
        return

    async with bot.locks[guild_id]:
        if overwrite:
            key_count = len(cfg.key_store)
            cfg.key_store.clear()
            cfg.key_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
            cfg.stats['keys_removed'] += key_count
            logging.info(f"Cleared {key_count} keys for overwrite in guild {guild_id}.")

        added, invalid = 0, 0
        for key in keys:
            if cfg.add_key(key):
                added += 1
            else:
                invalid += 1
        
        await bot.save_config()

    await interaction.followup.send(
        f"📦 Load complete. Added {added} new keys. "
        f"({invalid} were invalid or duplicates). "
        f"{'All previous keys were cleared.' if overwrite else ''}",
        ephemeral=True
    )

@app_commands.command(name="customize", description="📜 Customize the success messages for role claims.")
@app_commands.default_permissions(administrator=True)
async def customize(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    bot = interaction.client
    cfg = bot.config.get(guild_id)
    if not cfg:
        await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
        return
    await interaction.response.send_modal(CustomizeModal(cfg.success_msgs))

@app_commands.command(name="clearkeys", description="🗑️ Remove all available keys from the store.")
@app_commands.default_permissions(administrator=True)
async def clearkeys(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    bot = interaction.client
    cfg = bot.config.get(guild_id)
    if not cfg:
        await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    
    async with bot.locks[guild_id]:
        key_count = len(cfg.key_store)
        cfg.key_store.clear()
        cfg.key_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        cfg.stats['keys_removed'] += key_count
        cfg.stats['total_keys'] = 0
        await bot.save_config()
    
    await interaction.followup.send(f"🗑️ Cleared all {key_count} keys!", ephemeral=True)

@app_commands.command(name="stats", description="📊 View statistics for this realm.")
@app_commands.default_permissions(administrator=True)
async def stats(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    bot = interaction.client
    cfg = bot.config.get(guild_id)
    
    if not cfg:
        await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
        return

    role = interaction.guild.get_role(cfg.role_id)
    
    stats_embed = discord.Embed(
        title=f"📊 Statistics for {interaction.guild.name}",
        description=f"Tracking the `{cfg.command}` command for the {role.mention if role else 'Unknown Role'}.",
        color=discord.Color.blue()
    )
    
    stats_embed.add_field(
        name="🔑 Key Inventory",
        value=f"**Available:** {cfg.stats['total_keys']}\n"
              f"**Total Added:** {cfg.stats['keys_added']}\n"
              f"**Total Used/Removed:** {cfg.stats['keys_removed']}",
        inline=True
    )
    
    stats_embed.add_field(
        name="✨ Claim Activity",
        value=f"**Successful:** {cfg.stats['successful_claims']}\n"
              f"**Failed:** {cfg.stats['failed_claims']}",
        inline=True
    )

    last_claim = cfg.stats.get('last_claim_time', 0)
    last_claim_str = f"<t:{last_claim}:R>" if last_claim > 0 else "Never"
    stats_embed.add_field(
        name="⌛ Last Successful Claim",
        value=last_claim_str,
        inline=False
    )
    
    stats_embed.set_footer(text=f"Realm Keeper | Guild ID: {guild_id}")
    
    await interaction.response.send_message(embed=stats_embed, ephemeral=True)

class RealmKeeper(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix='!', intents=intents)
        self.config = dict()
        self.locks = defaultdict(asyncio.Lock)
        self.registered_commands = set()
        
        # This list of commands is now defined before the class, so this works.
        self.admin_commands = [setup, addkeys, removekeys, loadkeys, customize, clearkeys, stats]

    async def setup_hook(self):
        """Initialize bot systems"""
        try:
            await self.load_config()
            
            # Add all commands to the tree
            for cmd in self.admin_commands:
                self.tree.add_command(cmd)
            
            # Sync global commands
            await self.tree.sync()
            logging.info("✅ Global commands synced")
            
            # Create and sync guild-specific commands for existing configs
            for guild_id, cfg in self.config.items():
                if self.get_guild(guild_id):
                    await self._create_dynamic_command(guild_id, cfg.command)
            
            logging.info("✅ Realm Keeper initialized")
        except Exception as e:
            logging.error(f"Setup error: {e}")
            raise

    async def on_ready(self):
        """Called when bot is ready"""
        try:
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name="for ✨ mystical keys"
            )
            await self.change_presence(activity=activity)
            logging.info(f"✅ Bot ready as {self.user}")
        except Exception as e:
            logging.error(f"Ready event error: {e}")
            raise

    async def load_config(self):
        """
        Loads configuration from realms.json.
        """
        try:
            with open('realms.json') as f:
                realms = json.load(f)
                for gid, data in realms.items():
                    guild_id = int(gid)
                    self.config[guild_id] = GuildConfig(data['role_id'])
                    cfg = self.config[guild_id]
                    cfg.command = data.get('command', 'claim')
                    cfg.key_store = set(data.get('keys', []))
                    cfg.success_msgs = data.get('success_msgs', DRAMATIC_MESSAGES.copy())
                    cfg.custom_cooldown = data.get('custom_cooldown', 300)
                    
                    saved_stats = data.get('stats', {})
                    if saved_stats:
                        cfg.stats.update(saved_stats)
                    cfg.stats['total_keys'] = len(cfg.key_store)
                    
                    for key in cfg.key_store:
                        cfg.key_filter.add(key)
        except FileNotFoundError:
            logging.warning("No existing configuration found. realms.json will be created.")
            self.config = {}
        except json.JSONDecodeError:
            logging.error("Could not decode realms.json. File might be corrupt.")
            self.config = {}

    async def _create_dynamic_command(self, guild_id: int, command_name: str):
        """Create dynamic claim command for a guild"""
        try:
            guild = self.get_guild(guild_id)
            if not guild:
                logging.error(f"Guild {guild_id} not found")
                return False
            
            # Since admin commands are global, we only need to manage the one dynamic command here.
            # We clear guild commands to ensure no old dynamic commands are left over.
            self.tree.clear_commands(guild=guild)
            
            @app_commands.command(name=command_name, description="✨ Claim your role with a mystical key")
            @app_commands.guild_only()
            @app_commands.default_permissions() # This is the key to making it visible to @everyone
            async def claim_command(interaction: discord.Interaction):
                """Claim your role with a key"""
                if interaction.guild_id != guild_id:
                    return
                
                await interaction.response.send_modal(ArcaneGatewayModal())
            
            self.tree.add_command(claim_command, guild=guild)
            await self.tree.sync(guild=guild)
            
            self.registered_commands.add((guild_id, command_name))
            logging.info(f"✅ Created command /{command_name} in guild {guild_id}")
            
            return True
        except Exception as e:
            logging.error(f"Failed to create command '{command_name}' in guild {guild_id}: {e}")
            return False

    async def save_config(self):
        """
        Saves the current configuration to realms.json.
        """
        data = {
            str(gid): {
                'role_id': cfg.role_id,
                'command': cfg.command,
                'keys': list(cfg.key_store),
                'success_msgs': cfg.success_msgs,
                'custom_cooldown': cfg.custom_cooldown,
                'stats': cfg.stats
            }
            for gid, cfg in self.config.items()
        }
        with open('realms.json', 'w') as f:
            json.dump(data, f, indent=4)

    async def process_claim(self, interaction: discord.Interaction, key: str):
        """Process a key claim attempt"""
        try:
            await interaction.response.defer(ephemeral=True)
            
            guild_id = interaction.guild.id
            cfg = self.config.get(guild_id)
            
            if not cfg:
                await interaction.followup.send(
                    "🌌 The mystical gateway has not yet been established in this realm! An admin must run `/setup`.",
                    ephemeral=True
                )
                return

            role = interaction.guild.get_role(cfg.role_id)
            if not role:
                await interaction.followup.send(
                    "⚠️ The destined role has vanished from this realm! Seek the council of an elder.",
                    ephemeral=True
                )
                return

            if role in interaction.user.roles:
                await interaction.followup.send(
                    "✨ You have already been blessed with this power! One cannot claim the same blessing twice.",
                    ephemeral=True
                )
                return

            bot_member = interaction.guild.me
            if bot_member.top_role <= role:
                await interaction.followup.send(
                    "⚠️ My role must be higher than the role I'm trying to grant! Please move my role up in the server settings.",
                    ephemeral=True
                )
                return

            if not bot_member.guild_permissions.manage_roles:
                await interaction.followup.send(
                    "⚠️ I need the 'Manage Roles' permission to grant roles!",
                    ephemeral=True
                )
                return

            if not interaction.user.guild_permissions.administrator:
                last_try = cfg.cooldowns.get(interaction.user.id, 0)
                if time.time() - last_try < cfg.custom_cooldown:
                    remaining = int(cfg.custom_cooldown - (time.time() - last_try))
                    minutes, seconds = divmod(remaining, 60)
                    await interaction.followup.send(
                        f"⌛ The arcane energies must replenish... Return in {minutes}m {seconds}s.",
                        ephemeral=True
                    )
                    return

            try:
                uuid_obj = uuid.UUID(key.strip())
                key_normalized = str(uuid_obj).lower()
            except ValueError:
                cfg.stats['failed_claims'] += 1
                cfg.cooldowns[interaction.user.id] = time.time() # Start cooldown on failed attempt
                await interaction.followup.send(
                    "❌ Invalid key format! Keys must be in UUID format.",
                    ephemeral=True
                )
                return

            async with self.locks[guild_id]:
                if cfg.verify_key(key_normalized):
                    cfg.remove_key(key_normalized) # This updates stats['keys_removed']
                    
                    try:
                        await interaction.user.add_roles(role, reason="Key claim via Realm Keeper")
                        cfg.stats['successful_claims'] += 1
                        cfg.stats['last_claim_time'] = int(time.time())
                        
                        success_msg = random.choice(cfg.success_msgs)
                        await interaction.followup.send(
                            success_msg.format(
                                user=interaction.user.mention,
                                role=role.mention
                            ),
                            ephemeral=False # Announce success publicly
                        )
                        # Public announcement in the channel
                        await interaction.channel.send(f"A new member has joined the ranks! Welcome, {interaction.user.mention}!")

                    except discord.Forbidden:
                        # Restore key if role grant fails
                        cfg.add_key(key_normalized)
                        await self.save_config()
                        await interaction.followup.send(
                            "🔒 The mystical barriers prevent me from bestowing this power! (Role hierarchy or permissions issue)",
                            ephemeral=True
                        )
                    except Exception as e:
                        cfg.add_key(key_normalized) # Restore key
                        logging.error(f"Role grant error: {str(e)}")
                        await self.save_config()
                        await interaction.followup.send(
                            f"💔 The ritual of bestowal has failed! Error: {str(e)}",
                            ephemeral=True
                        )
                else:
                    cfg.stats['failed_claims'] += 1
                    cfg.cooldowns[interaction.user.id] = time.time()
                    await interaction.followup.send(
                        "🌑 This key holds no power in these lands...",
                        ephemeral=True
                    )
                await self.save_config() # Save config after any change
                
        except Exception as e:
            logging.error(f"Claim processing error: {str(e)}")
            try:
                await interaction.followup.send(
                    f"❌ A critical error occurred while processing your key! Please contact an admin.",
                    ephemeral=True
                )
            except discord.errors.InteractionResponded:
                pass # Already responded, can't send again

bot = RealmKeeper()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in environment")
    
    bot.run(TOKEN)