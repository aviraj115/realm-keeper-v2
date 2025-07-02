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
from dotenv import load_dotenv

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.FileHandler('realm.log'), logging.StreamHandler()]
)

# --- Default Messages ---
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

# --- Guild Configuration Class ---
class GuildConfig:
    """Stores all configuration and data for a single guild."""
    __slots__ = ('role_id', 'command', 'key_filter', 'key_store', 
                 'cooldowns', 'success_msgs', 'stats', 'custom_cooldown', 
                 'filter_path', 'announcement_channel_id')
    
    def __init__(self, role_id: int, guild_id: int):
        self.role_id = role_id
        self.command = "claim"
        self.filter_path = f'bloom_filters/filter_{guild_id}.bloom'
        self.announcement_channel_id = None # ID for the announcement channel
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
        """Add a key to the key store and Bloom filter."""
        try:
            key_normalized = str(uuid.UUID(key)).lower()
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
        """Remove a key from the key store."""
        try:
            key_normalized = str(uuid.UUID(key)).lower()
            if key_normalized in self.key_store:
                self.key_store.remove(key_normalized)
                self.stats['keys_removed'] += 1
                self.stats['total_keys'] = len(self.key_store)
                return True
            return False
        except ValueError:
            return False

    def verify_key(self, key: str) -> bool:
        """Verify if a key is valid using the Bloom filter and then the key store."""
        try:
            key_normalized = str(uuid.UUID(key)).lower()
            if key_normalized not in self.key_filter:
                return False
            return key_normalized in self.key_store
        except ValueError:
            return False

# --- Modals ---
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
        placeholder="e.g., claim, verify (no slash)",
        default="claim",
        min_length=1,
        max_length=32
    )
    announcement_channel_input = discord.ui.TextInput(
        label="📢 Announcement Channel (Optional)",
        placeholder="Enter the exact name of the text channel for success messages",
        required=False,
        max_length=100
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
            bot = interaction.client
            
            role_name = self.role_name_input.value.strip()
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if not role:
                await interaction.followup.send(f"❌ Role '{role_name}' not found!", ephemeral=True)
                return
            
            command_name = self.command_name_input.value.strip().lower()
            
            # Channel Processing
            channel_name = self.announcement_channel_input.value.strip()
            announcement_channel = None
            if channel_name:
                announcement_channel = discord.utils.get(interaction.guild.text_channels, name=channel_name)
                if not announcement_channel:
                    await interaction.followup.send(f"❌ Text channel '{channel_name}' not found! Please enter the exact name (case-sensitive).", ephemeral=True)
                    return

            guild_id = interaction.guild.id
            
            # If re-configuring, get old command name to check for conflicts
            old_command_name = None
            if guild_id in bot.config:
                old_command_name = bot.config[guild_id].command

            # Prevent setting a command name that's already a global/admin command
            # unless it's the command we're trying to rename
            reserved_names = [cmd.name for cmd in bot.tree.get_commands()]
            if command_name in reserved_names and command_name != old_command_name:
                 await interaction.followup.send(f"⚠️ Command name `/{command_name}` is already in use by the bot's admin commands! Choose another.", ephemeral=True)
                 return

            is_new_setup = guild_id not in bot.config
            
            if is_new_setup:
                bot.config[guild_id] = GuildConfig(role.id, guild_id)
            
            cfg = bot.config[guild_id]
            cfg.command = command_name
            cfg.role_id = role.id # Update role ID in case it changed
            if announcement_channel:
                cfg.announcement_channel_id = announcement_channel.id
            else:
                cfg.announcement_channel_id = None

            
            added, invalid = 0, 0
            initial_keys = self.initial_keys_input.value.strip()
            if initial_keys:
                keys = [k.strip() for k in initial_keys.split('\n') if k.strip()]
                for key in keys:
                    if cfg.add_key(key):
                        added += 1
                    else:
                        invalid += 1
            
            try:
                await bot.register_guild_commands(interaction.guild, command_name)
            except Exception as e:
                logging.error(f"Error registering commands during setup: {e}", exc_info=True)
                await interaction.followup.send("⚠️ Failed to create or update the slash command.", ephemeral=True)
                # Don't delete config on failure, just log the error
                return

            await bot.save_config()

            response = [
                f"✨ Realm configuration updated for {role.mention}!",
                f"Use `/{command_name}` to claim the role."
            ]
            if announcement_channel:
                response.append(f"📢 Success messages will be posted in {announcement_channel.mention}.")
            if initial_keys:
                response.append(f"\n📦 Loaded {added} initial keys ({invalid} were invalid or duplicates).")
            
            await interaction.followup.send("\n".join(response), ephemeral=True)
        except Exception as e:
            logging.error(f"Setup modal error: {str(e)}", exc_info=True)
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
        await interaction.followup.send(f"✨ Success messages updated! There are now {len(messages)} unique messages.", ephemeral=True)

# --- Admin Cog & Commands ---
class AdminCog(commands.Cog):
    """Cog for all admin-level commands."""
    def __init__(self, bot: "RealmKeeper"):
        self.bot = bot

    async def cog_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("🛡️ You must be a server administrator to use this command.", ephemeral=True)
            return False
        return True

    @app_commands.command(name="setup", description="🏰 Initialize or reconfigure the bot for this server.")
    async def setup(self, interaction: discord.Interaction):
        if not interaction.guild.me.guild_permissions.manage_roles:
            await interaction.response.send_message("🔒 I need the 'Manage Roles' permission to function!", ephemeral=True)
            return
        await interaction.response.send_modal(SetupModal())

    @app_commands.command(name="addkeys", description="📚 Add multiple keys to the store via a modal.")
    async def addkeys(self, interaction: discord.Interaction):
        if interaction.guild_id not in interaction.client.config:
            await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
            return
        await interaction.response.send_modal(BulkKeyModal())

    @app_commands.command(name="removekeys", description="🗑️ Remove multiple keys from the store via a modal.")
    async def removekeys(self, interaction: discord.Interaction):
        if interaction.guild_id not in interaction.client.config:
            await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
            return
        await interaction.response.send_modal(RemoveKeysModal())

    @app_commands.command(name="loadkeys", description="📤 Load keys from a text file.")
    @app_commands.describe(
        file="The text file containing keys (one per line).",
        overwrite="Select True to remove all existing keys before adding new ones."
    )
    async def loadkeys(self, interaction: discord.Interaction, file: discord.Attachment, overwrite: bool = False):
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

        if file.size > 5 * 1024 * 1024:
            await interaction.followup.send("⚖️ File is too large (max 5MB).", ephemeral=True)
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
    async def customize(self, interaction: discord.Interaction):
        cfg = interaction.client.config.get(interaction.guild_id)
        if not cfg:
            await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
            return
        await interaction.response.send_modal(CustomizeModal(cfg.success_msgs))

    @app_commands.command(name="clearkeys", description="🗑️ Remove ALL available keys from the store.")
    async def clearkeys(self, interaction: discord.Interaction):
        cfg = interaction.client.config.get(interaction.guild_id)
        if not cfg:
            await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        
        async with self.bot.locks[interaction.guild_id]:
            key_count = len(cfg.key_store)
            cfg.key_store.clear()
            cfg.key_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
            cfg.stats['keys_removed'] += key_count
            cfg.stats['total_keys'] = 0
        
        await self.bot.save_config()
        await interaction.followup.send(f"🗑️ Cleared all {key_count} keys!", ephemeral=True)

    @app_commands.command(name="stats", description="📊 View statistics for this realm.")
    async def stats(self, interaction: discord.Interaction):
        cfg = interaction.client.config.get(interaction.guild_id)
        if not cfg:
            await interaction.response.send_message("❌ Run `/setup` first!", ephemeral=True)
            return

        role = interaction.guild.get_role(cfg.role_id)
        
        embed = discord.Embed(
            title=f"📊 Statistics for {interaction.guild.name}",
            description=f"Tracking the `/{cfg.command}` command for the {role.mention if role else 'Unknown Role'}.",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="🔑 Key Inventory",
            value=f"**Available:** {cfg.stats['total_keys']}\n"
                  f"**Total Added:** {cfg.stats['keys_added']}\n"
                  f"**Total Used/Removed:** {cfg.stats['keys_removed']}",
            inline=True
        )
        embed.add_field(
            name="✨ Claim Activity",
            value=f"**Successful:** {cfg.stats['successful_claims']}\n"
                  f"**Failed:** {cfg.stats['failed_claims']}",
            inline=True
        )
        
        announcement_ch_id = cfg.announcement_channel_id
        announcement_ch_str = "Not Set"
        if announcement_ch_id:
            channel = interaction.guild.get_channel(announcement_ch_id)
            announcement_ch_str = channel.mention if channel else f"Invalid Channel (ID: {announcement_ch_id})"

        embed.add_field(
            name="📢 Announcements",
            value=f"**Channel:** {announcement_ch_str}",
            inline=True
        )

        last_claim = cfg.stats.get('last_claim_time', 0)
        last_claim_str = f"<t:{last_claim}:R>" if last_claim > 0 else "Never"
        embed.add_field(name="⌛ Last Successful Claim", value=last_claim_str, inline=False)
        embed.set_footer(text=f"Realm Keeper | Guild ID: {interaction.guild_id}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

# --- Dynamic Claim Cog ---
class ClaimCog(commands.Cog):
    """A cog created dynamically for each guild's claim command."""
    def __init__(self, bot: "RealmKeeper", command_name: str):
        self.bot = bot
        self._claim_command = app_commands.Command(
            name=command_name,
            description="✨ Claim your role with a mystical key",
            callback=self.claim_callback
        )

    async def claim_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ArcaneGatewayModal())
    
    def cog_load(self):
        self.bot.tree.add_command(self._claim_command, guild=self.guild)

    def cog_unload(self):
        self.bot.tree.remove_command(self._claim_command.name, guild=self.guild)

# --- Main Bot Class ---
class RealmKeeper(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix='!', intents=intents)
        self.config = dict()
        self.locks = defaultdict(asyncio.Lock)
        self.save_task = None
        
    async def setup_hook(self):
        try:
            await self.load_config()
            # AdminCog contains global commands, so it's added without a guild list.
            await self.add_cog(AdminCog(self))
            self.save_task = asyncio.create_task(self.periodic_save())
        except Exception as e:
            logging.error(f"Setup error: {e}", exc_info=True)
            raise

    async def on_ready(self):
        try:
            # Register one cog per configured guild to handle its dynamic command
            for guild in self.guilds:
                if guild.id in self.config:
                    cfg = self.config[guild.id]
                    if cfg.command:
                        await self.register_guild_commands(guild, cfg.command)

            # Sync global commands once.
            await self.tree.sync()
            logging.info("✅ Global and guild commands synced.")

            activity = discord.Activity(type=discord.ActivityType.watching, name="for ✨ mystical keys")
            await self.change_presence(activity=activity)
            logging.info(f"✅ Bot ready as {self.user}")
        except Exception as e:
            logging.error(f"Ready event error: {e}", exc_info=True)
            # Avoid raising the exception here to prevent the bot from crashing on startup

    async def on_guild_remove(self, guild: discord.Guild):
        if guild.id in self.config:
            async with self.locks['global']:
                del self.config[guild.id]
                await self.save_config()
            logging.info(f"Removed configuration for guild {guild.id} as I was removed.")

    async def periodic_save(self):
        await self.wait_until_ready()
        while not self.is_closed():
            await asyncio.sleep(300)
            logging.info("Initiating periodic configuration save...")
            async with self.locks['global']:
                await self.save_config()
            logging.info("Periodic configuration save complete.")

    async def load_config(self):
        if not os.path.exists('bloom_filters'):
            os.makedirs('bloom_filters')
            logging.info("Created 'bloom_filters' directory.")

        try:
            with open('realms.json', 'r') as f:
                realms = json.load(f)
            for gid, data in realms.items():
                guild_id = int(gid)
                self.config[guild_id] = GuildConfig(data['role_id'], guild_id)
                cfg = self.config[guild_id]
                cfg.command = data.get('command', 'claim')
                cfg.key_store = set(data.get('keys', []))
                cfg.success_msgs = data.get('success_msgs', DRAMATIC_MESSAGES.copy())
                cfg.custom_cooldown = data.get('custom_cooldown', 300)
                cfg.announcement_channel_id = data.get('announcement_channel_id', None)
                
                saved_stats = data.get('stats', {})
                if saved_stats:
                    cfg.stats.update(saved_stats)
                cfg.stats['total_keys'] = len(cfg.key_store)
                
                try:
                    with open(cfg.filter_path, "rb") as bf:
                        cfg.key_filter = ScalableBloomFilter.fromfile(bf)
                    logging.info(f"Loaded bloom filter from {cfg.filter_path} for guild {guild_id}")
                except FileNotFoundError:
                    logging.warning(f"No bloom filter file found for guild {guild_id}. Rebuilding...")
                    for key in cfg.key_store:
                        cfg.key_filter.add(key)
        except FileNotFoundError:
            logging.warning("No existing configuration found. realms.json will be created.")
            self.config = {}
        except json.JSONDecodeError:
            logging.error("Could not decode realms.json. File might be corrupt.")
            self.config = {}

    async def save_config(self):
        data_to_save = {
            str(gid): {
                'role_id': cfg.role_id,
                'command': cfg.command,
                'keys': list(cfg.key_store),
                'success_msgs': cfg.success_msgs,
                'custom_cooldown': cfg.custom_cooldown,
                'announcement_channel_id': cfg.announcement_channel_id,
                'stats': cfg.stats
            }
            for gid, cfg in self.config.items()
        }
        with open('realms.json', 'w') as f:
            json.dump(data_to_save, f, indent=4)

        for gid, cfg in self.config.items():
            try:
                with open(cfg.filter_path, "wb") as bf:
                    cfg.key_filter.tofile(bf)
            except Exception as e:
                logging.error(f"Could not save bloom filter for guild {gid}: {e}")

    async def register_guild_commands(self, guild: discord.Guild, command_name: str):
        """
        Registers or updates the dynamic claim command for a single guild.
        This function ensures one cog per guild for its dynamic command.
        """
        cog_name = f"ClaimCog_{guild.id}"
        existing_cog = self.get_cog(cog_name)

        if existing_cog:
            # If the cog exists, we might be renaming the command.
            # We need to remove the old command before adding the new one.
            await self.remove_cog(cog_name)
            logging.info(f"Removed old claim cog for guild {guild.id} to prepare for update.")

        # Create and add the new cog with the potentially new command name.
        claim_cog = ClaimCog(self, command_name)
        # Manually set the guild attribute so the cog knows where it belongs
        claim_cog.guild = guild
        await self.add_cog(claim_cog, guilds=[guild])
        logging.info(f"Registered command `/{command_name}` for guild {guild.name} ({guild.id})")
        
        # Sync the commands for this specific guild to make the change live.
        await self.tree.sync(guild=guild)
        logging.info(f"Synced commands for guild {guild.id} after command update.")


    async def process_claim(self, interaction: discord.Interaction, key: str):
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild.id
        cfg = self.config.get(guild_id)
        
        if not cfg:
            await interaction.followup.send("🌌 The mystical gateway has not yet been established in this realm! An admin must run `/setup`.", ephemeral=True)
            return

        role = interaction.guild.get_role(cfg.role_id)
        if not role:
            await interaction.followup.send("⚠️ The destined role has vanished from this realm!", ephemeral=True)
            return

        if role in interaction.user.roles:
            await interaction.followup.send("✨ You have already been blessed with this power!", ephemeral=True)
            return

        if interaction.guild.me.top_role <= role:
            await interaction.followup.send("⚠️ My role must be higher than the role I'm trying to grant!", ephemeral=True)
            return

        if not interaction.user.guild_permissions.administrator:
            last_try = cfg.cooldowns.get(interaction.user.id, 0)
            if time.time() - last_try < cfg.custom_cooldown:
                remaining = int(cfg.custom_cooldown - (time.time() - last_try))
                minutes, seconds = divmod(remaining, 60)
                await interaction.followup.send(f"⌛ The arcane energies must replenish... Return in {minutes}m {seconds}s.", ephemeral=True)
                return

        try:
            key_normalized = str(uuid.UUID(key.strip())).lower()
        except ValueError:
            cfg.stats['failed_claims'] += 1
            cfg.cooldowns[interaction.user.id] = time.time()
            await interaction.followup.send("❌ Invalid key format! Keys must be in UUID format.", ephemeral=True)
            return

        async with self.locks[guild_id]:
            if cfg.verify_key(key_normalized):
                cfg.remove_key(key_normalized)
                
                try:
                    await interaction.user.add_roles(role, reason="Key claim via Realm Keeper")
                    
                    cfg.stats['successful_claims'] += 1
                    cfg.stats['last_claim_time'] = int(time.time())
                    
                    success_msg = random.choice(cfg.success_msgs).format(user=interaction.user.mention, role=role.mention)
                    
                    announcement_channel = None
                    if cfg.announcement_channel_id:
                        announcement_channel = interaction.guild.get_channel(cfg.announcement_channel_id)

                    if announcement_channel and announcement_channel.permissions_for(interaction.guild.me).send_messages:
                        await announcement_channel.send(success_msg)
                        await interaction.followup.send(f"✅ Success! You have been granted the {role.mention} role. An announcement was made in {announcement_channel.mention}.", ephemeral=True)
                    else:
                        await interaction.followup.send(success_msg, ephemeral=False)
                        if cfg.announcement_channel_id and not announcement_channel:
                            logging.warning(f"Could not find announcement channel {cfg.announcement_channel_id} in guild {guild_id}.")
                        elif announcement_channel:
                             logging.warning(f"Missing 'Send Messages' permission in announcement channel {announcement_channel.name} ({cfg.announcement_channel_id}) in guild {guild_id}.")

                except (discord.Forbidden, discord.HTTPException) as e:
                    logging.error(f"Failed to grant role to {interaction.user}. Restoring key. Error: {e}")
                    cfg.add_key(key_normalized)
                    await interaction.followup.send("🔒 The mystical barriers prevent me from bestowing this power! Your key has not been consumed.", ephemeral=True)
                except Exception as e:
                    logging.error(f"An unexpected error occurred during role grant. Restoring key. Error: {e}", exc_info=True)
                    cfg.add_key(key_normalized)
                    await interaction.followup.send("💔 The ritual of bestowal has failed unexpectedly. Your key has not been consumed.", ephemeral=True)
            else:
                cfg.stats['failed_claims'] += 1
                cfg.cooldowns[interaction.user.id] = time.time()
                await interaction.followup.send("🌑 This key holds no power in these lands...", ephemeral=True)

if __name__ == "__main__":
    print("--- Realm Keeper script starting ---")
    load_dotenv()
    
    TOKEN = os.getenv('DISCORD_TOKEN')
    if not TOKEN:
        raise ValueError("Missing DISCORD_TOKEN in .env file or environment variables")
    
    bot = RealmKeeper()
    print("--- Token loaded, attempting to run bot ---")
    bot.run(TOKEN)
