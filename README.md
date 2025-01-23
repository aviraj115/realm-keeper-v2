# realm-keeper
Keeper of your keys and channels!

## Setup
1. Clone the repository
2. Create a `.env` file with your Discord bot token:
   ```
   DISCORD_TOKEN=your_token_here
   ```
3. Copy `config.json.example` to `config.json`
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Run the bot:
   ```bash
   python realm_keeper.py
   ```

## Commands
### Admin Commands
- `/setup` - Initial server setup (Admin only)
- `/addkey` - Add a single key (Admin only)
- `/addkeys` - Bulk add keys (Admin only)
- `/removekey` - Remove a single key (Admin only)
- `/removekeys` - Bulk remove keys (Admin only)
- `/clearkeys` - Clear all keys (Admin only)
- `/keys` - Check number of available keys (Admin only)
- `/sync` - Sync bot commands (Admin only)
- `/grimoire` - Reveal the ancient tomes of knowledge

### Member Commands
- Custom command (configured during setup) - Claim your role
