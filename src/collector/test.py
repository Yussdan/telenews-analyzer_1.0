from telethon import TelegramClient
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = '20271031'
API_HASH = '9657542463b5aebdc610b1b3eb4a9837'
SESSION = 'teletrends'

client = TelegramClient(SESSION, API_ID, API_HASH)

async def main():
    await client.start()
    me = await client.get_me()
    logger.info(f"Successfully connected as {me.username}")
    
    # Test: get messages from a public channel
    channel = await client.get_entity('@ru2ch')
    messages = await client.get_messages(channel, limit=10)
    for msg in messages:
        time.sleep(5)
        logger.info(msg.text)

with client:
    client.loop.run_until_complete(main())