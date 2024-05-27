import re
import os
import logging
import asyncio
from urllib.parse import urlparse
from aiogram import Bot, Dispatcher, types, executor
from pyrogram import Client

# Setup logging to print to the terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Aiogram setup
BOT_TOKEN = "7008524203:AAGTnDSdHaiVy6Facmf42P4XkA5r7RFIEjs"  # Replace this BOT_TOKEN
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Pyrogram setup
api_id = "1439618"  # Replace this API ID with your actual API ID
api_hash = "8c2aec88ebfe39b9db04ed9e758826d2"  # Replace this API HASH with your actual API HASH
phone_number = "+918405904190"  # Replace this with your phone number

user_client = Client("my_account", api_id=api_id, api_hash=api_hash, phone_number=phone_number)

scrape_queue = asyncio.Queue()

# Define admin user IDs
admin_ids = [629986639, 12345678]

default_limit = 10000  # Max limit for any user
admin_limit = 50000  # Max limit for admin users

def remove_duplicates(messages):
    unique_messages = list(set(messages))
    duplicates_removed = len(messages) - len(unique_messages)
    return unique_messages, duplicates_removed

def normalize_filter_text(filter_text):
    """Normalize the filter text for consistent searching."""
    if filter_text is None:
        return []
    variations = [
        filter_text.lower(),
        filter_text.replace(" ", "").lower(),
        filter_text.replace(" ", "-").lower(),
    ]
    return variations

async def search_messages_by_filter(user_client, channel_username, limit, filter_text):
    filtered_messages = []
    count = 0

    filter_variations = normalize_filter_text(filter_text)
    logging.info(f"Starting to search messages from {channel_username} with limit {limit} and filter {filter_text}")

    async for message in user_client.search_messages(channel_username):
        if count >= limit:
            break

        text = message.text if message.text else message.caption
        if text:
            if not filter_variations or any(variation in text.lower().replace(" ", "") for variation in filter_variations):
                filtered_messages.append(text)
                count += 1

    logging.info(f"Search completed. Found {len(filtered_messages)} messages matching the filter.")
    return filtered_messages

async def scrape_messages_from_filtered(filtered_messages, start_number=None):
    messages = []
    pattern = r'\d{16}\D*\d{2}\D*\d{2,4}\D*\d{3,4}'

    for text in filtered_messages:
        matched_messages = re.findall(pattern, text)
        if matched_messages:
            formatted_messages = []
            for matched_message in matched_messages:
                extracted_values = re.findall(r'\d+', matched_message)
                if len(extracted_values) == 4:
                    card_number, mo, year, cvv = extracted_values
                    year = year[-2:]
                    formatted_messages.append(f"{card_number}|{mo}|{year}|{cvv}")

            messages.extend(formatted_messages)

    if start_number:
        messages = [msg for msg in messages if msg.startswith(start_number)]

    return messages

async def process_scrape_queue(user_client, bot):
    while True:
        task = await scrape_queue.get()
        message, channel_username, limit, start_number, filter_text, temporary_msg = task

        logging.info(f"Received task with filter: {filter_text}")

        try:
            filtered_messages = await asyncio.wait_for(search_messages_by_filter(user_client, channel_username, limit, filter_text), timeout=600)
            scrapped_results = await scrape_messages_from_filtered(filtered_messages, start_number)

            if scrapped_results:
                unique_messages, duplicates_removed = remove_duplicates(scrapped_results)
                if unique_messages:
                    file_name = f"x{len(unique_messages)}_{channel_username}.txt"
                    with open(file_name, 'w', encoding='utf-8') as f:
                        f.write(f"Text Filtered: {filter_text}\n\n")
                        f.write("\n".join(unique_messages))

                    with open(file_name, 'rb') as f:
                        caption = (
                            f"<b>CC Scrapped Successful ✅</b>\n"
                            f"<b>━━━━━━━━━━━━━━━━</b>\n"
                            f"<b>Source:</b> <code>{channel_username}</code>\n"
                            f"<b>Amount:</b> <code>{len(unique_messages)}</code>\n"
                            f"<b>Duplicates Removed:</b> <code>{duplicates_removed}</code>\n"
                            f"<b>Text Filtered:</b> <code>{filter_text}</code>\n"
                            f"<b>━━━━━━━━━━━━━━━━</b>\n"
                            f"<b>Card-Scrapper By: <a href='https://t.me/chillyyyyyyyy'>Charlie</a></b>\n"
                        )
                        await temporary_msg.delete()
                        await bot.send_document(message.chat.id, f, caption=caption, parse_mode='html')
                    os.remove(file_name)
                else:
                    await temporary_msg.delete()
                    await bot.send_message(message.chat.id, "Sorry Bro ❌ No Credit Card Found")
            else:
                await temporary_msg.delete()
                await bot.send_message(message.chat.id, "Sorry Bro ❌ No Credit Card Found")
        except asyncio.TimeoutError:
            await temporary_msg.delete()
            await bot.send_message(message.chat.id, "Sorry Bro ❌ I was not able to find any matching text within 10 minutes. Try something else.")

        scrape_queue.task_done()

@dp.message_handler(commands=['start'])
async def start_cmd(message: types.Message):
    start_text = (
        "👋 Welcome to the Card Scraper Bot!\n\n"
        "🔹 Use <code>/scr &lt;channel&gt; &lt;limit&gt; &lt;filter&gt;</code> to scrape credit card details.\n"
        "🔹 Example: <code>/scr @group 10 Abudhabi</code>\n"
        "🔹 If no filter is provided, it will scrape any card numbers.\n\n"
        "🔹 Admins can set a higher limit for scraping.\n\n"
        "Happy scraping! 😎"
    )
    await message.reply(start_text, parse_mode='html')

@dp.message_handler(commands=['scr'])
async def scr_cmd(message: types.Message):
    args = message.text.split(maxsplit=3)[1:]
    if len(args) < 2 or len(args) > 3:
        await bot.send_message(message.chat.id, "<b>⚠️ Provide channel username, amount to scrape, and optional filter</b>", parse_mode='html')
        return

    channel_identifier = args[0]
    try:
        limit = int(args[1])
    except ValueError:
        await bot.send_message(message.chat.id, "<b>⚠️ Invalid amount to scrape</b>", parse_mode='html')
        return

    # Determine the max limit based on whether the user is an admin
    max_lim = admin_limit if message.from_user.id in admin_ids else default_limit

    if limit > max_lim:
        await bot.send_message(message.chat.id, f"<b>Sorry Bro! Amount over Max limit is {max_lim} ❌</b>", parse_mode='html')
        return

    start_number = None
    filter_text = None
    if len(args) == 3:
        if args[2].isdigit():
            start_number = args[2]
        else:
            filter_text = args[2]

    logging.info(f"Parsed command with filter: {filter_text}")

    parsed_url = urlparse(channel_identifier)
    if parsed_url.scheme and parsed_url.netloc:
        if parsed_url.path.startswith('/+'):
            try:
                chat = await user_client.join_chat(channel_identifier)
                channel_username = chat.id
            except Exception as e:
                if "USER_ALREADY_PARTICIPANT" in str(e):
                    try:
                        chat = await user_client.get_chat(channel_identifier)
                        channel_username = chat.id
                    except Exception as e:
                        await bot.send_message(message.chat.id, f"<b>Sorry Bro! 🥲 No ccs found</b>", parse_mode='html')
                        return
                else:
                    await bot.send_message(message.chat.id, f"<b>Sorry Bro! 🥲 No ccs found</b>", parse_mode='html')
                    return
        else:
            channel_username = parsed_url.path.lstrip('/')
    else:
        channel_username = channel_identifier

    try:
        await user_client.get_chat(channel_username)
    except Exception:
        await bot.send_message(message.chat.id, "<b>Hey Bro! 🥲 Incorrect username ❌</b>", parse_mode='html')
        return

    temporary_msg = await bot.send_message(message.chat.id, "<b>Scraping in progress wait.....</b>", parse_mode='html')

    await scrape_queue.put((message, channel_username, limit, start_number, filter_text, temporary_msg))

async def on_startup(dp):
    await user_client.start()
    asyncio.create_task(process_scrape_queue(user_client, bot))

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
