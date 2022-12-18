import datetime
import os
import csv
import io
import azure.functions as func
import azure.storage.blob as blob
from facebook_scraper import get_posts
import telegram

PAGES = "pages.csv"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FIELDS = ["telegram_name", "fbpage_tag", "last_post_date"]
MAX_COUNT_FLOOD_CONTROL = 20

def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        return

    connection_string = os.getenv("AZURE_STORAGE_BLOB_CONNECTIONSTRING")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    bot = telegram.bot.Bot(token)

    service = blob.BlobServiceClient.from_connection_string(connection_string)
    blob_client = service.get_container_client(container="configuration")
    writer_steram = io.StringIO()

    pagesContent = blob_client.download_blob(PAGES).content_as_text()
    pages_reader = csv.DictReader(io.StringIO(pagesContent))
    pages_writer = csv.DictWriter(writer_steram, FIELDS)
    pages_writer.writeheader()

    messages = 0
    for page in pages_reader:
        posts = list(get_posts(page["fbpage_tag"], page_limit=2))
        posts.sort(key = lambda x: x["time"])
        last_post_dt = datetime.datetime.strptime(page["last_post_date"], DATE_FORMAT)

        for post in posts:
            if messages >= MAX_COUNT_FLOOD_CONTROL: # next time will be the rest
                break

            if post["time"] <= last_post_dt: # skip this one
                continue

            if post["image"] is not None or post["image_lowquality"] is not None:
                bot.send_photo(chat_id, post["image"] if post["image"] else post["image_lowquality"], (post['text'] if post['text'] else ''))
                if (post['time'] > last_post_dt):
                    last_post_dt = post['time']
            elif post["text"] is not None:
                bot.send_message(chat_id, (post['text'] if post['text'] else ''))
                if (post['time'] > last_post_dt):
                    last_post_dt = post['time']

            messages += 1

        row = {'telegram_name': page['telegram_name'], 'fbpage_tag': page['fbpage_tag'], 'last_post_date': last_post_dt}
        pages_writer.writerow(row)

    writer_steram.flush()
    blob_client.upload_blob(name = PAGES, data = writer_steram.getvalue(), overwrite = True)