import datetime
import logging
import os
import csv
import io
import azure.functions as func
import azure.storage.blob as blob

PAGES = "pages.csv"

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    connection_string = os.getenv("AZURE_STORAGE_BLOB_CONNECTIONSTRING")
    service = blob.BlobServiceClient.from_connection_string(connection_string)
    blob_client = service.get_container_client(container="configuration")

    pagesContent = blob_client.download_blob(PAGES).content_as_text()
    stream = io.StringIO(pagesContent)
    pages_reader = csv.DictReader(stream)
    
    for page in pages_reader:
        logging.info("%s -> %s", page["page_name"], page["page_tag"])