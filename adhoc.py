import os
import logging

# import azure.functions as func
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient
from sqlalchemy import create_engine, text
import azure.functions as func
from dateutil import parser
import re
from dotenv import load_dotenv

load_dotenv(r"C:\Users\A200234345\OneDrive - Deutsche Telekom AG\Dokumente\Lernen\podsum\rss-parse-function-app-python\.env")
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

sql_server_name = os.environ["SQLServerName"]
database_name = os.environ["DBName"]
sql_username = os.environ["SQLUserName"]
sql_password = os.environ["SQLPass"]
storage_account_name = os.environ["storageAccountName"]
storage_account_key = os.environ["storageAccountKey"]
container_name = "xml"


# Construct the SQLAlchemy connection string
connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
engine = create_engine(connection_string)

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient(
    account_url=f"https://{storage_account_name}.blob.core.windows.net",
    credential=storage_account_key)
container_client = blob_service_client.get_container_client(container_name)
print(f"container_client:{container_client}")

query = "SELECT TOP 1 * \
         FROM dbo.rss_urls \
         WHERE last_parsed < CAST(GETDATE() AS DATE)"

# Execute the query and fetch the result
with engine.connect() as connection:
    result = connection.execute(text(query))
    top_record = result.fetchone()

# Print the top record
if top_record:
    print("Top record from rss_urls table:")
    print(dict(top_record))
else:
    print("No records found in rss_urls table.")

#     def insert_rss_item(title, description, pub_date, enclosure_url, podcast_title, language):
#         title = re.sub(r'[^\w\-_\. ]', '_', title.replace("'", "''"))
#         description = description.replace("'", "''")
#         podcast_title = re.sub(r'[^\w\-_\. ]', '_', podcast_title.replace("'", "''"))

#         try:
#             with engine.begin() as conn:
#                 # Check if the item already exists
#                 check_query = text("SELECT 1 FROM rss_schema.rss_feed WHERE link = :enclosure_url")
#                 result = conn.execute(check_query, {'enclosure_url': enclosure_url}).fetchone()
                
#                 # If the item doesn't exist, insert it
#                 if result is None:
#                     insert_query = text("""
#                         INSERT INTO rss_schema.rss_feed (title, description, pubDate, link, parse_dt, download_flag_azure, podcast_title, language)
#                         VALUES (:title, :description, :pub_date, :enclosure_url, GETDATE(), 'N', :podcast_title, :language)
#                     """)
#                     conn.execute(insert_query, {
#                         'title': title,
#                         'description': description,
#                         'pub_date': pub_date,
#                         'enclosure_url': enclosure_url,
#                         'podcast_title': podcast_title,
#                         'language': language
#                     })
#                     logging.info(f"Item inserted: {title}")
#         except Exception as e:
#             logging.error(f"Failed to insert item: {title}. Error: {str(e)}")
    

#     for blob in container_client.list_blobs():
#         blob_client = container_client.get_blob_client(blob)
#         blob_content = blob_client.download_blob().readall()
#         local_path = f"/tmp/{blob.name}"  # Correcting the path to use /tmp directory

#         # Write blob content to a local file
#         with open(local_path, 'wb') as file:
#             file.write(blob_content)
#             #logging.info(f"Successfully written the blob_content.")


#         # Load XML file
#         try:
#             tree = ET.parse(local_path)
#             root = tree.getroot()

#             # Extract podcast title and language
#             channel = root.find('.//channel')
#             podcast_title = channel.find('title').text
#             language = channel.find('language').text

#             # Process each item in the RSS feed
#             for item in channel.findall('item'):
#                 title = item.find('title').text
#                 description = item.find('description').text
#                 pub_date = parser.parse(item.find('pubDate').text)
#                 enclosure_url = item.find('enclosure').get('url')
                
#                 insert_rss_item(title, description, pub_date, enclosure_url, podcast_title, language)

#             # Delete the local file after processing
#             os.remove(local_path)

#         except Exception as e:
#             print(f"Failed to process XML file: {local_path}. Error: {e}")

#     print("Function completed for all files in the container.")


print('Hello world')