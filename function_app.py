import os
import logging
import requests
import azure.functions as func
import xml.etree.ElementTree as ET
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient
from sqlalchemy import create_engine, text
import azure.functions as func
from dateutil import parser

app = func.FunctionApp()

@app.schedule(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def rss_refresh_daily(myTimer: func.TimerRequest) -> None:

    logging.info('Starting rss_refresh_daily')

    # Setup Azure credential
    credential = DefaultAzureCredential()
    key_vault_name = os.environ["MyKeyVault"]
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
    
    logging.info(f"Key vault name fetched: {key_vault_name}")

    # Create a secret client
    client = SecretClient(vault_url=key_vault_uri, credential=credential)
    logging.info(f"Connected to client: {client}")

    try:
        # Fetch secrets from Azure Key Vault
        server_name = client.get_secret("SQLServerName").value
        database_name = client.get_secret("DBName").value
        username = client.get_secret("SQLUserName").value
        password = client.get_secret("SQLPass").value
        storage_account_name = client.get_secret("storageAccountName").value
        storage_account_key = client.get_secret("storageAccountKey").value

        logging.info(f"server_name: {server_name}")
        logging.info(f"database_name: {database_name}")
        logging.info(f"username: {username}")
        logging.info(f"password: {password}")
        logging.info(f"storage_account_name: {storage_account_name}")
        logging.info(f"storage_account_key: {storage_account_key}")
        
        logging.info("Fetched database connection details from Key Vault successfully.")
    except Exception as e:
        logging.error(f"Failed to fetch secrets from Key Vault. Error: {str(e)}")
        raise

    # Construct the SQLAlchemy connection string
    connection_string = f"mssql+pymssql://{username}:{password}@{server_name}/{database_name}"

    # Connect to SQL Database using SQLAlchemy
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            query = text("SELECT podcast_name, rss_url FROM dbo.rss_urls")
            result = conn.execute(query)
            podcasts = result.fetchall()

        logging.info(f"RSS URLs and podcast names fetched from SQL Database successfully: {podcasts}")
    except Exception as e:
        logging.error(f"Failed to fetch RSS URLs and podcast names from SQL Database. Error: {str(e)}")
        raise

    # Download and process each podcast
    for podcast_name, rss_url in podcasts:
        safe_podcast_name = podcast_name.replace(' ', '_')
        try:
            local_filename = os.path.join("/tmp", f"{safe_podcast_name}.xml")
            response = requests.get(rss_url)
            with open(local_filename, 'wb') as file:
                file.write(response.content)
            logging.info(f"XML file downloaded to {local_filename}")

            # Upload to Azure Blob Storage
            blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net/", credential=credential)
            blob_client = blob_service_client.get_blob_client(container="xml", blob=f"{safe_podcast_name}.xml")
            with open(local_filename, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logging.info("XML file has been uploaded to blob storage successfully.")

            # Clean up local file
            os.remove(local_filename)
            logging.info("Local XML file cleaned up successfully.")
        except Exception as e:
            logging.error(f"Failed to process podcast '{podcast_name}' with URL '{rss_url}'. Error: {str(e)}")
            continue

    logging.info("Function completed successfully.")
    
    
    
    
    
    
    
    
    
    
    
    
# ####################################################################################################################################################################################################################      
    
    
    
    
    
    
    
    
    
    
    
    
    
@app.timer_trigger(schedule="0 */20 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def mp3_download(myTimer: func.TimerRequest) -> None:

    logging.info("MP3 download function started...")

    # Azure Key Vault configuration
    credential = DefaultAzureCredential()
    key_vault_name = os.environ["MyKeyVault"]
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
    client = SecretClient(vault_url=key_vault_uri, credential=credential)

    # Retrieve secrets from Azure Key Vault
    storage_account_name = client.get_secret("storageAccountName").value
    container_name = "mp3"
    sql_server_name = client.get_secret("SQLServerName").value
    database_name = client.get_secret("DBName").value
    sql_username = client.get_secret("SQLUserName").value
    sql_password = client.get_secret("SQLPass").value
    
    # Construct the SQLAlchemy connection string
    connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
    engine = create_engine(connection_string)

    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net/",
        credential=credential)
    logging.info(f"Blob container client initialized for container: {container_name}")

    # Query for episodes
    query = text("""
    SELECT TOP 1 *
    FROM rss_schema.rss_feed
    WHERE download_flag_azure = 'N'
    ORDER BY pubDate DESC;
    """)
    logging.info("Query defined without issues.")

    with engine.begin() as connection:
        result = connection.execute(query)
        logging.info(f"Query executed without issues: {result}")
        episodes = result.fetchall()  # Fetch all results at once
        logging.info(f"Number of episodes fetched: {len(episodes)}")
        logging.info(f"Episodes: {episodes}")
        for episode in episodes:
            logging.info("first nest")
            podcast_title = episode[10].replace(' ', '-') if episode[10] else 'unknown_podcast'
            logging.info(f"podcast_title: {podcast_title}")
            episode_title = episode[1].replace(' ', '-') if episode[1] else 'unknown_title'
            logging.info(f"episode_title: {episode_title}")
            rss_url = episode[4]
            logging.info(f"rss_url: {rss_url}")
            folder_path = f"{container_name}/{podcast_title}"
            logging.info(f"folder_path: {folder_path}")
            blob_path = f"{folder_path}/{episode_title}.mp3"
            logging.info(f"blob_path: {blob_path}")

            # Download the MP3 file
            local_file_path = os.path.join('/tmp', f"{episode_title}.mp3")
            logging.info(f"local_file_path: {local_file_path}")
            response = requests.get(rss_url)
            logging.info("URL downloaded")
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            logging.info("MP3 file written locally")

            # Upload the MP3 file to Azure Blob Storage
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            logging.info(f"blob_client for uploading: {blob_client}")
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logging.info("blob_client uploaded to blob storage")
            
            # Clean up the local file
            os.remove(local_file_path)
            logging.info("file removed locally")

            # Update SQL database
            update_query = text(f"""
            UPDATE rss_schema.rss_feed SET download_flag_azure = 'Y', download_dt_azure = GETDATE() WHERE id = {episode[0]};
            """)
            connection.execute(update_query, {'rss_url': rss_url})
            logging.info("query updated")

    logging.info("Process completed successfully.")











# ####################################################################################################################################################################################################################      
 












@app.timer_trigger(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def reading_in_rss_and_writing_to_sql(myTimer: func.TimerRequest) -> None:

    print("reading_in_rss_and_writing_to_sql Function started...")

    # Azure Key Vault configuration
    credential = DefaultAzureCredential()
    key_vault_name = os.environ["MyKeyVault"]
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_uri, credential=credential)

    # Retrieve secrets from Azure Key Vault
    storage_account_name = client.get_secret("storageAccountName").value
    storage_account_key = client.get_secret("storageAccountKey").value
    container_name = "xml"
    sql_server_name = client.get_secret("SQLServerName").value
    database_name = client.get_secret("DBName").value
    sql_username = client.get_secret("SQLUserName").value
    sql_password = client.get_secret("SQLPass").value
    
    # Construct the SQLAlchemy connection string
    connection_string = f"mssql+pymssql://{sql_username}:{sql_password}@{sql_server_name}/{database_name}"
    engine = create_engine(connection_string)

    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=storage_account_key)
    container_client = blob_service_client.get_container_client(container_name)
    logging.info(f"container_client:{container_client}")

    def insert_rss_item(title, description, pub_date, enclosure_url, podcast_title, language):
        title = title.replace("'", "''")
        description = description.replace("'", "''")
        podcast_title = podcast_title.replace("'", "''")

        try:
            with engine.begin() as conn:
                # Check if the item already exists
                check_query = text("SELECT 1 FROM rss_schema.rss_feed WHERE link = :enclosure_url")
                result = conn.execute(check_query, {'enclosure_url': enclosure_url}).fetchone()
                
                # If the item doesn't exist, insert it
                if result is None:
                    insert_query = text("""
                        INSERT INTO rss_schema.rss_feed (title, description, pubDate, link, parse_dt, download_flag_azure, podcast_title, language)
                        VALUES (:title, :description, :pub_date, :enclosure_url, GETDATE(), 'N', :podcast_title, :language)
                    """)
                    conn.execute(insert_query, {
                        'title': title,
                        'description': description,
                        'pub_date': pub_date,
                        'enclosure_url': enclosure_url,
                        'podcast_title': podcast_title,
                        'language': language
                    })
                    logging.info(f"Item inserted: {title}")
        except Exception as e:
            logging.error(f"Failed to insert item: {title}. Error: {str(e)}")
    

    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob)
        blob_content = blob_client.download_blob().readall()
        local_path = f"/tmp/{blob.name}"  # Correcting the path to use /tmp directory

        # Write blob content to a local file
        with open(local_path, 'wb') as file:
            file.write(blob_content)
            #logging.info(f"Successfully written the blob_content.")


        # Load XML file
        try:
            tree = ET.parse(local_path)
            root = tree.getroot()

            # Extract podcast title and language
            channel = root.find('.//channel')
            podcast_title = channel.find('title').text
            language = channel.find('language').text

            # Process each item in the RSS feed
            for item in channel.findall('item'):
                title = item.find('title').text
                description = item.find('description').text
                pub_date = parser.parse(item.find('pubDate').text)
                enclosure_url = item.find('enclosure').get('url')
                
                insert_rss_item(title, description, pub_date, enclosure_url, podcast_title, language)

            # Delete the local file after processing
            os.remove(local_path)

        except Exception as e:
            print(f"Failed to process XML file: {local_path}. Error: {e}")

    print("Function completed for all files in the container.")