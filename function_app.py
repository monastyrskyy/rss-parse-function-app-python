import os
import logging
import pyodbc
import requests
import shutil
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient
import azure.functions as func

app = func.FunctionApp()

@app.schedule(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def rss_refresh_daily(myTimer: func.TimerRequest) -> None:

    logging.info('Starting rss_refresh_daily')

    # Setup Azure credential
    credential = DefaultAzureCredential()
    key_vault_name = os.environ["language-app-key-vault"]
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"

    # Create a secret client
    client = SecretClient(vault_url=key_vault_uri, credential=credential)

    try:
        # Fetch secrets from Azure Key Vault
        server_name = client.get_secret("SQLServerName").value
        database_name = client.get_secret("DBName").value
        username = client.get_secret("SQLUserName").value
        password = client.get_secret("SQLPass").value
        storage_account_name = client.get_secret("storageAccountName").value
        storage_account_key = client.get_secret("storageAccountKey").value
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}"

        logging.info("Fetched database connection details from Key Vault successfully.")
    except Exception as e:
        logging.error(f"Failed to fetch secrets from Key Vault. Error: {str(e)}")
        raise

    # Connect to SQL Database
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT podcast_name, rss_url FROM dbo.rss_urls")
        podcasts = cursor.fetchall()
        conn.close()

        logging.info("RSS URLs and podcast names fetched from SQL Database successfully.")
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
            blob_client = blob_service_client.get_blob_client(container="xml", blob=f"{safe_podcast_name}_with_python.xml")
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
    
    
    
    
    
    
    
    
    
    
    
    
####################################################################################################################################################################################################################      
    
    
    
    
    
    
    
    
    
    
    
    

@app.timer_trigger(schedule="0 2/20 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def mp3_download(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Second func, just straight up in line?')












####################################################################################################################################################################################################################      
 












@app.timer_trigger(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def reading_in_rss_and_writing_to_sql(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Another test.')