# azure-automated-file-backup
This script is designed to keep track of the files in the specified folder. It constantly checks the folder for any changes, such as new files being added, existing files being modified, or files being deleted. If it detects any changes, it automatically uploads the new or modified files to Azure Blob Storage, which is a cloud-based storage service. This means that the updated files will be accessible to the end user. It also keeps a record of all the files in a database called CosmosDB, which is a NoSQL database offered by Azure. The script also compares the state of the files from the last time the script was run and compares it with the current state of the files to detect changes, ensuring that the files in the cloud storage and the files in the local storage are always in sync.

Requires: Azure Account, CosmosDB URI, key, and Storage Container String. All must be created before running the setup script. Additionally, Splunk HEC needs to be configured with: a custom index, HEC created, and Currently Pushes log events to Splunk HTTP Event Collector. 

Note: Create a unique cosmosDB Container names for each user; example: file-tracker, file-tracker-2

To assist with configuration, I created a setup.py to automate the creation of config.ini; config-sample.ini is only for reference.


