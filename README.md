# azure-automated-file-backup
Backup and Sync a local folder on your computer automatically to cloud container storage and share with multiple clients or computers.

Requires: Azure Account, CosmosDB URI, key, and Storage Container String. All must be created before running the setup script. Additionally, Splunk HEC needs to be configured with: a custom index, HEC created, and the key placed in log.py. 


Currently Pushes log events to Splunk HTTP Event Collector. Settings must be changed in log.py for the moment. 
