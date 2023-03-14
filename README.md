# azurevideoindexer
Using Python to interact with Azure Video Indexer

The purpose of this script is to connect to an existing Azure Video Indexer instance, and do the following :

* Connect to an existing Azure ADLS Storage Account
* Look for blobs of a specific file type ( based on file extension )
* Check if the file has already been uploaded into Video Indexer
* If not, upload into Video Indexer
