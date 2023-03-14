# Python script to browse and upload videos to Video Indexer instance

####################
# import libraries #
####################
from azure.storage.filedatalake import FileSystemClient

from azure.identity import DefaultAzureCredential, AzureCliCredential

import requests, json, uuid, datetime

from adal import AuthenticationContext

from urllib import parse

#######################
# important variables #
#######################
dataLakeConnStr = "BlobEndpoint="
dataLakeContainer = "input"

viAccessToken = ""

fileTypeList = [ '.mp4', '.wmv', '.mov', '.avi', '.mpeg', '.mpg', '.flv', '.mxf', '.gxf', '.asf', '.dvr', '.mkv' ]

viApiKey = ""
viLocation = "" # region where VI resource is created
viAccountId = "" # VI account id
viAccountName = "" # VI resource name
viSubscriptionId = "" # subscription id where VI exists
viTenantid = "" # tenant id where VI exists
viResourceGroup = "" # resource group where VI exists

# Enterprise App Registration within AAD
viAppClientId = ""
viAppClientSecret = ""

rootBlobUrl = "" # URL for Storage Account where files exist ( assumes Storage Account has Hierarchical Namespaces enabled )
blobSasToken = "" # SAS Token for above

rootApiUrl = "" # URL for Video Indexer API
rootArmUrl = "" # ARM Management URL for requesting ARM token
rootAuthUrl = "" # Auth URL for ARM request above

dataLakeConnStr = dataLakeConnStr + rootBlobUrl + ";SharedAccessSignature=" + blobSasToken

######################################
# function(s) for repeatable actions #
######################################
def fnGetArmToken( tenant, app, secret, armUrl, authUrl ) :

    print( "*********************" )
    print( "* Getting ARM Token *" )
    print( "*********************" )

    ctxtAuthUrl =  authUrl + tenant

    context = AuthenticationContext( ctxtAuthUrl )

    tokenOutput = context.acquire_token_with_client_credentials( armUrl, app, secret )

    armToken = tokenOutput[ "accessToken" ]

    return armToken

def fnGetAccessToken( location, accountName, subId, rgName, apiKey, apiUrl, armToken ) :

    print( "************************" )
    print( "* Getting Access Token *" )
    print( "************************" )

    requestHeaders = {
        # "Ocp-Apim-Subscription-Key" : apiKey ,
        "Authorization" : "Bearer " + armToken ,
        "content-type" : "application/json"
    }

    requestBody = {
        "permissionType" : "Contributor" ,
        "scope" : "Account"
    }

    requestTokenUrl = apiUrl + "subscriptions/" + subId + "/resourceGroups/" + rgName + "/providers/Microsoft.VideoIndexer/accounts/" + accountName + "/generateAccessToken?api-version=2022-08-01"

    tokenResponse = requests.post( requestTokenUrl , json = requestBody , headers = requestHeaders )

    # print( tokenResponse.json() )

    tokenJson = tokenResponse.json()

    accessToken = tokenJson[ "accessToken" ]

    # print( "Access Token: ", accessToken )

    return accessToken

def fnGetVideoList( location, accountId, apiKey, apiUrl, accessToken ) :

    videoBatchSize = 25
    videoSkipSize = 0

    videoList = []

    requestHeaders = {
        "Ocp-Apim-Subscription-Key" : apiKey
    }

    requestListUrl = apiUrl + location + "/Accounts/" + accountId + "/Videos?accessToken=" + accessToken + "&pageSize=" + str( videoBatchSize) + "&skip=" + str( videoSkipSize )

    response = requests.get( requestListUrl, headers = requestHeaders )

    # print( response.json() )

    jsonResponse = response.json()

    jsonVideos = jsonResponse[ "results" ]
    jsonPaging = jsonResponse[ "nextPage" ]

    endOfList = jsonPaging[ "done" ]

    # print( jsonVideos.keys() )

    while not endOfList :
        # print( jsonVideos ) 

        if len( jsonVideos ) > 0 :

            for result in jsonVideos :

                # print( "View each result :", result )

                videoList.append( result[ "name" ] )

            # increment counters and run again
            videoSkipSize += videoBatchSize

            requestListUrl = apiUrl + location + "/Accounts/" + accountId + "/Videos?accessToken=" + accessToken + "&pageSize=" + str( videoBatchSize) + "&skip=" + str( videoSkipSize )
            response = requests.get( requestListUrl, headers = requestHeaders )

            # print( response.json() )

            jsonResponse = response.json()

            jsonVideos = jsonResponse[ "results" ]
            jsonPaging = jsonResponse[ "nextPage" ]

            endOfList = jsonPaging[ "done" ]

            if endOfList :

                #  run one last time to capture final batch
                videoSkipSize += videoBatchSize

                requestListUrl = apiUrl + location + "/Accounts/" + accountId + "/Videos?accessToken=" + accessToken + "&pageSize=" + str( videoBatchSize) + "&skip=" + str( videoSkipSize )
                response = requests.get( requestListUrl, headers = requestHeaders )

                # print( response.json() )

                jsonResponse = response.json()

                jsonVideos = jsonResponse[ "results" ]
                jsonPaging = jsonResponse[ "nextPage" ]

                if len( jsonVideos ) > 0 :

                    for result in jsonVideos :

                        # print( "View each result :", result )

                        videoList.append( result[ "name" ] )

        else :

            # videoResults = []
            return

    print( "videoList :", videoList )

    return videoList

def fnUploadVideo( location, accountId, apiUrl, apiKey, video, videoUrl, accessToken ) :

    # generate new token, reset parameter values
    # print( "Generating new token for upload..." )
    # ( viAccessToken ) = fnGetAccessToken( viLocation, viAccountName, viSubscriptionId, viResourceGroup, viApiKey, rootArmUrl, armToken )

    requestHeaders = {
        "Ocp-Apim-Subscription-Key" : apiKey
    }

    requestBody = {
        "name" : video ,
        "videoUrl" : videoUrl ,
        "accessToken" : accessToken
    }

    parsedUrl = parse.quote( videoUrl )

    print( parsedUrl, " post replace" )

    uploadVideoUrl = apiUrl + location + "/Accounts/" + accountId + "/Videos?name=" + video + "&videoUrl=" + parsedUrl + "&accessToken=" + accessToken
    # uploadVideoUrl = apiUrl + location + "/Accounts/" + accountId + "/Videos"

    print( "Upload URL: ", uploadVideoUrl )

    # response = requests.post( uploadVideoUrl, json = requestBody, headers = requestHeaders )
    response = requests.post( url = uploadVideoUrl, headers = requestHeaders )

    print( response.json() )

#############
# main body #
#############
# create arm token
armToken = fnGetArmToken( viTenantid, viAppClientId, viAppClientSecret, rootArmUrl, rootAuthUrl )

# create vi access token
viAccessToken = fnGetAccessToken( viLocation, viAccountName, viSubscriptionId, viResourceGroup, viApiKey, rootArmUrl, armToken )

# get list of videos uploaded to VI
mediaList = fnGetVideoList( viLocation, viAccountId, viApiKey, rootApiUrl, viAccessToken )

# connect to Storage Account - assumes SA is ADLS V2
fileSysConn = FileSystemClient.from_connection_string( dataLakeConnStr, file_system_name = dataLakeContainer )

foundBlobs = fileSysConn.get_paths()

for blob in foundBlobs :

    if not blob.is_directory :

        blobSplit4 = blob.name[ -4 : ].lower()
        blobSplit5 = blob.name[ -5 : ].lower()

        if ( blobSplit4 in fileTypeList or blobSplit5 in fileTypeList ) :

            # print( "File Name: ", blob.name )

            blobName = blob.name

            blobSplit = blobName.split( "/" )

            mediaFile = blobSplit[ -1 ]
            print( "Checking for file :", mediaFile )

            # check if video has already been uploaded
            if mediaFile in mediaList :

                print( "Video already uploaded..." )

                # return

            else :

                # print( "Let's upload a video!" )

                externalVideoUrl = rootBlobUrl + dataLakeContainer + "/" + blobName + blobSasToken
                # externalVideoUrl = rootBlobUrl + dataLakeContainer + "/" + blobName

                print( "Video URL that doesn't currently exist in VI: ", externalVideoUrl, " - Video Name : ", mediaFile )

                fnUploadVideo( viLocation, viAccountId, rootApiUrl, viApiKey, mediaFile, externalVideoUrl, viAccessToken )

                input( "Enter something to continue : " )