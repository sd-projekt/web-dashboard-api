from fastapi import FastAPI
import pymongo
from datetime import datetime, timedelta

USER = "user"
PASS = "pass"
HOST = "localhost"
PORT = "27017"

CONNECTION_STRING = "mongodb://" + USER + ":" + PASS + "@" + HOST + ":" + PORT + "/" 

# Function declarations
def connect_to_mongodb():
    return pymongo.MongoClient(CONNECTION_STRING)

def select_db(mc : pymongo.MongoClient, dbName):
    return mc[dbName]

def select_col(db, colName):
    return db[colName]

# Main function
app = FastAPI()

# FastAPI methods
@app.get("/data/{component}/{value}")
async def data_return(component, value, fromWhen : str = ""):
    databaseClient = connect_to_mongodb()
    
    # Select the appropriate component database based on the variable in the path
    componentDB = select_db(databaseClient, component)
    
    try:
        componentDB.validate_collection(value)
    except pymongo.errors.OperationFailure:
        return { "message": "ERROR: The specified component and/or the value do not exist.", "error": 1 }

    # Select the approriate value collection according to the variable in the path
    valueCol = select_col(componentDB, value)

    if (fromWhen == ""):
        # No time specified, return the newest element
        # Filter for the newest document, exclude id because not processable
        newestDocument = valueCol.find({},{ "_id": 0}).sort("timestamp", -1)[0]

        # Return the newest document as a result
        return newestDocument
    elif (fromWhen.endswith("h")):
        # Return the values of the last n hours
        numberOfHours = int(fromWhen[:-1])
        # Create the appropriate timestamp, n hours ago
        timestampNHoursAgo = (datetime.now() - timedelta(hours = numberOfHours)).isoformat()

        # Create an array of documents that are newer than the timestamp, exclude id again
        documentsNewerThanTimestamp = valueCol.find({ "timestamp" : { "$gt": timestampNHoursAgo}}, { "_id": 0})
        
        # Create a dictionary that contains the results
        resultDictionary = { "results": []}

        for document in documentsNewerThanTimestamp:
            resultDictionary["results"].append(document)

        return resultDictionary

    elif (fromWhen.endswith("m")):
        # Return the values of the last n minutes
        numberOfMinutes = int(fromWhen[:-1])
        # Create the appropriate timestamp, n minutes ago
        timestampNMinutesAgo = (datetime.now() - timedelta(minutes = numberOfMinutes)).isoformat()

        # Create an array of documents that are newer than the timestamp, exclude id again
        documentsNewerThanTimestamp = valueCol.find({ "timestamp" : { "$gt": timestampNMinutesAgo}}, { "_id": 0})
        
        # Create a dictionary that contains the results
        resultDictionary = { "results": []}

        for document in documentsNewerThanTimestamp:
            resultDictionary["results"].append(document)

        return resultDictionary
    
    else:
        # Parameter error
        return { "message": "ERROR: The query parameters provided are wrong/incomplete.", "error": 2 }