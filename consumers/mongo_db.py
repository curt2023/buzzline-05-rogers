""" mongo_db.py 

Has the following functions:
- init_db(config): Initialize the Mongo db database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import sys,os
from pymongo import MongoClient,errors

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(mongodb_uri):
    """Initializes the MongoDB connection."""
    try:
        client = MongoClient(mongodb_uri)
        db = client["mongo_buzz_database"]  # Replace with your database name
        collection = db["mongo_buzz_collection"]  # Replace with your collection name
        return collection
    except errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
        sys.exit(1)

def insert_message(message, collection):
    """Inserts a message into the MongoDB collection."""
    try:
        collection.insert_one(message)
        logger.info(f"Inserted message: {message}")
    except errors.PyMongoError as e:
        logger.error(f"Error inserting message: {e}")


#####################################
# Define Function to Insert a Processed Message into the Database
#####################################

def insert_message(message, collection):
    """Inserts a message into the MongoDB collection."""
    try:
        collection.insert_one(message)
        logger.info(f"Inserted message: {message}")
    except errors.PyMongoError as e:
        logger.error(f"Error inserting message: {e}")


#####################################
# Define Function to Delete a Message from the Database
#####################################




##############################################################
# Conditional Execution
#####################################


