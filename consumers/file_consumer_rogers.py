"""
file_consumer_case.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

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

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import pathlib
import time
import sys
from pymongo import MongoClient,errors

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Consume Messages from Live Data File
#####################################


def consume_messages_from_file(live_data_path, mongodb_uri, interval_secs, last_position):
    """
    Consume new messages from a file and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - live_data_path (pathlib.Path): Path to the live data file.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval in seconds to check for new messages.
    - last_position (int): Last read position in the file.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {mongodb_uri=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")

    logger.info("1. Initialize the database.")
    collection = init_db(mongodb_uri)

    logger.info("2. Set the last position to 0 to start at the beginning of the file.")
    last_position = 0

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                # Move to the last read position
                file.seek(last_position)
                for line in file:
                    # If we strip whitespace and there is content
                    if line.strip():

                        # Use json.loads to parse the stripped line
                        message = json.loads(line.strip())

                        # Call our process_message function
                        processed_message = process_message(message)

                        # If we have a processed message, insert it into the database
                        if processed_message:
                            insert_message(processed_message, collection)

                # Update the last position that's been read to the current file position
                last_position = file.tell()

                # Return the last position to be used in the next iteration
                return last_position

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)


#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.

    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        mongodb_uri: str = config.get_mongodb_uri()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)



    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, mongodb_uri, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")

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
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
