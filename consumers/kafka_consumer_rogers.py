"""
kafka_consumer_case.py

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
import os
import pathlib
import sys

# import external modules
from kafka import KafkaConsumer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

from pymongo import MongoClient,errors


# Ensure the parent directory is in sys.path
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
#from consumers.db_sqlite_case import init_db, insert_message

mongo_uri = 'mongodb:localhost:27107/'
mongo_db_name = 'mongo_buzz_database'
mongo_collection_name = 'mongo_buzz collection'

def init_db(mongodb_uri):
    """Initializes the MongoDB connection."""
    try:
        client = MongoClient(mongodb_uri)
        db = client["product_launch_mongodb"]  # Replace with your database name
        collection = db["messages"]  # Replace with your collection name
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
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single message.

    For now, this function simply logs the message.
    You can extend it to perform other tasks, like counting words
    or storing data in a database.

    Args:
        message (str): The message to process.
    """
    logger.info(f"Processing message: {message}")



#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    mongodb_uri: str,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {mongodb_uri=}")
    logger.info(f"   {interval_secs=}")

  
    collection = init_db(mongodb_uri)
    logger.info("Step 1. Verify Kafka Services.")
    
    try:
        verify_services()
    except Exception as e:
       logger.error(f"ERROR: Kafka services verification failed: {e}")
       sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        # consumer is a KafkaConsumer
        # message is a kafka.consumer.fetcher.ConsumerRecord
        # message.value is a Python dictionary
        for message in consumer:
            processed_message = (message.value)
            collection.insert_one(processed_message)
            logger.info(f"inserted message{processed_message}")

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


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
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        mongodb_uri: str = config.get_mongodb_uri()
        mongodb_db: str = config.get_mongodb_db ()
        mongodb_collection: str = config.get_mongodb_collection()

        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)



    logger.info("STEP 3. Initialize a new database with an empty table.")
    


    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, mongodb_collection, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
