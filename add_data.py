import logging
import os
import time

import pymongo
from dotenv import load_dotenv


def connect_to_mongo():
    load_dotenv()

    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    port = int(os.getenv("PORT"))

    client = pymongo.MongoClient(f"mongodb://{username}:{password}@mongo:{port}/")
    db = client.test_db

    return db


def insert_data(db):
    collection = db.test_collection
    collection.insert_one({"name": "Sample Data", "value": 123})


if __name__ == "__main__":
    logger = logging.getLogger("Logger")
    try:
        logger.log(logging.INFO, "Connecting to MongoDB!")
        mongodb = connect_to_mongo()
        logger.log(logging.INFO, "Connected to MongoDB!")

    except Exception as e:
        logger.log(logging.ERROR, f"Could not connect to MongoDB due to error: {e}")
        exit(1)

    try:
        logger.log(logging.INFO, "Adding data to MongoDB")
        insert_data(mongodb)
        logger.log(logging.INFO, "Added data to MongoDB")
    except Exception as e:
        logger.log(logging.ERROR, f"Could not add data to MongoDB due to error: {e}")
        exit(1)
