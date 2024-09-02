import logging
import os

import pandas as pd
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


def insert_data(db, file, file_name):
    collection = db.data
    data = pd.read_csv("eldenringScrap/" + file)
    print(data.head())

    collection.insert_one({"name": "Sample Data", "value": 123})


if __name__ == "__main__":
    logger = logging.getLogger("DATA-INSERTER")
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    try:
        logger.info("Connecting to MongoDB.")
        mongodb = connect_to_mongo()
        logger.info("Connected to MongoDB.")

    except Exception as e:
        logger.error(f"Could not connect to MongoDB due to error: {e}")
        exit(0)

    try:
        for file in os.listdir("eldenringScrap"):
            file_name = file.split(".")[0]
            logger.info(f"Adding {file_name} data to MongoDB.")
            insert_data(mongodb, file, file_name)
            logger.info(f"Added {file_name} data to MongoDB.")
    except Exception as e:
        logger.error(f"Could not add data to MongoDB due to error: {e}")
        exit(0)
