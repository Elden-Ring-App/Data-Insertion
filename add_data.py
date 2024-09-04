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
    host = os.getenv("HOST")

    client = pymongo.MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")
    db = client.data

    return db


def insert_data(db, file, collection_name):

    collection = db[collection_name]

    try:
        data = pd.read_csv("eldenringScrap/" + file)

    except Exception as e:
        return logging.error(f"Could not get {collection_name} data due to error:", e)

    try:
        data = data.to_dict(orient='records')
        collection.insert_many(data)
        logger.info(f"Added {file_name} data to MongoDB.")

    except Exception as e:
        return logging.error(f"Could not insert {collection_name} data due to error:", e)


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
        for csv_file in os.listdir("eldenringScrap"):
            file_name = csv_file.split(".")[0]
            insert_data(mongodb, csv_file, file_name)

    except Exception as e:
        logger.error(f"Could not add data to MongoDB due to error: {e}")
        exit(0)
