import sys
import os
import pymongo
import certifi
from dotenv import load_dotenv

from PredictiveAnalytics.exception import PredictiveAnalyticsException
from PredictiveAnalytics.logger import logging
from PredictiveAnalytics.constants import DATABASE_NAME, MONGODB_URL_KEY

load_dotenv()
ca = certifi.where()


class MongoDBClient:
    """
    Class Name: MongoDBClient
    Description: Creates a MongoDB client and exposes a connection to the database.
    Output: MongoDB database connection object
    On Failure: Raises PredictiveAnalyticsException
    """

    client = None

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                # Ensure key is a string for Pylance typing
                key: str = str(MONGODB_URL_KEY)

                # Typed-safe environment variable retrieval
                mongo_db_url = os.environ.get(key)

                if not mongo_db_url:
                    raise PredictiveAnalyticsException(
                        f"Environment variable '{key}' is not set.", sys
                    )

                MongoDBClient.client = pymongo.MongoClient(
                    mongo_db_url,
                    tlsCAFile=ca
                )

            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name

            logging.info("MongoDB connection successful")

        except Exception as e:
            raise PredictiveAnalyticsException(e, sys)
