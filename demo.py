# from PredictiveAnalytics.logger import logging
# from PredictiveAnalytics.exception import PredictiveAnalyticsException
# import sys


# logging.info("Welcome to our custom log")

# try:
#     a = 2/0
# except Exception as e:
#     raise PredictiveAnalyticsException(e, sys)

## Another test
# import os
# from dotenv import load_dotenv
# load_dotenv()


# mongo_db_url = os.getenv('MONGODB_URL_KEY')
# print(mongo_db_url)


## Another test
from PredictiveAnalytics.pipline.training_pipeline import TrainPipeline

obj = TrainPipeline()
obj.run_pipeline()


