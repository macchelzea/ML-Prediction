# from PredictiveAnalytics.logger import logging
# from PredictiveAnalytics.exception import PredictiveAnalyticsException
# import sys


# logging.info("Welcome to our custom log")

# try:
#     a = 2/0
# except Exception as e:
#     raise PredictiveAnalyticsException(e, sys)

## Another test
import os
from dotenv import load_dotenv
load_dotenv()


# mongo_db_url = os.getenv('MONGODB_URL_KEY')
# print(mongo_db_url)

# aws_accesskey = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secretkey = os.getenv('AWS_SECRET_ACCESS_KEY')
# print(aws_accesskey)
# print(aws_secretkey)



# # # after running
# # # export AWS_ACCESS_KEY_ID=<youraccesskey>
# aws_accesskey = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secretkey = os.getenv('AWS_SECRET_ACCESS_KEY')
# print(aws_accesskey)
# print(aws_secretkey)

## Another test
from PredictiveAnalytics.pipline.training_pipeline import TrainPipeline

obj = TrainPipeline()
obj.run_pipeline()


# import os
# from dotenv import load_dotenv
# load_dotenv()

# from PredictiveAnalytics.configuration.aws_connection import S3Client
# my_s3client = S3Client()
# # print(my_s3client.access_key_id)
# print(my_s3client.s3_resource)
# print(my_s3client.s3_client)
# # print(my_s3client.region_name)