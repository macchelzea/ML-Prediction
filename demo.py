from PredictiveAnalytics.logger import logging
from PredictiveAnalytics.exception import PredictiveAnalyticsException
import sys


logging.info("Welcome to our custom log")

try:
    a = 2/0
except Exception as e:
    raise PredictiveAnalyticsException(e, sys)