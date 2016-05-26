import datetime
import os
import pymysql
import configparser
import sqlalchemy
from filter_japanese_text import filter_japanese_text

# Logging ver. 2016-05-20
from logging import handlers
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.handlers.RotatingFileHandler('log.log', maxBytes=1000000, backupCount=3)  # file handler
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()  # console handler
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
logger.info('Initializing.')


# Database configuration
conf = configparser.ConfigParser()
conf.read('../config.cfg')
REMOTE_DB = {
    "host": conf.get('RDS', 'host'),
    "user": conf.get('RDS', 'user'),
    "passwd": conf.get('RDS', 'passwd'),
    "db_name": conf.get('RDS', 'db_name'),
}
ENGINE_CONF = "mysql+pymysql://" + REMOTE_DB["user"] + ":" + REMOTE_DB["passwd"] + "@" + REMOTE_DB["host"] + "/" + REMOTE_DB["db_name"] + "?charset=utf8mb4"


# topic modeling
### Retrieval parameters for testing
# timestart = "2013-07-31 00:00:00"
# timestart_text = "20130731"
# timeend = "2013-07-31 23:59:59"
# timeend_text = "20130731"
# # Area of interest
# # aoi = [122.933198,24.045416,153.986939,45.522785] # Japan
# aoi = [139.75, 35.67, 139.77, 35.69]  # Greater Tokyo Area

## Retrieval parameters
TIMESTART = "2013-07-01 00:00:00"
TIMESTART_TEXT = "20130701"
TIMEEND = "2013-07-31 23:59:59"
TIMEEND_TEXT = "20130731"
# Area of interest
# aoi = [122.933198,24.045416,153.986939,45.522785] # Japan
AOI = [138.72, 34.9, 140.87, 36.28]  # Greater Tokyo Area

UNIT_TEMPORAL = 60  # in minutes
UNIT_SPATIAL = 0.01  # in degrees

NUM_TOPIC = 50

EXPERIMENT_DIR = "/Users/koitaroh/Documents/Data/Experiments/"
DATA_DIR = "/Users/koitaroh/Documents/Data/GPS/2013/"
# MODEL_NAME = "ClassifiedTweet_20130731_20130731"
MALLET_FILE = '/Users/koitaroh/Documents/GitHub/Workspace/src/mallet-2.0.7/bin/mallet'
STOPLIST_FILE = "/Users/koitaroh/Documents/Data/Model/stoplist_jp.txt"
TWEET_COUNTER_FILE = '/Users/koitaroh/Documents/Data/Tweet/counter_20130731_20130731.pkl'
now = datetime.datetime.now()
MODELS_DIR = EXPERIMENT_DIR + "Experiment_" + now.strftime("%Y%m%d_%H%M%S")
os.makedirs(MODELS_DIR)
MODEL_NAME = "ClassifiedTweet_" + TIMESTART_TEXT + '_' + TIMEEND_TEXT
TEXTS_DIR = os.path.join(MODELS_DIR, MODEL_NAME)
STOPLIST = set(line.strip() for line in open(STOPLIST_FILE))
DOCNAMES = os.path.join(MODELS_DIR, MODEL_NAME+"_docnames.csv")
TWEET_DF_NAME = os.path.join(MODELS_DIR, MODEL_NAME+".csv")
tweet_counter_file = os.path.join(MODELS_DIR + "/tweet_counter.pkl")
LSI_counter_file_1 = os.path.join(MODELS_DIR, MODEL_NAME+"_lsi_1.pkl")
LSI_counter_file_2 = os.path.join(MODELS_DIR, MODEL_NAME+"_lsi_2.pkl")
LDA_counter_file_1 = os.path.join(MODELS_DIR, MODEL_NAME+"_lda.pkl")
LDA_counter_file_2 = os.path.join(MODELS_DIR, MODEL_NAME+"_lda_2.pkl")
HDP_counter_file_1 = os.path.join(MODELS_DIR, MODEL_NAME+"_hdp.pkl")
HDP_counter_file_2 = os.path.join(MODELS_DIR, MODEL_NAME+"_hdp_2.pkl")
OUTDIR = os.path.join(MODELS_DIR, MODEL_NAME+"/")