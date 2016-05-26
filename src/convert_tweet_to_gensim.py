# convert_tweet_to_gensim.py
# Last Update: 2016-05-26
# Author: Satoshi Miyazawa
# koitaroh@gmail.com
#


import os
import collections
import datetime
import configparser
import pickle
import sqlalchemy
import numpy
import pandas
from convert_points_to_grid import define_spatiotemporal_unit, raw_txy_to_index_txy
import settings as s


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


def create_twitter_text_files(engine_conf, table, st_units, tweet_counter_file, aoi, timestart, timeend, tweet_df_name, outdir, unit_temporal, unit_spatial):
    logger.debug('Loading tweets to dataframe.')
    density = numpy.zeros(st_units)
    # Loading tweets to dataframe
    engine = sqlalchemy.create_engine(engine_conf, echo=False)
    sql = "SELECT * FROM %s where (tweeted_at between '%s' and '%s') and (x BETWEEN %s and %s) and (y BETWEEN %s and %s)" %(table, timestart, timeend, aoi[0], aoi[2], aoi[1], aoi[3])
    conn = engine.connect()
    tweet_df = pandas.read_sql_query(sql, engine)
    logger.debug('Applying indices.')
    # Applying indices
    tweet_df_time = tweet_df['tweeted_at']
    temp = pandas.DatetimeIndex(tweet_df['tweeted_at'])
    tweet_df['date'] = temp.date
    tweet_df['time'] = temp.time
    tweet_df['t_index'] = (tweet_df['date']).astype(str) + '-' + (tweet_df['time'].astype(str)).str[:2]
    tweet_df['x_index'] = (tweet_df['x']/0.01).astype(int)
    tweet_df['y_index'] = (tweet_df['y']/0.01).astype(int)
    logger.debug('Saving files.')
    # Save files
    tweet_df.to_csv(tweet_df_name, encoding='utf-8')
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    tweet_counter = collections.Counter()
    for index, row in tweet_df.iterrows():
        t_index = str(row['t_index'])
        x_index = str(row['x_index'])
        y_index = str(row['y_index'])
        words = str(row['words'])
        workfile = t_index + "_" + x_index + "_" + y_index + ".txt"
        tweet_counter[t_index + "_" + x_index + "_" + y_index] += 1
        f = open(outdir + workfile, 'a')
        f.write(words + "\n")
        f.close()

        t = str(row['tweeted_at'])
        x = float(row['x'])
        y = float(row['y'])
        try:
            index = raw_txy_to_index_txy(timestart, timeend, aoi, unit_temporal, unit_spatial, t, x, y)
            density[index[0], index[1], index[2]] += 1
        except IndexError as err:
            logger.debug(t, err)
            continue

    f = open(tweet_counter_file, 'wb')
    logger.debug("saving numpy array.")
    pickle.dump(density, f)
    f.close()

    # writer = csv.writer(open(tweet_counter_file, 'w'))
    # for key, value in tweet_counter.items():
    #     writer.writerow([key, value])


if __name__ == '__main__':
    now = datetime.datetime.now()
    logger.debug('Setting parameters.')


    engine_conf = s.ENGINE_CONF
    table = s.TABLE

    timestart = s.TIMESTART
    timestart_text = s.TIMESTART_TEXT
    timeend = s.TIMEEND
    timeend_text = s.TIMEEND_TEXT
    aoi = s.AOI
    unit_temporal = s.UNIT_TEMPORAL
    unit_spatial = s.UNIT_SPATIAL
    num_topic = s.NUM_TOPIC

    data_dir = s.DATA_DIR
    tweet_counter_file = s.TWEET_COUNTER_FILE

    MODELS_DIR = s.MODELS_DIR
    MODEL_NAME = s.MODEL_NAME
    MALLET_FILE = s.MALLET_FILE
    TEXTS_DIR = s.TEXTS_DIR
    STOPLIST = s.STOPLIST
    DOCNAMES = s.DOCNAMES
    LSI_counter_file = s.LSI_counter_file_1
    LDA_counter_file = s.LDA_counter_file_1
    HDP_counter_file = s.HDP_counter_file_1
    tweet_df_name = s.TWEET_DF_NAME
    outdir = s.OUTDIR


    # # Parameter setting
    # MODELS_DIR = "/Users/koitaroh/Documents/Data/Experiments/Model_" + now.strftime("%Y%m%d_%H%M%S")
    # os.makedirs(MODELS_DIR)
    # MODEL_NAME = "ClassifiedTweet_20130731_20130731"
    # MODEL_NAME = "ClassifiedTweet_" + timestart_text + '_' + timeend_text
    # MALLET_FILE = '/Users/koitaroh/Documents/GitHub/Workspace/src/mallet-2.0.7/bin/mallet'
    # TEXTS_DIR = os.path.join(MODELS_DIR, MODEL_NAME)
    # STOPLIST_FILE = "/Users/koitaroh/Documents/Data/Model/stoplist_jp.txt"
    # stoplist = set(line.strip() for line in open(STOPLIST_FILE))
    # DOCNAMES = os.path.join(MODELS_DIR, MODEL_NAME+"_docnames.csv")
    # LSI_counter_file = os.path.join(MODELS_DIR, MODEL_NAME+"_lsi.pkl")
    # LDA_counter_file = os.path.join(MODELS_DIR, MODEL_NAME+"_lda.pkl")
    # HDP_counter_file = os.path.join(MODELS_DIR, MODEL_NAME+"_hdp.pkl")
    # tweet_df_name = os.path.join(MODELS_DIR, MODEL_NAME+".csv")
    # outdir = os.path.join(MODELS_DIR, MODEL_NAME+"/")
    # # tweet_counter_file = '/Users/koitaroh/Documents/Data/Tweet/tweet_df_'+timestart_text+'_'+timeend_text+'_counter.csv'
    # tweet_counter_file = os.path.join(MODELS_DIR + "/tweet_counter.pkl")


    st_units = define_spatiotemporal_unit(timestart, timeend, aoi, unit_temporal, unit_spatial)
    logger.debug(st_units)
    create_twitter_text_files(engine_conf, table, st_units, tweet_counter_file, aoi, timestart, timeend, tweet_df_name, outdir, unit_temporal, unit_spatial)


