__author__ = 'koitaroh'

# convert_tweet_to_grid.py
# Last Update: 2015-12-19
# Author: Satoshi Miyazawa
# koitaroh@gmail.com
# Convert tweets to population density array p[t, x, y]

import csv
import configparser
import numpy
import pickle
import os
import datetime
import sqlalchemy
import settings as s

# Logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('spam.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


def define_spatiotemporal_unit(timestart, timeend, aoi, unit_temporal, unit_spatial):
    x_start = int(round(aoi[0]/unit_spatial))
    logger.debug(x_start)
    y_start = int(round(aoi[1]/unit_spatial))
    logger.debug(y_start)
    x_end = int(round(aoi[2]/unit_spatial))
    logger.debug(x_end)
    y_end = int(round(aoi[3]/unit_spatial))
    logger.debug(y_end)
    x_size = round(x_end - x_start)
    logger.debug(x_size)
    y_size = round(y_end - y_start)
    logger.debug(y_size)
    t_start = datetime.datetime.strptime(timestart, '%Y-%m-%d %H:%M:%S')
    t_end = datetime.datetime.strptime(timeend, '%Y-%m-%d %H:%M:%S')
    t_size = round((t_end - t_start) / datetime.timedelta(minutes=unit_temporal))
    logger.debug(t_size)
    # t_size = int(t_size)
    # logger.debug(t_size)
    return [t_size, x_size, y_size]

def define_spatiotemporal_unit_topic(timestart, timeend, aoi, unit_temporal, unit_spatial, numtopic):
    x_start = int(round(aoi[0]/unit_spatial))
    logger.debug(x_start)
    y_start = int(round(aoi[1]/unit_spatial))
    logger.debug(y_start)
    x_end = int(round(aoi[2]/unit_spatial))
    logger.debug(x_end)
    y_end = int(round(aoi[3]/unit_spatial))
    logger.debug(y_end)
    x_size = round(x_end - x_start)
    logger.debug(x_size)
    y_size = round(y_end - y_start)
    logger.debug(y_size)
    t_start = datetime.datetime.strptime(timestart, '%Y-%m-%d %H:%M:%S')
    t_end = datetime.datetime.strptime(timeend, '%Y-%m-%d %H:%M:%S')
    t_size = round((t_end - t_start) / datetime.timedelta(minutes=unit_temporal))
    logger.debug(t_size)
    # t_size = int(t_size)
    # logger.debug(t_size)
    return [t_size, x_size, y_size, numtopic]


def raw_txy_to_index_txy(timestart, timeend, aoi, unit_temporal, unit_spatial, t, x, y):
    timestart = datetime.datetime.strptime(timestart, '%Y-%m-%d %H:%M:%S')
    t = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
    t_index = int((t - timestart)/datetime.timedelta(minutes=unit_temporal))
    x_index = int((x - aoi[0])/unit_spatial)
    y_index = int((y - aoi[1])/unit_spatial)
    return [t_index, x_index, y_index]


def gps_to_grid(st_units, outfile):
    density = numpy.zeros(st_units)
    for root, dirs, files in os.walk(data_dir):
        filelist = files[0:30]
        logger.debug(filelist)
        for fn in filelist:
            # density = numpy.zeros(st_units)
            with open(root + os.sep + fn) as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    # print(row)
                    if len(row) == 6:
                        t = str(row[1])
                        y = float(row[2])
                        x = float(row[3])
                        if (aoi[0] <= x <= aoi[2]) and (aoi[1] <= y <= aoi[3]):
                            try:
                                index = raw_txy_to_index_txy(timestart, timeend, aoi, unit_temporal, unit_spatial, t, x, y)
                                # logger.debug(index)
                                density[index[0], index[1], index[2]] += 1
                            except IndexError as err:
                                logger.debug(t, err)
                                continue
                f = open(outfile, 'wb')
                logger.debug("saving.")
                pickle.dump(density, f)
                f.close()


def tweets_to_grid(engine_conf, table, st_units, outfile):
    density = numpy.zeros(st_units)
    engine = sqlalchemy.create_engine(engine_conf, echo=False)
    conn = engine.connect()
    metadata = sqlalchemy.MetaData(engine)
    table = sqlalchemy.Table(table, metadata, autoload=True, autoload_with=engine)
    s = sqlalchemy.select([table.c.id_str, table.c.tweeted_at, table.c.y, table.c.x, table.c.words])\
        .where(sqlalchemy.between(table.c.tweeted_at, timestart, timeend))\
        .where(sqlalchemy.between(table.c.x, aoi[0], aoi[2]))\
        .where(sqlalchemy.between(table.c.y, aoi[1], aoi[3]))
    result = conn.execute(s)
    for row in result.fetchall():
        id_str = row['id_str']
        words = row['words']
        t = str(row['tweeted_at'])
        x = float(row['x'])
        y = float(row['y'])
        logger.debug(id_str)
        try:
            index = raw_txy_to_index_txy(timestart, timeend, aoi, unit_temporal, unit_spatial, t, x, y)
            density[index[0], index[1], index[2]] += 1
        except IndexError as err:
            logger.debug(t, err)
            continue
    f = open(outfile, 'wb')
    logger.debug("saving.")
    pickle.dump(density, f)
    f.close()
    result.close()


if __name__ == '__main__':


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

    # # Retrieval parameters for testing
    # timestart = "2013-07-31 12:00:00"
    # timestart_text = "201307031"
    # timeend = "2013-07-31 23:59:59"
    # timeend_text = "20130731"
    # # Area of interest
    # # aoi = [122.933198,24.045416,153.986939,45.522785] # Japan
    # aoi = [139.71, 35.65, 139.77, 35.69]  # Greater Tokyo Area
    #
    # conf = configparser.ConfigParser()
    # conf.read('../config.cfg')
    # remote_db = {
    #     "host": conf.get('RDS', 'host'),
    #     "user": conf.get('RDS', 'user'),
    #     "passwd": conf.get('RDS', 'passwd'),
    #     "db_name": conf.get('RDS', 'db_name'),
    # }
    # engine_conf = "mysql+pymysql://" + remote_db["user"] + ":" + remote_db["passwd"] + "@" + remote_db["host"] + "/" + remote_db["db_name"] + "?charset=utf8mb4"
    # table = "social_activity_201307"

    # # Retrieval parameters
    # timestart = "2013-07-01 00:00:00"
    # timestart_text = "20130701"
    # timeend = "2013-07-31 23:59:59"
    # timeend_text = "20130731"
    # # Area of interest
    # # aoi = [122.933198,24.045416,153.986939,45.522785] # Japan
    # aoi = [138.72, 34.9, 140.87, 36.28]  # Greater Tokyo Area

    unit_temporal = 60  # in minutes
    unit_spatial = 0.01  # in degrees
    example_x = 138.750000
    example_y = 34.91000
    example_t = "2013-07-03 00:00:00"

    st_units = define_spatiotemporal_unit(timestart, timeend, aoi, unit_temporal, unit_spatial)
    logger.debug(st_units)
    # example_index = raw_txy_to_index_txy(timestart, timeend, aoi, unit_temporal, unit_spatial, example_t, example_x, example_y)
    # logger.debug(example_index)
    tweets_to_grid(engine_conf, table, st_units, tweet_counter_file)
    logger.debug("Done.")
