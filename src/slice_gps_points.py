# -*- coding: utf-8 -*-
__author__ = 'koitaroh'

# slice_gps_points
# Last Update: 2016-06-10
# Author: Satoshi Miyazawa
# koitaroh@gmail.com

import csv
import configparser
import pickle
import os
import collections
import datetime
import configparser
import sqlalchemy
import numpy
import pandas
import geopy
from geopy.distance import vincenty
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

# Parameter
aoi = s.AOI
timestart = s.TIMESTART
timestart_text = s.TIMESTART
timeend = s.TIMEEND
timeend_text = s.TIMEEND_TEXT
unit_spatial = s.UNIT_SPATIAL_METER
unit_temporal = s.UNIT_TEMPORAL
neighborhood_spatial = s.NEIGHBORHOOD_SPATIAL
neighborhood_temporal = s.NEIGHBORHOOD_TEMPORAL
gps_dir = s.GPS_DIR
gps_counter_file = s.GPS_COUNTER_FILE


def define_spatiotemporal_unit(aoi, timestart, timeend, unit_spatial, unit_temporal):
    x1y1 = (aoi[1], aoi[0])
    x2y1 = (aoi[1], aoi[2])
    x1y2 = (aoi[3], aoi[0])
    x2y2 = (aoi[3], aoi[2])
    x_distance = geopy.distance.vincenty(x1y1, x2y1).meters
    y_distance = geopy.distance.vincenty(x1y1, x1y2).meters
    logger.debug("X distance: %s meters", x_distance)
    logger.debug("Y distance: %s meters", y_distance)
    x_size = int(x_distance // unit_spatial) + 1
    y_size = int(y_distance // unit_spatial) + 1
    logger.info("X size: %s", x_size)
    logger.info("Y size: %s", y_size)
    t_start = datetime.datetime.strptime(timestart, '%Y-%m-%d %H:%M:%S')
    t_end = datetime.datetime.strptime(timeend, '%Y-%m-%d %H:%M:%S')
    t_size = round((t_end - t_start) / datetime.timedelta(minutes=unit_temporal))
    logger.info("T size: %s", t_size)
    return [t_size, x_size, y_size]


def raw_txy_to_index_txy(aoi, timestart, timeend, unit_spatial, unit_temporal, t: str, x, y):
    x1y1 = (aoi[1], aoi[0])
    x2y1 = (aoi[1], x)
    x1y2 = (y, aoi[0])
    x2y2 = (y, x)
    x_distance = geopy.distance.vincenty(x1y1, x2y1).meters
    y_distance = geopy.distance.vincenty(x1y1, x1y2).meters
    # logger.debug("X distance: %s meters", x_distance)
    # logger.debug("Y distance: %s meters", y_distance)
    x_index = int(x_distance // unit_spatial) + 1
    y_index = int(y_distance // unit_spatial) + 1
    # logger.info("X size: %s", x_index)
    # logger.info("Y size: %s", y_index)
    timestart = datetime.datetime.strptime(timestart, '%Y-%m-%d %H:%M:%S')
    t = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
    t_index = int((t - timestart)/datetime.timedelta(minutes=unit_temporal))
    # x_index = int((x - aoi[0])/unit_spatial)
    # y_index = int((y - aoi[1])/unit_spatial)
    return [t_index, x_index, y_index]

def gps_to_grid(st_units, data_dir, outfile):
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
                                index = raw_txy_to_index_txy(aoi, timestart, timeend, unit_spatial, unit_temporal, t, x, y)
                                # logger.debug(index)
                                density[index[0], index[1], index[2]] += 1
                            except IndexError as err:
                                logger.debug(t, err)
                                continue
                f = open(outfile, 'wb')
                logger.debug("saving.")
                pickle.dump(density, f)
                f.close()


if __name__ == '__main__':
    # Setting sample parameter
    aoi = [138.72, 34.9, 140.87, 36.28]  # Greater Tokyo Area
    # aoi = [139.751014, 35.670163, 139.762781, 35.678529]  # Greater Tokyo Area
    timestart = "2013-07-03 00:00:00"
    timestart_text = "2013-07-03"
    timeend = "2013-07-04 23:59:59"
    timeend_text = "2013-07-03"
    unit_spatial = 1000
    unit_temporal = 60
    neighborhood_spatial = 14
    neighborhood_temporal = 0

    example_x = 138.750000
    example_y = 34.91000
    example_t = "2013-07-03 12:00:00"

    st_units = define_spatiotemporal_unit(aoi, timestart, timeend, unit_spatial, unit_temporal)
    logger.debug(st_units)
    # st_index = raw_txy_to_index_txy(aoi, timestart, timeend, unit_spatial, unit_temporal, example_t, example_x, example_y)
    # logger.debug(st_index)
    gps_to_grid(st_units, gps_dir, gps_counter_file)