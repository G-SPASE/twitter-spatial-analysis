# Last Update: 2016-05-23
# @author: Satoshi Miyazawa
# koitaroh@gmail.com
# Specify a table on a database. Apply filter_japanese_text.py on corresponding column.

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


def prepare_tweet_db(engine_conf, in_table):
    engine = sqlalchemy.create_engine(engine_conf, echo=False)
    conn = engine.connect()
    conn
    metadata = sqlalchemy.MetaData(engine)
    table = sqlalchemy.Table(in_table, metadata, autoload=True, autoload_with=engine)
    s = sqlalchemy.select([table.c.id, table.c.text]).where(table.c.text != None)
    result = conn.execute(s)
    for row in result.fetchall():
        id = row['id']
        text = row['text']
        words = filter_japanese_text(text)
        logger.debug(words)
        stmt = table.update().where(table.c.id == id).values(words=words)
        conn.execute(stmt)
    result.close()
    return True

if __name__ == '__main__':

    conf = configparser.ConfigParser()
    conf.read('../config.cfg')
    remote_db = {
        "host": conf.get('RDS', 'host'),
        "user": conf.get('RDS', 'user'),
        "passwd": conf.get('RDS', 'passwd'),
        "db_name": conf.get('RDS', 'db_name'),
    }
    engine_conf = "mysql+pymysql://" + remote_db["user"] + ":" + remote_db["passwd"] + "@" + remote_db["host"] + "/" + remote_db["db_name"] + "?charset=utf8mb4"
    test1 = "帰るよー (@ 渋谷駅 (Shibuya Sta.) in 渋谷区, 東京都) https://t.co/UwXP9Gr0wJ check this out (http://t.co/nYHbleBtBT)"
    test2 = "終電変わらず(｀・ω・´)ゞ @ 川崎駅にタッチ！ http://t.co/DJFKEEUW3n"
    test3 = "月ザンギョ100とか200とかそら死ぬわな、と実感しつつある。 (@ ノロワレハウス II in 杉並区, 東京都) https://t.co/BkcI7uNigi"
    prepare_tweet_db(engine_conf, "tweet_table_201501_test")
