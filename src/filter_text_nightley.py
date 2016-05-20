# filter_text_nightley.py
# Last Update: 2015-11-07
# @author: Satoshi Miyazawa
# koitaroh@gmail.com
# Specify a table on a database. Apply filter_japanese_text.py on corresponding column.

import csv
import MeCab
import sqlalchemy
from filter_japanese_text import text_filter, mecab_parse, dict_to_space_text_words
from datetime import datetime, date, time

# MECAB_MODE = 'mecabrc'
# PARSE_TEXT_ENCODING = 'utf-8'
#
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

# def text_filter(text):
#     # "RT @user:"を削除
#     if "RT " in text and ":" in text:
#         text = text.split(":", 1)[1]
#         # "@user"を削除
#     #     if "@" in text and " " in text:
#     if text[0] == "@":
#         text = text.split(" ", text.count("@"))[-1]
#     # "#tag"を削除
#     if "#" in text:
#         text = text.split("#", 1)[0]
#     # "URL"を削除
#     if "http" in text:
#         text = text.split("http", 1)[0]
#     # delete @ and following text (for Foursquare)
#     if "(@" in text:
#         text = text.split("(@", 1)[0]
#     # delete @ and following text (for other service)
#     if "@" in text:
#         text = text.split("@", 1)[0]
#     text = text.replace('\n','') # Get rid of return
#     text = text.replace('\r','') # Get rid of return
#     text = text.replace("\"",' ') # Get rid of return
#     text = text.replace("\'",' ') # Get rid of return
#     text = text.replace("\\",' ') # Get rid of return
#     text = text.rstrip()
#     return text
#
# def mecab_parse(text):
#     tagger = MeCab.Tagger(MECAB_MODE)
#     tagger.parse('')
#     node = tagger.parseToNode(text)
#     words = []
#     nouns = []
#     verbs = []
#     adjs = []
#     while node:
#         pos = node.feature.split(",")[0]
#         word = node.surface
#         type(word)
#         if pos == "名詞":
#             nouns.append(word)
#         elif pos == "動詞":
#             lemma = node.feature.split(",")[6]
#             # verbs.append(word)
#             verbs.append(lemma)
#         elif pos == "形容詞":
#             lemma = node.feature.split(",")[6]
#             # adjs.append(word)
#             adjs.append(lemma)
#         words.append(word)
#         node = node.next
#     parsed_words_dict = {
#         "all": words[1:-1],
#         "nouns": nouns,
#         "verbs": verbs,
#         "adjs": adjs
#     }
#     return parsed_words_dict

# For data from Nightley, which has text column at 4.
def prepare_tweet_nightley(inname, outname):
    lines = []
    with open(inname,'r', encoding="utf-8") as f:
        for line in f.readlines():
            lines.append(line[:-1])

    with open(outname,'w', newline='', encoding="utf-8") as correct:
        writer = csv.writer(correct)
        with open(inname) as mycsv:
            reader = csv.reader((line.replace('\0','') for line in mycsv))
            for row in reader:
                if row[0] not in lines:
                    if "I'm at" not in row[4]:
                        text = filter_text.text_filter(row[4])
                        words_dict = filter_text.mecab_parse(text)
                        words = filter_text.dict_to_space_text_words(words_dict)
                        # words = " ".join(words_dict['all'])
                        writer.writerow([row[0], row[1], row[2], row[3], words.replace(',', ' ')])

def prepare_tweet_nightley_db(dbname, in_table):
    engine = sqlalchemy.create_engine("mysql+pymysql://root:csisc@localhost/twitter?charset=utf8mb4", echo=False)
    conn = engine.connect()
    conn
    metadata = sqlalchemy.MetaData(engine)
    table = sqlalchemy.Table(in_table, metadata, autoload=True, autoload_with=engine)
    s = sqlalchemy.select([table.c.id_str, table.c.text]).where(table.c.text != None)
    result = conn.execute(s)
    for row in result.fetchall():
        id_str = row['id_str']
        text = row['text']
        print(text)
        text = text_filter(text)
        words_dict = mecab_parse(text)
        words = dict_to_space_text_words(words_dict)
        print(words)
        stmt = table.update().where(table.c.id_str == id_str).values(words=words)
        conn.execute(stmt)
    result.close()


    return True

if __name__ == '__main__':
    logger.debug('initializing.')
    # inname = '/Users/koitaroh/Documents/Data/Tweet/Tweet_Nightley_20150317_20150331.csv'
    # outname = '/Users/koitaroh/Documents/Data/Tweet/Tweet_Nightley_20150317_20150331_3.csv'
    # prepare_tweet_nightley(inname, outname)
    test1 = "帰るよー (@ 渋谷駅 (Shibuya Sta.) in 渋谷区, 東京都) https://t.co/UwXP9Gr0wJ check this out (http://t.co/nYHbleBtBT)"
    test2 = "終電変わらず(｀・ω・´)ゞ @ 川崎駅にタッチ！ http://t.co/DJFKEEUW3n"
    test3 = "月ザンギョ100とか200とかそら死ぬわな、と実感しつつある。 (@ ノロワレハウス II in 杉並区, 東京都) https://t.co/BkcI7uNigi"
    prepare_tweet_nightley_db("twitter", "social_activity_201307")
