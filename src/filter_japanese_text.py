# filter_text
# Last Update: 2015-11-06
# @author: Satoshi Miyazawa
# koitaroh@gmail.com

# Applies simple text filter, then apply MeCab filter to break down text into words (separated by space)

import MeCab

MECAB_MODE = 'mecabrc'
PARSE_TEXT_ENCODING = 'utf-8'

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

def text_filter(text):
    # "RT @user:"を削除
    try:
        if "RT " in text and ":" in text:
            text = text.split(":", 1)[1]
            if text[0] == " ":
                text = text.lstrip(' ')
        # "@user"を削除
        #     if "@" in text and " " in text:
        if text[0] == "@":
            text = text.split(" ", text.count("@"))[-1]
            if text[0] == " ":
                text = text.lstrip(' ')
        # Delete tweets start with "I'm at ".
        if text[0:7] == "I'm at ":
            text = ""
            return text
        # "#tag"を削除
        if "#" in text:
            text = text.split("#", 1)[0]
        # "URL"を削除
        if "http" in text:
            text = text.split("http", 1)[0]
        # delete @ and following text (for Foursquare or Swarm)
        if "(@" in text:
            text = text.split("(@", 1)[0]
        # delete @ and following text (for other service)
        if "@" in text:
            text = text.split("@", 1)[0]
        text = text.replace('\n','') # Get rid of return
        text = text.replace('\r','') # Get rid of return
        text = text.replace("\"",' ') # Get rid of return
        text = text.replace("\'",' ') # Get rid of return
        text = text.replace("\\",' ') # Get rid of return
        text = text.rstrip()
    except IndexError:
        pass
    return text

def mecab_parse(text):
    tagger = MeCab.Tagger(MECAB_MODE)
    tagger.parse('')
    node = tagger.parseToNode(text)
    words = []
    nouns = []
    verbs = []
    adjs = []
    while node:
        pos = node.feature.split(",")[0]
        word = node.surface
        type(word)
        if pos == "名詞":
            nouns.append(word)
        elif pos == "動詞":
            lemma = node.feature.split(",")[6]
            # verbs.append(word)
            verbs.append(lemma)
        elif pos == "形容詞":
            lemma = node.feature.split(",")[6]
            # adjs.append(word)
            adjs.append(lemma)
        words.append(word)
        node = node.next
    parsed_words_dict = {
        "all": words[1:-1],
        "nouns": nouns,
        "verbs": verbs,
        "adjs": adjs
    }
    return parsed_words_dict

def dict_to_space_text_words(words_dict):
    words = " ".join(words_dict['all'])
    words = words.replace(',', ' ')
    return words

if __name__ == '__main__':
    logger.debug('initializing.')
    # test_tweet = "RT @koitaroh: テストをします。"
    test_tweet = "RT @test: I'm at アミューズメントパークエルロフト - @l_loft in 茨木市, 大阪府 https://t.co/Bgm813Qamu"
    print(test_tweet)
    tweet_text = text_filter(test_tweet)
    words_dict = mecab_parse(tweet_text)
    words = dict_to_space_text_words(words_dict)
    print(words)