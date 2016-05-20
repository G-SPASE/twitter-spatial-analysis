# twitter-spatial-analysis
=========

Run basic filter, conversion, slicing, and spatial analysis

### src

| file name     | Description                    |
| ------------- | ------------------------------ |
| filter_japanese_text.py | Set of Mecab filters |
| filter_text_nightley.py | Filter |
| filter_text_twitter.py | Filter for typical tweet database |
| (slice.py) | slice tweets with given AOI and time |
| (spatial-analysis.py) | produce spatial metrics (count, distribution) |
| (estimate-nationality.py) | estimate nationality of users |

### Requirements:
* Python 3.4 or later
* PyMySQL
* MySQL
* config.cfg
* MeCab

You'll need a configuration file with twitter API authentication and MySQL connection information.
As specified on line 18, make a configuration file "config.cfg" in parent directory.
It's a text file. in it, write your twitter API keys and MySQL
connection file like below (replace * with your keys).

```
[twitter]
consumer_key = ****
consumer_secret = ****
access_token_key = ****
access_token_secret = ****

[local_db]
host = ****
user = ****
passwd = ****
db_name = ****
```

### Workflow:
1. Run filter_text_twitter.py
This will apply filter for word segmentation.