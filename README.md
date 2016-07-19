# twitter-spatial-analysis
=========

Run basic filter, conversion, slicing, and spatial analysis on Twitter data.

### src

| file name     | Description                    |
| ------------- | ------------------------------ |
| filter_japanese_text.py | Set of Mecab filters |
| filter_text_nightley.py | Filter |
| filter_text_twitter.py | Filter for typical tweet database |
| convert_points_to_grid.py | Define spatiotemporal index for experiment. Slice tweets or gps to create density file |
| convert_tweet_to_gensim.py | Slice tweets and convert to text files for gensim |
| run_gensim_topicmodels.py | Run gensim topic modeling models to sliced tweets |
| settings.py | Model and experiment settings |


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

[remote_db]
host = ****
user = ****
passwd = ****
db_name = ****
```

### Workflow:
1. Check and adjust settings.py
Set model parameters, resource paths.

2. Run filter_text_twitter.py
This will apply filter for word segmentation.

3. Run run_gensim_topicmodels.py
This will run topic modeling