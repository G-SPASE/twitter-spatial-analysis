# twitter-spatial-analysis
=========

Collect tweets using Twitter API.
Run basic conversion, slicing, and spatial analysis

### src

| file name     | Description                    |
| ------------- | ------------------------------ |
| slice.py | slice tweets with given AOI and time |
| spatial-analysis.py | produce spatial metrics (count, distribution) |
| (estimate-nationality.py) | estimate nationality of users |

### Requirements:
* Python 3.4 or later
* PyMySQL
* MySQL
* config.cfg

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