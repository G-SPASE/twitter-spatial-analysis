import csv, pymysql, configparser, time
import filter_japanese_text

conf = configparser.ConfigParser()
conf.read('../config.cfg')

local_db = {
    "host": conf.get('local_db', 'host'),
    "user": conf.get('local_db', 'user'),
    "passwd": conf.get('local_db', 'passwd'),
    "db_name": conf.get('local_db', 'db_name'),
}

rds_db = {
    "host": conf.get('RDS', 'host'),
    "user": conf.get('RDS', 'user'),
    "passwd": conf.get('RDS', 'passwd'),
    "db_name": conf.get('RDS', 'db_name'),
}


# Replace commas to spaces in a csv file
def replace(inname, outname):
    reader = csv.reader(open(inname, encoding="utf-8"))
    # reader = csv.reader((line.replace('\0','') for line in inname), delimiter=",")
    # writer = csv.writer(open('/Users/koitaroh/Documents/GitHub/GeoTweetCollector/data/tweet_table_20150317140247.csv', 'w', newline='', encoding="utf-8"))
    writer = csv.writer(open(outname, 'w', newline='', encoding="utf-8"))
    for row in reader:
        for x in row:
            x.replace('\0', '')
            x.replace('\x00', '')
            x.replace('\\', '')
        # reader = csv.reader(x.replace('\0', '') for x in mycsv)
        # data = csv.reader((line.replace('\0','') for line in inname), delimiter=",")
        writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9].replace(',', ' '), row[10].replace(',', ' '), row[11].replace(',', ' '), row[12].replace(',', ' ')])

# Execute sql like insert, update
def execute_sql(sql, db_info, is_commit = False):
    connection = pymysql.connect(host = db_info["host"],
                                 user = db_info["user"],
                                 passwd = db_info["passwd"],
                                 db = db_info["db_name"],
                                 charset = "utf8mb4",
                                 cursorclass=pymysql.cursors.SSDictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        if is_commit:
            connection.commit()
        cursor.close()
    finally:
        connection.close()

# Find commas in a column in a table
def find_comma_db(db_info, table):
    connection = pymysql.connect(host = db_info["host"],
                                 user = db_info["user"],
                                 passwd = db_info["passwd"],
                                 db = db_info["db_name"],
                                 charset = "utf8mb4",
                                 cursorclass=pymysql.cursors.SSCursor)
    sql = """
    SELECT tweet_id, words FROM %s WHERE words like "%%,%%";
    """ %(
        table
    )
    try:

        cursor = connection.cursor()
        t0 = time.time()
        results = cursor.execute(sql)
        t1 = time.time()

        keys = []
        for desc in cursor.description:
            keys.append(desc[0])
        for row in cursor:
            tweet_id = row[keys.index('tweet_id')]
            words = row[keys.index('words')]
            words = words.replace(',', ' ')
            words = words.replace('\n','')
            words = words.replace('\r','')
            words = words.replace("\"",'')
            words = words.replace("\'",'')
            words = words.replace("\\",'')
            print(tweet_id, words)
            # replace_comma_to_space_db(db_info, table, tweet_id, words)


        # rows = cursor.fetchmany(size=100)
        # # rows = cursor.fetchone()
        # while rows:
        #         for row in rows:
        #             tweet_id = row['tweet_id']
        #             words = row['words']
        #             words = words.replace(',', ' ')
        #             words = words.replace('\n','')
        #             words = words.replace('\r','')
        #             words = words.replace("\"",'')
        #             words = words.replace("\'",'')
        #             words = words.replace("\\",'')
        #             print(tweet_id, words)
        #             # replace_comma_to_space_db(db_info, table, tweet_id, words)
        #         rows = cursor.fetchmany(size=100)
        #         # rows = cursor.fetchone()
        cursor.close()
    finally:
        connection.close()

# Replace commas to spaces in a table
def replace_comma_to_space_db(db_info, table, tweet_id, words):
    sql = """
    UPDATE %s set words = '%s' WHERE tweet_id = %s;
    """ %(
        table,
        words,
        tweet_id
    )
    execute_sql(sql, db_info, is_commit = True)
    return True

if __name__ == '__main__':
    # input shoud not include blank or corrupted lines

    table_name = "tweet_table_201604_1"
    find_comma_db(rds_db, table_name)

