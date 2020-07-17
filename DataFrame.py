import pyspark as ps
import json
import datetime
import numpy as np
from pyspark.sql.types import *

class TweetsDataFrame:
    
    def __init__(self, spark, sc):
        '''
            Takes a spark and spark context as parameters
            .tweets contains a dataframe of all of the tweets that have been read in.
        '''
        self.tweets = self.build_dataframe(spark, sc)
        
    def build_dataframe(self, spark, sc):
        '''
            Takes a spark and spark context object.
            Returns the tweets dataframe
        '''
        rdd = sc.textFile('data/french_tweets.json')
        filtered_rdd = rdd.map(self.convert_to_dict).filter(lambda x: False if x == None else True).map(self.parse_tweet).filter(lambda x: False if x == None else True)
        
        
        schema = StructType([
            StructField('user_id', StringType(), True),
            StructField('followers', IntegerType(), True),
            StructField('verified', BooleanType(), True),
            StructField('text', StringType(), True),
            StructField('year', IntegerType(), True),
            StructField('month', IntegerType(), True),
            StructField('day', IntegerType(), True),
            StructField('hour', IntegerType(), True),
            StructField('minute', IntegerType(), True),
            StructField('retweets', IntegerType(), True),
            StructField('favorites', IntegerType(), True),
            StructField('lang', StringType(), True),
            StructField('place_type', StringType(), True),
            StructField('place_name', StringType(), True),
            StructField('place_full_name', StringType(), True),
            StructField('country_code', StringType(), True),
            StructField('country', StringType(), True),
        ])
        
        filtered_df = spark.createDataFrame(filtered_rdd, schema)
        return filtered_df
        
    def convert_to_dict(self, row):
        try:
            return json.loads(row)
        except:
            return None
        
    def parse_tweet(self, tweet_dict):
        
        if 'created_at' not in tweet_dict.keys():
            return None
        create_date = datetime.datetime.strptime(tweet_dict['created_at'], '%a %b %d %H:%M:%S %z %Y')
        
        text = tweet_dict['text']
        user_id = int(tweet_dict['user']['id'])
        followers = int(tweet_dict['user']['followers_count'])
        verified = bool(tweet_dict['user']['verified'])
        
        if (tweet_dict['place'] is None) or ('place_type' not in tweet_dict['place'].keys()):
            return None
        place_type = tweet_dict['place']['place_type']
        
        place_name = tweet_dict['place']['name']
        place_full_name = tweet_dict['place']['full_name']
        country_code = tweet_dict['place']['country_code']
        country = tweet_dict['place']['country']
        retweets = int(tweet_dict['retweet_count'])
        favorites = int(tweet_dict['favorite_count'])
        lang = tweet_dict['lang']
        
        return (user_id, followers, verified, text,
                create_date.year, create_date.month,
                create_date.day, create_date.hour,
                create_date.minute, retweets, 
                favorites, lang, place_type, 
                place_name, place_full_name, country_code, country)

if __name__ == "__main__":
    pass