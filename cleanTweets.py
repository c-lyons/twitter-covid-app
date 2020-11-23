import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import nltk
from datetime import datetime
# from nltk.corpus import stopwords

# # loading stop words
# nltk.download('stopwords')
# stopwords = set(stopwords.words('english'))

def get_ats(tweets):
    """extracts who is @'ed in a tweet (if any)'"""
    tweets['at_who'] = tweets.text.map(lambda x: str([word for word in x.split() if word[0] == '@']))  # converting list to string for MySQL import handling
    tweets['text'] = tweets.text.map(lambda x: [word for word in x.split() if word[0] != '@'])
    tweets['text'] = tweets.text.map(lambda x: ' '.join(x))
    return tweets

def clean_links(tweets):
    """removes links from tweets if https found in word in tweet"""
    tweets['text'] = tweets.text.map(lambda x: [word for word in x.split() if 'https' not in word])
    tweets['text'] = tweets.text.map(lambda x: ' '.join(x))
    return tweets

def clean_punc(tweets):
    """removes apostrophes, hyphens and full stops and replaces with no whitespace"""
    tweets['text'] = tweets.text.map(lambda x: re.sub('(?<=[a-zA-Z])’(?=[a-zA-Z])', '', x))
    tweets['text'] = tweets.text.map(lambda x: re.sub('(?<=[a-zA-Z])\'(?=[a-zA-Z])', '', x))
    tweets['text'] = tweets.text.map(lambda x: re.sub('(?<=[a-zA-Z])-(?=[a-zA-Z])', '', x))
    tweets['text'] = tweets.text.map(lambda x: re.sub('(?<=[a-zA-Z])\.(?=[a-zA-Z])', '', x))
    return tweets

def clean_special_chars(tweets):
    """removes all special chars and numbers and replaces with whitespace"""
    tweets['text'] = tweets.text.map(lambda x: re.sub('[^A-Za-z]+', ' ', x))
    return tweets

def get_datetime(tweets):
    '''Gets MYSQL formatted string for DATETIME format. Converts to datetime then extracts string.'''
    tweets['created_at'] = tweets.created_at.map(lambda x: pd.to_datetime(x))  # converting to datetime
    tweets['created_at'] = tweets.created_at.map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))  # extracting str for MySQL format
    return tweets

def tweets_clean(tweets):
    tweets = get_datetime(tweets)  # getting datetime
    tweets = clean_links(tweets)  # cleaning links from text
    tweets = get_ats(tweets)  # clean/ get @'s' from text
    tweets = clean_punc(tweets)  # removing hyphens/ apost and rejoining
    tweets = clean_special_chars(tweets)  # removing special chars from text
    tweets.rename({'id':'tweet_id'},axis=1, inplace=True)
    tweets.tweet_id = tweets.tweet_id.apply(pd.to_numeric)  # convert tweet_id to int from str
    return tweets

# def get_key_words(tweets, keyWord):
#     """Gets tweets containing user specified word. Skips filtering if no word given."""
#     if keyWord == None:
#         return tweets
#     else:
#         tweets['text'] = tweets.text.map(lambda x: [x if keyWord.lower() in x.lower().split() else 'NaN'])
#         tweets['text'] = tweets.keyWordTweets.map(lambda x: x[0])  # unpacking list
#         return tweets[tweets['text']!='NaN']

# def tweets_prep(tweets):
#     """Gets length of chars in tweet and number of words"""
#     tweet_temp = tweets.copy()
#     tweet_temp.loc[:, 'words'] = tweet_temp.text.map(lambda x: x.split())
#     tweet_temp.loc[:, 'tweet_len'] = tweet_temp.text.map(lambda x: len(x))
#     tweet_temp.loc[:, 'tweet_no_words'] = tweet_temp.words.map(lambda x: len(x))
#     return tweet_temp

# def listWords(tweets):
#     """returns list of all words in all tweets for input into word cloud generator"""
#     return [word for tweet in tweets.text for word in tweet.split()]

def main(tweets):
    """Removes special chars and links, converts date format for MySQL, gets twitter user @'ed in tweet (if any)"""

    print('Cleaning Tweets...')
    #tweets = pd.read_csv('results.csv')
    tweets.dropna(inplace=True)  # removing any NaN rows
    cleaned_tweets = tweets_clean(tweets)
    cleaned_tweets.to_csv('cached_data/cleaned_tweets.csv', header=True, index=False)
    print('Tweets cleaned.')
    return cleaned_tweets

if __name__ == "__main__":
    main()
