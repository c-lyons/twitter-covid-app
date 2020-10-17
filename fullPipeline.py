import fetchTweets
import writeToMySQL
import cleanTweets

def main():
    """Full pipeline to fetch tweets from Twitter for given user query, cleans tweets and
    writes tweets to MySQL database."""
    tweets = fetchTweets.main()
    cleaned_tweets = cleanTweets.main(tweets)
    writeToMySQL.main(cleaned_tweets)

if __name__ == "__main__":
    main()
