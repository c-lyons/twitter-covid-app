import schedule
import time
import fetchTweets
import writeToMySQL
import cleanTweets

def main():
    """Full pipeline to fetch tweets from Twitter for given user query, cleans tweets and
    writes tweets to MySQL database."""
    tweets = fetchTweets.main()
    cleaned_tweets = cleanTweets.main(tweets)
    writeToMySQL.main(cleaned_tweets)
    print('Write to Database successful.')

schedule.every(10).minutes.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()
