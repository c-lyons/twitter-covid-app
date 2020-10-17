import requests
import json
import pandas as pd
from requests.auth import HTTPBasicAuth


def getCredentials(credentialsFile='credentials.json'):
    '''Reads credentials file stored locally and returns required key and secrets.'''
    try:
        with open(credentialsFile) as credentials:
            data = json.load(credentials)
            apiKey = data['api_key']
            apiSecret = data['api_secret']

        return apiKey, apiSecret
    except:
        raise RuntimeError('Error reading credentials file. Please check file contents.')

def getToken(apiKey, apiSecret, base_url='https://api.twitter.com/', auth_url='oauth2/token'):

    auth_headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
    auth_data = {'grant_type':'client_credentials'}

    auth_resp = requests.post(base_url+auth_url, headers=auth_headers, data=auth_data, auth=HTTPBasicAuth(apiKey, apiSecret))
    try:
        return auth_resp.json()['access_token']
    except:
        raise RuntimeError('Could not get token using provided API key and secret. Please check credentials provided.')

def getSearchResults(query, access_token, base_url='https://api.twitter.com/',search_url ='{}2/tweets/search/recent?query={}'):

    search_url = search_url.format(base_url, query)
    search_headers = {'Authorization': 'Bearer {}'.format(access_token)}
    search_params = {'tweet.fields':'id,created_at,lang'}

    search_resp = requests.get(search_url, headers=search_headers, params=search_params)

    return search_resp.json()

def main(query='(covid OR (covid 19) OR covid19 OR (covid-19) OR coronavirus) lang:en -is:retweet'):
    """Returns dataframe for first 10 tweets returns for user query.
    Query can use OR operators, white space denotes AND. Use brackets and OR to include multiple queries.
    E.g. "(query 1) OR (query 2)"
    Refer to Twitter API docs for more info.
    Query can contain flags:
        lang:<language code>  - returns tweets in specified language only e.g. lang:en
        -is:retweet           - returns tweets only (not retweets)"""

    apiKey, apiSecret = getCredentials()
    access_token = getToken(apiKey, apiSecret)
    results = getSearchResults(query, access_token)
    tweets = pd.DataFrame(results['data'])
    tweets.to_csv('cached_data/results.csv', header=True, index=False)
    return tweets

if __name__ == "__main__":
    main()
