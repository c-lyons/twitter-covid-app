import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
import nltk
from PIL import Image    # to import the image
from wordcloud import WordCloud
from wordcloud import ImageColorGenerator
from nltk.corpus import stopwords


def listWords(tweets):
    """returns list of all words in all tweets for input into word cloud generator"""
    drop_words = ['covid', 'coronavirus', 'covid19', 'amp']
    word_list = [word for tweet in tweets.text for word in tweet.split()]
    word_list_cleaned = [word for word in word_list if word.lower() not in drop_words]
    return word_list_cleaned

def createMask(mask_dir='masks', image_name='mask.png'):
    '''Creates mask using vectorized image with white background to create wordclouds'''
    mask_path = mask_dir+'/'+image_name  # path to mask
    mask = np.array(Image.open(mask_path))
    mask_colors = ImageColorGenerator(mask)
    return mask, mask_colors

def loadStopwords():
    '''Loads stopwords from nltk lib'''
    # loading stop words
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))
    return stop_words

def createCloud(word_list, mask, mask_colors, stopwords, font_path='fonts/KGDoYouLoveMe.ttf', max_words=500, max_font_size=150, background_color='white', prefer_horizontal =0.7, seed=42):
    '''Creates wordcloud from list of words generated from selected tweets.'''

    word_cloud = WordCloud(max_words=max_words,
                        max_font_size=max_font_size,
                        background_color='white',
                        prefer_horizontal = prefer_horizontal,
                        mask=mask,
                        font_path=font_path,
                        random_state=seed,
                        width=mask.shape[1],
                        height=mask.shape[0],
                        color_func=mask_colors,
                        stopwords=stopwords).generate(' '.join(word_list))
    return word_cloud

def writeCloud(wordcloud, default_dir='cached_data', file_name='latest_cloud.png'):
    '''Writes generated wordcloud to file.'''
    wordcloud.to_file('latest_cloud.png')
    return print('Wordcloud saved successfully in {} directory.'.format(default_dir))

def plotCloud(wordcloud, figsize=(50,50), interpolation="bilinear"):
    '''Plots wordcloud to screen.'''
    plt.figure(figsize=figsize)
    plt.imshow(wordcloud, interpolation=interpolation)
    plt.axis("off")
    plt.margins(x=0, y=0)
    plt.show()

def main():
    '''Uses provided tweet dataframe to extract list of words and create a wordcloud using user selected image.'''
    tweets = pd.read_csv('cached_data/cleaned_tweets.csv')
    word_list = listWords(tweets)
    mask, mask_colors = createMask()
    stop_words = loadStopwords()
    word_cloud = createCloud(word_list, mask, mask_colors, stop_words)
    writeCloud(word_cloud)
    plotCloud(word_cloud)

if __name__ == "__main__":
    main()
