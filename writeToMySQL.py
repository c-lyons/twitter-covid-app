import mysql.connector
import csv
import json
import pandas as pd
from sqlalchemy import create_engine, exc, MetaData, Table, Column, Integer, String, DateTime, Text
import pymysql
from mysql.connector import errorcode

# def create_database(cursor, DB_NAME):
#     try:
#         cursor.execute(
#             "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
#     except mysql.connector.Error as err:
#         print("Failed creating database: {}".format(err))
#         exit(1)

# def drop_table(cursor, table_name):
#     try:
#         cursor.execute(
#             "DROP TABLE {}".format(table_name))
#         print('Table {} dropped succesfully.'.format(table_name))
#     except mysql.connector.Error as err:
#         print("Failed to drop table: {}".format(err))
#         exit(1)

def getCredentials(credentialsFile):
    '''Reads credentials file stored locally and returns required key and secrets.'''
    try:
        with open(credentialsFile) as credentials:
            data = json.load(credentials)
            db_user = data['db_user']
            db_password = data['db_password']
            db_host = data['db_host']
            db_name = data['db_name']
            db_table = data['db_table']
        print('Credentials found.')
        return db_user, db_password, db_host, db_name, db_table
    except:
        raise RuntimeError('Error reading credentials file. Please check file contents.')

def create_connection(db_user, db_password, db_host, db_name):
    """Creates connection using credentials provided in credentials.json"""
    try:
        cnx = mysql.connector.connect(user=db_user,
                                    password=db_password,
                                    host=db_host,
                                    database=db_name)
        cursor = cnx.cursor()
        print('Connection created successfully.')
        return cursor, cnx
    except:
        raise RuntimeError('Could not connect to database. Check details in credentials file and try again.')

def write_to_table(cleaned_tweets, db_user, db_password, db_host, db_name, db_table):
    """Writes in memory pandas dataframe to MySQL database using predefined schema.

    Args:
        cleaned_tweets (df): pandas df of cleaned tweets.
        db_user (str): MySQL user
        db_password (str): MySQL password
        db_host (str): MySQL hostname
        db_name (str): MySQL database name
        db_table (str): MySQL table na,e

    Raises:
        RuntimeError: If fatal database error occurs.
    """

    TABLES = {}
    TABLES['tweets'] = (
    "CREATE TABLE {} (".format(db_table))
    "  id int(12) AUTO_INCREMENT,"
    "  created_at DATETIME NOT NULL,"
    "  tweet_id int(21) NOT NULL,"
    "  lang varchar(3) NOT NULL,"
    "  text varchar(280) NOT NULL,"
    "  at_who varchar(32) NOT NULL,"
    "  PRIMARY KEY (id)"
    ") ENGINE=InnoDB"

    cursor, cnx = create_connection(db_user, db_password, db_host, db_name)

    # Insert DataFrame recrds row by row.
    for i,row in cleaned_tweets.iterrows():
        cursor.execute('INSERT INTO tweets(created_at, \
                            tweet_id, lang, text, at_who )' \
                            'VALUES("%s", "%s", "%s", "%s", "%s")',
                            (row['created_at'], row['tweet_id'], row['lang'], row['text'], row['at_who']))  # using list comp to keep dataframe col order for tuple
    # close the connection to the database.
    cnx.commit()
    cursor.close()

def main(cleaned_tweets):
    """Uses credentials in credentials.json file to connect to MySQL database and write cleaned tweets to db."""
    print('Getting credentials...')
    db_user, db_password, db_host, db_name, db_table = getCredentials(credentialsFile='credentials.json')
    print('Uploading data to database...')
    write_to_table(cleaned_tweets, db_user, db_password, db_host, db_name, db_table)
    print("Data added successfully.")

if __name__ == "__main__":
    main()