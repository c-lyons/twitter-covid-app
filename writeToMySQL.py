# from __future__ import print_function
# import mysql.connector
import csv
import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Text
import pymysql
from mysql.connector import errorcode
from sqlalchemy import exc

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

# def create_connection(db_user, db_password, db_host, db_name):
#     """Creates connection using credentials provided in credentials.json"""
#     try:
#         cnx = mysql.connector.connect(user=db_user,
#                                     password=db_password,
#                                     host=db_host,
#                                     database=db_name)
#         cursor = cnx.cursor()
#         print('Connection created successfully.')
#         return cursor
#     except:
#         raise RuntimeError('Could not connect to database. Check details in credentials file and try again.')

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

    # TABLES = {}
    # TABLES['tweets'] = (
    # "CREATE TABLE {} (".format(db_table))
    # "  id int(12) AUTO_INCREMENT,"
    # "  created_at DATETIME NOT NULL,"
    # "  tweet_id int(21) NOT NULL,"
    # "  lang varchar(3) NOT NULL,"
    # "  text varchar(280) NOT NULL,"
    # "  at_who varchar(32) NOT NULL,"
    # "  PRIMARY KEY (id)"
    # ") ENGINE=InnoDB"

    # cursor = create_connection(db_user, db_password, db_host, db_name)

    try:
        print('Creating connection...')
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}", echo=True)
        con = engine.connect()
        # cnx = mysql.connector.connect(user=db_user,
        #                             password=db_password,
        #                             host=db_host,
        #                             database=db_name)
        # cursor = cnx.cursor()
        print('Connection created successfully.')
        # return cursor
    except: raise RuntimeError('Could not connect to database. Check details in credentials file and try again.')
    try:
        meta = MetaData()
        tweets = Table(
                        'tweets', meta,
                        Column('id', Integer(12),  primary_key = True, autoincrement=True),
                        Column('created_at', DateTime, nullable=False),
                        Column('tweet_id', String(21)),
                        Column('lang', String(3)),
                        Column('text', String(280)),
                        Column('at_who', String(32))
                        )

        meta.create_all(engine)
    except: print('Warning raised during table creation. Table may exist. Trying to append data...')

    # trying to create table - if exists, warning is raised.
    # for table_name in TABLES:
    #     table_description = TABLES[table_name]
    #     try:
    #         print("Creating table {}: ".format(table_name), end='')
    #         con.execute(table_description)
    #         print("New table created.")
    #     #except mysql.connector.Error as err:
    #     except exc.SQLAlchemyError as e:
    #         if e.args[0] == '(MySQLdb._exceptions.OperationalError) (1050, "Table \'{}\' already exists")'.format(db_table):
    #             print("Warning, table already exists. Data will be appended...")
    #         else: raise RuntimeError('Fatal Error: '+str(e))

    print('Uploading data to database...')
    print(cleaned_tweets.head())
    cleaned_tweets.to_sql(db_table, engine, if_exists='append')
        #         cursor.execute('INSERT INTO tweets(created_at, \
    #                         id, lang, text, at_who )' \
    #                         'VALUES("%s", "%s", "%s", "%s", "%s")',
    #                         row)
    # with open('cleaned_tweets.csv', 'r') as csvfile:

    #     csvreader = csv.reader(csvfile)
    #     # This skips the first row of the CSV file.
    #     next(csvreader)
    #     for row in csvreader:
    #         cursor.execute('INSERT INTO tweets(created_at, \
    #                         id, lang, text, at_who )' \
    #                         'VALUES("%s", "%s", "%s", "%s", "%s")',
    #                         row)
    #close the connection to the database.
    cnx.commit()
    cursor.close()

def main(cleaned_tweets):
    """Uses credentials in credentials.json file to connect to MySQL database and write cleaned tweets to db."""
    print('Getting credentials...')
    db_user, db_password, db_host, db_name, db_table = getCredentials(credentialsFile='credentials.json')
    write_to_table(cleaned_tweets, db_user, db_password, db_host, db_name, db_table)
    print("Data added successfully.")

if __name__ == "__main__":
    main()