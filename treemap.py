#!/usr/bin/env python3
__author__ = 'Meirkhan Rakhmetzhanov, mail:rakhmetzhanoff@gmail.com'

import psycopg2
import csv
import logging
from config import config

logging.basicConfig(level=logging.INFO)
conf = config.Config()

class BotDB():
    """
    Connection to Postgres database
    """
    def __init__(self):
        self.connection = None

    def connect_db(self, db, db_user, db_pwd):
        if self.connection is None:
            if not db_user:
                db_user = 'postgres'
            self.connection = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)

    def close(self):
        self.connection.close()


    def create_table(self, table_name):
        commands = [
            """
            DROP TABLE IF EXISTS %s;
            """ % table_name,
            """
            CREATE TABLE %s (
                ID INTEGER PRIMARY KEY,
                location geography
            );
            """ % table_name
        ]
        try:
            cursor = self.connection.cursor()
            for command in commands:
                cursor.execute(command)
            cursor.close()
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


    def insert_data(self, file, table):
        cursor = self.connection.cursor()
        with open(file, 'r') as f:
            next(f)
            reader = csv.reader(f)
            all_rows = []
            counter = 0
            for row in reader:
                row = [int(row[0]), str(row[1]), int(row[2])]
                all_rows.append(row)
                if counter == 10000:  # in order to not store everything in memory
                    sql = "INSERT INTO {} (id, location) VALUES (%s, ST_GeomFromText(%s, %s))".format(table)
                    cursor.executemany(sql, all_rows)
                    counter = 0
                    all_rows = []
                counter += 1

            sql = "INSERT INTO {} (id, location) VALUES (%s, ST_GeomFromText(%s, %s))".format(table)
            cursor.executemany(sql, all_rows)
            self.connection.commit()


    def insert_without_srid(self):
        """
        Easy way to upload all data at once
        """
        cursor = self.connection.cursor()
        this_copy = 'COPY %s FROM STDIN WITH CSV HEADER'

        for table in conf.db_tables.keys():
            files = conf.db_tables[table]
            for file in files:
                with open(file, 'rb') as this_file:
                    cursor.copy_expert(this_copy % table, this_file)
        self.connection.commit()


    def setup(self):
        """
        DB tables setup and data ingestion
        """
        try:
            for table in conf.db_tables.keys():
                self.create_table(table)
                files = conf.db_tables[table]
                for file in files:
                    self.insert_data(file, table)
        except Exception as e:
            logging.error(e)
        finally:
            self.connection.close()


def main():
    db_factory = BotDB()
    db_factory.connect_db(conf.db, conf.db_user, conf.db_passwd)
    db_factory.setup()
    print('finished')

    # TODO: Need to see/create DB indexes to speed up the search



if __name__ == '__main__':
    main()
