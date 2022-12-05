import mysql.connector
import logging


class MockDB:
    def __init__(self, host, user, password, port):
        self.cnx = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            port=port
        )

    def drop_db(self, db_name):
        logging.info(f'Dropping database {db_name}')
        cursor = self.cnx.cursor(dictionary=True)

        # Drop test database if it already exists
        try:
            cursor.execute(f"DROP DATABASE {db_name}")
            cursor.close()
        except mysql.connector.Error as err:
            logging.error(f"Fail to delete database: {err}")

    def create_db(self, db_name):
        logging.info(f'Creating database {db_name}')
        cursor = self.cnx.cursor(dictionary=True)

        # Create test database if it does not already exist
        try:
            cursor.execute(
                f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8'")
            self.cnx.database = db_name
        except mysql.connector.Error as err:
            logging.error(f"Fail to create database: {err}")
