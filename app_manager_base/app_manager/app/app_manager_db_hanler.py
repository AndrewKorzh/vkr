import psycopg2
from app.app_manager_private_config import DB_CONFIG

from decimal import Decimal
import datetime
import json


def normalize_value(value):
    if isinstance(value, datetime.date):
        return value.isoformat()
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, bytes):
        return value.decode('utf-8')
    elif isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value)
    return value


class AppManagerDBHandler:

    def __init__(self):
        self.db_config = DB_CONFIG
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        self.connection = psycopg2.connect(**self.db_config)
        self.cursor = self.connection.cursor()
        self.cursor.execute("SET timezone = 'Europe/Moscow';")
        self.connection.commit()

    def execute_query(self, query, params=None):
        """Выполнение SQL-запроса"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                raise e

    def execute_many(self, query, params_list):
        if self.cursor:
            try:
                self.cursor.executemany(query, params_list)
                self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                raise e

    def fetch_all(self, query, params=None):
        """Выборка всех данных"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except Exception as e:
                raise e

    def fetch_one(self, query, params=None):
        """Выборка одной строки"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchone()
            except Exception as e:
                raise e

    def execute_and_fetch_single_row(self, query, params=None):
        if not self.cursor:
            raise RuntimeError("Cursor is not initialized")

        try:
            self.cursor.execute(query, params)
            result = None
            if self.cursor.description:
                row = self.cursor.fetchone()
                if row:
                    colnames = [desc[0] for desc in self.cursor.description]
                    result = dict(zip(colnames, row))
            if self.connection:
                self.connection.commit()

            return result

        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise e

    def fetch_all_with_headers(self, query, params=None):
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                rows = self.cursor.fetchall()
                headers = [desc[0] for desc in self.cursor.description]
                result = [headers] + [[normalize_value(cell) for cell in row]
                                      for row in rows]
                return result
            except Exception as e:
                raise e

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        print("connection closed")
