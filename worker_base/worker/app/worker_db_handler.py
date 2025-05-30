import psycopg2
from app.worker_private_config import DB_CONFIG
from psycopg2.extensions import connection as pg_connection, cursor as pg_cursor
from typing import Optional

import time
from functools import wraps


def log_duration(method):

    @wraps(method)
    def timed(*args, **kwargs):
        start = time.time()
        result = method(*args, **kwargs)
        duration = time.time() - start
        print(f"[TIMER] {method.__name__} executed in {duration:.3f} seconds")
        return result

    return timed


class WorkerDBHandler:

    def __init__(self):
        self.db_config = DB_CONFIG
        self.connection: Optional[pg_connection] = None
        self.cursor: Optional[pg_cursor] = None
        self.connect()

    def connect(self):
        print(f"DB_CONFIG: {self.db_config}")
        self.connection = psycopg2.connect(**self.db_config)
        self.cursor = self.connection.cursor()
        self.cursor.execute("SET timezone = 'Europe/Moscow';")
        self.connection.commit()

    # @log_duration
    def execute_query(self, query, params=None):
        """Выполнение SQL-запроса"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                raise e

    # @log_duration
    def execute_many(self, query, params=None):
        """Массовая вставка данных"""
        if self.cursor:
            try:
                if params and isinstance(params, list):
                    self.cursor.executemany(query, params)
                else:
                    raise ValueError(
                        "Params must be a list of tuples for bulk insert.")

                self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                raise e

    # @log_duration
    def fetch_all(self, query, params=None):
        """Выборка всех данных"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except Exception as e:
                raise e

    # @log_duration
    def fetch_one(self, query, params=None):
        """Выборка одной строки"""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchone()
            except Exception as e:
                raise e

    # @log_duration
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

    # @log_duration
    def execute_and_fetch_all(self, query, params=None) -> list[dict] | None:
        if not self.cursor:
            raise RuntimeError("Cursor is not initialized")

        try:
            self.cursor.execute(query, params)
            if not self.cursor.description:
                if self.connection:
                    self.connection.commit()
                return None
            colnames = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            result = [dict(zip(colnames, row)) for row in rows]

            if self.connection:
                self.connection.commit()

            return result if result else None

        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise e

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        print("connection closed")
