import psycopg2
import os
from psycopg2.extensions import connection as pg_connection, cursor as pg_cursor
from typing import Optional
import pandas as pd


class AdminDBHandler:

    def __init__(self, db_config):
        self.db_config = db_config
        self.connection: Optional[pg_connection] = None
        self.cursor: Optional[pg_cursor] = None
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
                return self.cursor.fetchone(
                )  # Возвращает одну строку или None
            except Exception as e:
                raise e

    def fetch_as_dataframe(self, query: str, params=None) -> pd.DataFrame:
        if not self.cursor:
            raise ConnectionError("Нет подключения к базе данных.")

        try:
            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description
                       ]  # Получаем названия столбцов
            data = self.cursor.fetchall()
            return pd.DataFrame(data, columns=columns)  # Создаём DataFrame
        except Exception as e:
            raise e

    def get_table_columns(self, schema_name: str, table_name: str) -> list:
        try:
            query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position;
            """
            self.cursor.execute(query, (schema_name, table_name))
            columns = [row[0] for row in self.cursor.fetchall()]

            if not columns:
                print(f"No columns found for table {schema_name}.{table_name}")
                return []

            print(
                f"Retrieved {len(columns)} columns from {schema_name}.{table_name}"
            )
            return columns

        except Exception as e:
            print(f"ERROR while retrieving table columns: {e}")
            return []

    def copy_table_to_csv(
        self,
        schema_name: str,
        table_name: str,
        columns: list,
        csv_file_path: str,
    ) -> bool:
        try:
            directory = os.path.dirname(csv_file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)

            query = f"""
            COPY {schema_name}.{table_name} (
                {",".join(columns)}
            )
            TO STDOUT
            WITH (FORMAT CSV, HEADER TRUE);
            """

            with open(csv_file_path, 'w', newline='',
                      encoding='utf-8') as csvfile:
                self.cursor.copy_expert(query, csvfile)

            print(
                f"Table {schema_name}.{table_name} exported to {csv_file_path} successfully."
            )
            return True

        except FileNotFoundError as fnf_error:
            print(f"FileNotFoundError: {fnf_error}")
            self.connection.rollback()
            return False

        except PermissionError as perm_error:
            print(f"PermissionError: {perm_error}")
            self.connection.rollback()
            return False

        except Exception as e:
            print(f"ERROR during export: {e}")
            self.connection.rollback()
            return False

        finally:
            self.close()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()
        print("connection closed")
