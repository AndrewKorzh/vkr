import time

import requests
import warnings
from urllib3.exceptions import InsecureRequestWarning

from io import StringIO
import csv

warnings.simplefilter('ignore', InsecureRequestWarning)

from .task_base import (
    TaskBase,
    TaskStatus,
)

from app.worker_public_config import STG_SCHEMA_NAME, STG_FACT_SALES_INFO_TABLE_NAME, STG_FACT_SALES_TABLE_NAME


class taskFactSales(TaskBase):
    task_class_identifier = "taskFactSales"

    def __init__(self, db_handler, logger, store_id, api_token, last_run_time):
        super().__init__(
            db_handler,
            logger,
            store_id,
            api_token,
            last_run_time,
        )

    def get_wb_sales(self, date_from):
        url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
        flag = 0
        params = {"dateFrom": date_from, "flag": flag}

        headers = {"Authorization": self.api_token}
        try:
            response = requests.get(f"{url}",
                                    headers=headers,
                                    params=params,
                                    verify=False)

            if response.status_code == 200:
                data = response.json()
            else:
                print(f"Ошибка при запросе: {response.status_code}")
                print(response.text)

        except Exception as e:
            print(f"Произошла ошибка: {e}")

        return data

    def get_status(self):
        SALES_SCEDUAL = "6 hours 15 minutes"

        status_query = f""" 
        SELECT
            CASE
                WHEN COUNT(*) = 0 THEN 'need_load'
                WHEN SUM(CASE WHEN is_final = FALSE THEN 1 ELSE 0 END) > 0 THEN 'need_load'
                WHEN SUM(CASE WHEN is_final = TRUE AND (last_change_date)::timestamp < (CURRENT_TIMESTAMP)::DATE + INTERVAL '{SALES_SCEDUAL}' THEN 1 ELSE 0 END) > 0 THEN 'need_load'
                ELSE 'ok'
            END AS status,
            MAX(last_change_date) AS last_change_date
        FROM {STG_SCHEMA_NAME}.{STG_FACT_SALES_INFO_TABLE_NAME}
        WHERE store_id = {self.store_id};
        """
        return self.db_handler.execute_and_fetch_single_row(query=status_query)

    def process_fact_sales_data(self, data):
        processed_data = []
        for ell in data:
            processed_data.append({
                "nmId": ell["nmId"],
                "last_change_date": ell["lastChangeDate"],
                "date": ell["date"][:10],
                "sale_id": ell["saleID"],
                "sale_type": ell["saleID"][0],
                "price_with_disc": ell["priceWithDisc"]
            })

        return processed_data

    def insert_sales_data(self, sales_data: list) -> str:
        """
        Вставляет данные о продажах через временную таблицу с использованием COPY.
        Обновляет существующие записи при конфликте по sale_id.
        Вся операция выполняется в рамках одной транзакции.
        """
        if not sales_data:
            return "No sales data to insert"

        try:
            start_time = time.time()

            temp_table_name = f"temp_{STG_FACT_SALES_TABLE_NAME}"
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table_name} (
                LIKE {STG_SCHEMA_NAME}.{STG_FACT_SALES_TABLE_NAME} 
                INCLUDING DEFAULTS
            ) ON COMMIT DROP;
            """

            buf = StringIO()
            writer = csv.writer(buf, delimiter='\t')

            for item in sales_data:
                row = (
                    self.store_id,
                    item["sale_id"],
                    item["nmId"],
                    item["sale_type"],
                    item["date"],
                    item["last_change_date"],
                    item["price_with_disc"],
                )
                writer.writerow(row)

            buf.seek(0)

            with self.db_handler.connection:
                with self.db_handler.connection.cursor() as cur:
                    cur.execute(create_temp_table_query)

                    cur.copy_from(buf,
                                  temp_table_name,
                                  sep='\t',
                                  columns=('store_id', 'sale_id', 'nm_id',
                                           'sale_type', 'date',
                                           'last_change_date',
                                           'price_with_disc'))

                    insert_query = f"""
                    INSERT INTO {STG_SCHEMA_NAME}.{STG_FACT_SALES_TABLE_NAME} AS target
                    SELECT * FROM {temp_table_name}
                    ON CONFLICT (sale_id) DO UPDATE SET
                        store_id = EXCLUDED.store_id,
                        nm_id = EXCLUDED.nm_id,
                        sale_type = EXCLUDED.sale_type,
                        date = EXCLUDED.date,
                        last_change_date = EXCLUDED.last_change_date,
                        price_with_disc = EXCLUDED.price_with_disc;
                    """
                    cur.execute(insert_query)

                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                    processed_count = cur.fetchone()[0]

            duration = time.time() - start_time
            print(
                f"-- insert_sales_data -- \nProcessed {processed_count} records in {duration:.2f} seconds"
            )

            return f"Successfully processed {processed_count} records into {STG_SCHEMA_NAME}.{STG_FACT_SALES_TABLE_NAME}"

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            print(f"Error during sales data insert operation: {str(e)}")
            raise

    def insert_or_update_sales_status(self, last_change_date, is_final):
        query = f"""
            INSERT INTO {STG_SCHEMA_NAME}.{STG_FACT_SALES_INFO_TABLE_NAME} (store_id, last_change_date, is_final)
            VALUES (%s, %s, %s)
            ON CONFLICT (store_id)
            DO UPDATE SET
                last_change_date = EXCLUDED.last_change_date,
                is_final = EXCLUDED.is_final;
        """
        try:
            self.db_handler.execute_query(
                query, (self.store_id, last_change_date, is_final))
            print("Status inserted/updated successfully.")
            return "Status inserted/updated successfully."
        except Exception as e:
            print(f"Error while inserting/updating status: {str(e)}")
            return f"Error while inserting/updating status: {str(e)}"

    def process(self):
        status_info = self.get_status()
        status = status_info["status"]
        last_change_date = status_info["last_change_date"]
        if status == "need_load":
            date_from = "2025-01-01T00:00:00"
            if last_change_date:
                date_from = last_change_date

            data = self.get_wb_sales(date_from=date_from)
            if len(data) == 0:
                self.insert_or_update_sales_status(last_change_date=date_from,
                                                   is_final=True)
                self.status = TaskStatus.SUCCESS
            else:
                processed_data = self.process_fact_sales_data(data=data)
                insert_res = self.insert_sales_data(sales_data=processed_data)
                if insert_res:
                    last_change_date = processed_data[len(processed_data) -
                                                      1]["last_change_date"]
                    self.insert_or_update_sales_status(
                        last_change_date=last_change_date, is_final=False)
                else:
                    self.logger.error(
                        source="taskFactSales",
                        message=f"insert_res: {insert_res}",
                        store_id=self.store_id,
                    )
                    self.status = TaskStatus.ERROR
                    return self._make_response(
                        f"unknown status, status_info: {status_info}")
                print(f"insert_res: {insert_res}")
        elif status == "ok":
            print("data is loaded already")
            self.status = TaskStatus.SUCCESS
        else:
            self.logger.error(
                source="taskFactSales",
                message=f"unknown status: {status}",
                store_id=self.store_id,
            )

        return self._make_response()
