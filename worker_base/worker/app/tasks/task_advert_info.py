import time
from enum import Enum
from copy import deepcopy

import requests
import pandas as pd
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning

from io import StringIO
import csv

warnings.simplefilter('ignore', InsecureRequestWarning)

from .task_base import (
    TaskBase,
    TaskStatus,
)

from app.worker_public_config import STG_SCHEMA_NAME, STG_ADVERT_INFO_TABLE_NAME, ADVERT_UPDATE_SCEDUAL


class taskAdvertInfo(TaskBase):
    task_class_identifier = "taskAdvertInfo"

    def __init__(self, db_handler, logger, store_id, api_token, last_run_time):
        super().__init__(
            db_handler,
            logger,
            store_id,
            api_token,
            last_run_time,
        )

    def get_advert_list_data(self):
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"

        headers = {"Authorization": self.api_token}
        try:
            response = requests.get(f"{url}", headers=headers, verify=False)

            if response.status_code == 200:
                data = response.json()
                return data
            else:
                self.raise_error(
                    f"request error: {response.status_code}: {response.text}")

        except Exception as e:
            self.raise_error(f"request error: {e}")

    def process_advert_list_data(self, advert_list_data):
        advert_list_mapping = []
        for advert_big in advert_list_data["adverts"]:
            advert_big_type = advert_big["type"]
            advert_big_list = advert_big["advert_list"]
            for advert_small in advert_big_list:
                advert_list_mapping.append({
                    "advert_id":
                    advert_small["advertId"],
                    "advert_type":
                    advert_big_type
                })

        return advert_list_mapping

    def advert_list_is_ok(self):
        query = f"""
            SELECT
                COUNT(CASE WHEN (al.created_at)::DATE >= (CURRENT_TIMESTAMP - INTERVAL '{ADVERT_UPDATE_SCEDUAL}')::DATE THEN 1 END) AS actual,
                COUNT(*) AS count_all
            FROM {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME} al
            WHERE al.store_id = {self.store_id};
        """

        status_report = self.db_handler.execute_and_fetch_single_row(query)

        actual = status_report["actual"]
        count_all = status_report["count_all"]

        if count_all == 0 or actual < count_all:
            return False
        elif actual == count_all:
            return True
        else:
            self.logger.error(
                source="advert_list_is_ok",
                message=f"unknown status_report",
                store_id=self.store_id,
            )
            return False

    def advert_info_is_ok(self):
        query = f"""
            SELECT
                COUNT(CASE WHEN last_info_update_time IS NULL THEN 1 END) AS null_count,
                COUNT(CASE WHEN last_info_update_time >= (CURRENT_TIMESTAMP - INTERVAL '{ADVERT_UPDATE_SCEDUAL}') THEN 1 END) AS actual_count,
                COUNT(*) AS total_count
            FROM {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME}
            WHERE store_id = {self.store_id};
        """

        status_report = self.db_handler.execute_and_fetch_single_row(query)

        null_count = status_report["null_count"]
        actual_count = status_report["actual_count"]
        total_count = status_report["total_count"]

        if total_count == 0:
            return False

        if null_count > 0:
            return False

        if actual_count < total_count:
            return False

        if actual_count == total_count:
            return True

        self.logger.error(
            source="advert_info_is_ok",
            message=f"unknown status_report",
            store_id=self.store_id,
        )

        return False

    def get_advert_ids_by_store(self) -> list[int]:
        query = f"""
            SELECT advert_id 
            FROM {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME}
            WHERE store_id = %s
            ORDER BY advert_id;
        """

        try:
            result = self.db_handler.fetch_all(query, (self.store_id, ))
            return [row[0] for row in result] if result else None
        except Exception as e:
            self.logger.error(
                source="get_advert_ids_by_store",
                message=f"Failed to get advert_ids for store : {str(e)}",
                store_id=self.store_id,
            )
        return None

    def get_list_by_parts(self, lst, p_size=45):
        lst_len = len(lst)
        parts = []

        for i in range(lst_len // p_size + 1):
            lower_index = i * p_size
            higher_index = min((i + 1) * p_size, len(lst))
            parts.append(lst[lower_index:higher_index])

        return parts

    def get_all_advert_info_data(self, ids):
        parts = self.get_list_by_parts(ids)
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"

        headers = {"Authorization": self.api_token}
        result = []
        len_parst = len(parts)
        for i in range(len_parst):
            time.sleep(0.25)
            payload = parts[i]
            try:
                response = requests.post(url,
                                         headers=headers,
                                         json=payload,
                                         verify=False)
                if response.status_code == 200:
                    adverts = response.json()
                    result.extend(adverts)
                elif response.status_code == 429:
                    self.logger.error(
                        source="get_all_advert_info",
                        message=f"to many request",
                        store_id=self.store_id,
                    )
                else:
                    self.logger.error(
                        source="get_all_advert_info",
                        message=
                        f"Ошибка при запросе: {response.status_code}, ответ: {response.text}",
                        store_id=self.store_id,
                    )
                    return None

            except Exception as e:
                self.logger.error(
                    source="get_all_advert_info",
                    message=f"Произошла ошибка при подключении: {e}",
                    store_id=self.store_id,
                )
                return None

        return result

    def process_all_advert_info_data(self, advert_info_data):
        result = []
        try:
            for advert in advert_info_data:
                result.append({
                    "advert_id": advert["advertId"],
                    "start_time": advert["startTime"],
                    "end_time": advert["endTime"],
                    "create_time": advert["createTime"],
                    "change_time": advert["changeTime"],
                })

            return result

        except Exception as e:
            self.logger.error(
                source="process_all_advert_info_data",
                message=f"Произошла ошибка: {e}",
                store_id=self.store_id,
            )
            return None

    def insert_advert_info(self, advert_data: list[dict]) -> bool:
        if not advert_data:
            return False

        try:
            start_time = time.time()

            temp_table_name = f"temp_{STG_ADVERT_INFO_TABLE_NAME}_info"
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table_name} (
                store_id INTEGER,
                advert_id INTEGER,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                create_time TIMESTAMP,
                change_time TIMESTAMP
            ) ON COMMIT DROP;
            """
            buf = StringIO()
            writer = csv.writer(buf, delimiter='\t')

            for item in advert_data:
                row = (self.store_id, item['advert_id'], item['start_time'],
                       item['end_time'], item['create_time'],
                       item['change_time'])
                writer.writerow(row)

            buf.seek(0)

            with self.db_handler.connection:
                with self.db_handler.connection.cursor() as cur:
                    cur.execute(create_temp_table_query)
                    cur.copy_from(buf,
                                  temp_table_name,
                                  sep='\t',
                                  columns=('store_id', 'advert_id',
                                           'start_time', 'end_time',
                                           'create_time', 'change_time'))

                    update_query = f"""
                    UPDATE {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME} AS target
                    SET
                        start_time = temp.start_time,
                        end_time = temp.end_time,
                        create_time = temp.create_time,
                        change_time = temp.change_time,
                        last_info_update_time = CURRENT_TIMESTAMP
                    FROM {temp_table_name} AS temp
                    WHERE target.store_id = temp.store_id
                    AND target.advert_id = temp.advert_id;
                    """
                    cur.execute(update_query)
                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                    updated_count = cur.fetchone()[0]

            duration = time.time() - start_time
            print(
                f"-- insert_advert_info -- \nUpdated {updated_count} records in {duration:.2f} seconds"
            )

            return True

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            self.logger.error(
                source="process_all_advert_info_data",
                message=f"Error in bulk update via temp table: {str(e)}",
                store_id=self.store_id,
            )
            return False

    def delete_advert_list_by_store_id(self):
        query = f"""
            DELETE FROM {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME}
            WHERE store_id = {self.store_id};
        """
        try:
            self.db_handler.execute_query(query)
            return f"Deleted advert list entries for store_id={self.store_id}"
        except Exception as e:
            return f"Error while deleting advert list entries for store_id={self.store_id}: {str(e)}"

    def insert_advert_list(self, advert_data: list) -> str:
        if not advert_data:
            return "No advert data to insert"

        try:
            start_time = time.time()

            temp_table_name = f"temp_{STG_ADVERT_INFO_TABLE_NAME}"
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table_name} (
                store_id INTEGER,
                advert_id INTEGER,
                advert_type INTEGER
            ) ON COMMIT DROP;
            """

            buf = StringIO()
            writer = csv.writer(buf, delimiter='\t')

            for item in advert_data:
                row = (self.store_id, item["advert_id"], item["advert_type"])
                writer.writerow(row)

            buf.seek(0)

            with self.db_handler.connection:
                with self.db_handler.connection.cursor() as cur:
                    cur.execute(create_temp_table_query)
                    cur.copy_from(buf,
                                  temp_table_name,
                                  sep='\t',
                                  columns=('store_id', 'advert_id',
                                           'advert_type'))
                    insert_query = f"""
                        INSERT INTO {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME} AS target
                            (store_id, advert_id, advert_type)
                        SELECT 
                            store_id, advert_id, advert_type
                        FROM {temp_table_name}
                        ON CONFLICT (store_id, advert_id)
                        DO UPDATE SET
                            advert_type = EXCLUDED.advert_type;
                        """
                    cur.execute(insert_query)
                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                    processed_count = cur.fetchone()[0]

            duration = time.time() - start_time
            print(
                f"-- insert_advert_list -- \nProcessed {processed_count} records in {duration:.2f} seconds"
            )

            return f"Successfully processed {processed_count} advert records into {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME}"

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            print(f"Error during advert insert operation: {str(e)}")
            raise

    def process(self):

        if self.advert_list_is_ok() and self.advert_info_is_ok():
            self.status = TaskStatus.SUCCESS
            return self._make_response()

        if not self.advert_list_is_ok():
            self.delete_advert_list_by_store_id()
            ald = self.get_advert_list_data()
            if ald:
                processed_ald = self.process_advert_list_data(ald)
                insert_result = self.insert_advert_list(processed_ald)
                print(f"insert_result: {insert_result}")
            else:
                self.logger.error(
                    source="taskFactSales",
                    message=f"advert_list_data is none",
                    store_id=self.store_id,
                )

        if not self.advert_list_is_ok():
            self.status = TaskStatus.IN_PROGRESS
            return self._make_response()

        if not self.advert_info_is_ok():
            advert_ids = self.get_advert_ids_by_store()
            if not advert_ids:
                self.status = TaskStatus.IN_PROGRESS
                return self._make_response()

            aid = self.get_all_advert_info_data(advert_ids)

            if not aid:
                self.status = TaskStatus.IN_PROGRESS
                return self._make_response()

            processed_all_advert_info_data = self.process_all_advert_info_data(
                aid)

            if not processed_all_advert_info_data:
                self.status = TaskStatus.IN_PROGRESS
                return self._make_response()

            advert_info_insert_result = self.insert_advert_info(
                processed_all_advert_info_data, )

            if not advert_info_insert_result:
                self.status = TaskStatus.IN_PROGRESS
                return self._make_response()

        if not self.advert_info_is_ok():
            self.logger.error(
                source="taskFactSales",
                message=f"advert_info_is_ok should be True",
                store_id=self.store_id,
            )
            self.status = TaskStatus.IN_PROGRESS
            return self._make_response()

        self.status = TaskStatus.SUCCESS
        return self._make_response()
