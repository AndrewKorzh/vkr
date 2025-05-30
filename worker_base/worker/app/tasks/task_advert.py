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
import time

warnings.simplefilter('ignore', InsecureRequestWarning)

from .task_base import (
    TaskBase,
    TaskStatus,
    RequestLimiter,
)

from app.worker_public_config import STG_SCHEMA_NAME, STG_ADVERT_INFO_TABLE_NAME, STG_ADVERT_LOAD_INFO_TABLE_NAME, ADVERT_UPDATE_SCEDUAL, STG_ADVERT_STAT_TABLE_NAME

import pandas as pd

ADVERT_DAYS_TO_LOAD = 90

ADVERT_IDS_CHUNK_MAX_SIZE = 100
ADVERT_DATES_CHUNK_MAX_SIZE = 31


class taskAdvert(TaskBase):

    task_class_identifier = "taskAdvert"

    def __init__(self, db_handler, logger, store_id, api_token, last_run_time):
        super().__init__(
            db_handler,
            logger,
            store_id,
            api_token,
            last_run_time,
        )

        self.request_limiter = RequestLimiter(
            max_requests=1,
            per_seconds=70,
        )

    def get_data_to_load_as_payload(self) -> dict:
        query = f"""
            WITH 
            distinct_ids AS (
                SELECT DISTINCT advert_id
                FROM {STG_SCHEMA_NAME}.{STG_ADVERT_LOAD_INFO_TABLE_NAME}
                WHERE loaded = false and store_id = {self.store_id}
                LIMIT {ADVERT_IDS_CHUNK_MAX_SIZE}
            ),
            ids_with_dates AS (
                SELECT 
                    li.advert_id,
                    array_agg(li."date" ORDER BY li."date") AS dates_array
                FROM {STG_SCHEMA_NAME}.{STG_ADVERT_LOAD_INFO_TABLE_NAME} li
                JOIN distinct_ids di ON li.advert_id = di.advert_id
                WHERE li.loaded = false
                GROUP BY li.advert_id
                LIMIT {ADVERT_DATES_CHUNK_MAX_SIZE}
            )
            SELECT 
                advert_id,
                dates_array[1:LEAST(array_length(dates_array, 1), {ADVERT_DATES_CHUNK_MAX_SIZE})] AS dates
            FROM ids_with_dates;
        """

        data_to_load = self.db_handler.fetch_all(query)

        data_to_load_dict = [{
            "id": row[0],
            "dates": [d.isoformat() for d in row[1]]
        } for row in data_to_load]

        return data_to_load_dict

    def get_advert_data(
        self,
        payload,
    ):
        request_is_allowed = self.request_limiter.is_request_allowed()
        if not request_is_allowed:
            print("---  request is not allowed in get_advert_data")
            return None
        api_url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        try:

            response = requests.post(api_url,
                                     headers=headers,
                                     json=payload,
                                     verify=False)
            if response.status_code == 200:
                response_json = response.json()
                if not response_json:
                    return []
                return response_json
            elif response.status_code == 400:
                return []
            elif response.status_code == 429:
                print("---  get_advert_data: block_for_60_seconds")
                self.logger.error(
                    source="get_advert_data",
                    message=f"get_advert_data: block_for_60_seconds",
                    store_id=self.store_id,
                )
                self.request_limiter.block_for_60_seconds()
                return None
            else:
                self.logger.error(
                    source="get_advert_data",
                    message=
                    f"Ошибка при запросе: {response.status_code} : {response.text}",
                    store_id=self.store_id,
                )
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(
                source="get_advert_data",
                message=f"Произошла ошибка при подключении: {e}",
                store_id=self.store_id,
            )
            return None

    def process_advert_data(self, data):
        try:
            advert_stat = [{
                "advert_id": single_advert["advertId"],
                "days": single_advert["days"]
            } for single_advert in data]

            advert_day_stat = []
            for single_advert in advert_stat:
                advert_id = single_advert["advert_id"]
                days = single_advert["days"]
                advert_day_stat.extend([{
                    "advert_id": advert_id,
                    "date": single_day["date"][:10],
                    "apps": single_day["apps"]
                } for single_day in days])

            advert_day_app_stat = []
            for single_advert_day in advert_day_stat:
                advert_id = single_advert_day["advert_id"]
                date = single_advert_day["date"]
                apps = single_advert_day["apps"]
                advert_day_app_stat.extend([{
                    "advert_id": advert_id,
                    "date": date,
                    "app_type": single_app["appType"],
                    "nm": single_app["nm"]
                } for single_app in apps])

            advert_day_app_nm_stat = []
            for single_advert_day_app_nm in advert_day_app_stat:
                advert_id = single_advert_day_app_nm["advert_id"]
                date = single_advert_day_app_nm["date"]
                app_type = single_advert_day_app_nm["app_type"]
                nm = single_advert_day_app_nm["nm"]

                advert_day_app_nm_stat.extend([{
                    "advert_id":
                    advert_id,
                    "date":
                    date,
                    "app_type":
                    app_type,
                    "nm_id":
                    single_nm.get("nmId"),
                    "views":
                    single_nm.get("views"),
                    "clicks":
                    single_nm.get("clicks"),
                    "ctr":
                    single_nm.get("ctr"),
                    "cpc":
                    single_nm.get("cpc"),
                    "sum":
                    single_nm.get("sum"),
                    "atbs":
                    single_nm.get("atbs"),
                    "orders":
                    single_nm.get("orders"),
                    "cr":
                    single_nm.get("cr"),
                    "shks":
                    single_nm.get("shks"),
                    "sum_price":
                    single_nm.get("sum_price")
                } for single_nm in nm])

            return advert_day_app_nm_stat
        except Exception as e:
            self.logger.error(
                source="process_advert_data",
                message=f"Произошла ошибка при обработке: {e}",
                store_id=self.store_id,
            )
            return None

    def insert_advert_stat(self, advert_data: list[dict]) -> str:
        if not advert_data:
            return "No data to insert"

        def safe_int(value):
            try:
                return int(value)
            except (ValueError, TypeError):
                return 0

        def safe_float(value):
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0

        try:
            start_time = time.time()

            temp_table_name = "temp_stg_advert_stat"
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table_name} (
                date DATE,
                store_id INTEGER,
                advert_id INTEGER,
                app_type INTEGER,
                nm_id INTEGER,

                views INTEGER,
                clicks INTEGER,
                ctr NUMERIC,
                cpc NUMERIC,
                sum NUMERIC,
                atbs INTEGER,
                orders INTEGER,
                cr NUMERIC,
                shks INTEGER,
                sum_price NUMERIC
            ) ON COMMIT DROP;
            """

            buf = StringIO()
            writer = csv.writer(buf, delimiter='\t')

            for item in advert_data:
                writer.writerow([
                    item.get("date"),
                    self.store_id,
                    item.get("advert_id"),
                    item.get("app_type"),
                    item.get("nm_id"),
                    safe_int(item.get("views")),
                    safe_int(item.get("clicks")),
                    safe_float(item.get("ctr")),
                    safe_float(item.get("cpc")),
                    safe_float(item.get("sum")),
                    safe_int(item.get("atbs")),
                    safe_int(item.get("orders")),
                    safe_float(item.get("cr")),
                    safe_int(item.get("shks")),
                    safe_float(item.get("sum_price")),
                ])

            buf.seek(0)

            with self.db_handler.connection:
                with self.db_handler.connection.cursor() as cur:
                    cur.execute(create_temp_table_query)
                    cur.copy_from(buf,
                                  temp_table_name,
                                  sep='\t',
                                  columns=[
                                      "date", "store_id", "advert_id",
                                      "app_type", "nm_id", "views", "clicks",
                                      "ctr", "cpc", "sum", "atbs", "orders",
                                      "cr", "shks", "sum_price"
                                  ])

                    insert_query = f"""
                    INSERT INTO {STG_SCHEMA_NAME}.{STG_ADVERT_STAT_TABLE_NAME} (
                        date, store_id, advert_id, app_type, nm_id,
                        views, clicks, ctr, cpc, sum, atbs, orders, cr, shks, sum_price
                    )
                    SELECT 
                        date, store_id, advert_id, app_type, nm_id,
                        views, clicks, ctr, cpc, sum, atbs, orders, cr, shks, sum_price
                    FROM {temp_table_name}
                    ON CONFLICT (date, store_id, advert_id, app_type, nm_id)
                    DO UPDATE SET
                        views = EXCLUDED.views,
                        clicks = EXCLUDED.clicks,
                        ctr = EXCLUDED.ctr,
                        cpc = EXCLUDED.cpc,
                        sum = EXCLUDED.sum,
                        atbs = EXCLUDED.atbs,
                        orders = EXCLUDED.orders,
                        cr = EXCLUDED.cr,
                        shks = EXCLUDED.shks,
                        sum_price = EXCLUDED.sum_price,
                        created_at = CURRENT_TIMESTAMP;
                    """

                    cur.execute(insert_query)
                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                    count = cur.fetchone()[0]

            duration = time.time() - start_time
            return f"Inserted/Updated {count} rows in {duration:.2f} seconds"

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            raise RuntimeError(f"Error during advert stat insert: {str(e)}")

    def generate_advert_load_data(self) -> bool:
        try:
            query = f"""
                DELETE FROM {STG_SCHEMA_NAME}.{STG_ADVERT_LOAD_INFO_TABLE_NAME}
                WHERE store_id = {self.store_id};

                INSERT INTO {STG_SCHEMA_NAME}.{STG_ADVERT_LOAD_INFO_TABLE_NAME} (store_id, advert_id, date, loaded)
                WITH 
                    filtered_ids AS (
                        SELECT advert_id
                        FROM {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME}
                        WHERE store_id = {self.store_id}
                        AND end_time >= (NOW() - INTERVAL '{ADVERT_DAYS_TO_LOAD} days')
                    ),
                    date_series AS (
                        SELECT generate_series(
                            date_trunc('day', NOW() - INTERVAL '{ADVERT_DAYS_TO_LOAD} days'),
                            date_trunc('day', NOW()),
                            INTERVAL '1 day'
                        )::date AS report_date
                    )
                SELECT 
                    {self.store_id} AS store_id,
                    fi.advert_id,
                    ds.report_date,
                    false AS loaded
                FROM filtered_ids fi
                CROSS JOIN date_series ds;
            """
            self.db_handler.execute_query(query)
            return True

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            raise RuntimeError(
                f"Failed to regenerate advert load data: {str(e)}")

    def mark_adverts_as_loaded(self, advert_date_dicts: list[dict]) -> str:
        if not advert_date_dicts or len(advert_date_dicts) == 0:
            return f"No data to mark as loaded, advert_date_dicts: {advert_date_dicts}"

        try:
            start_time = time.time()

            temp_table_name = f"temp_stg_advert_load_info"
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table_name} (
                store_id INTEGER,
                advert_id INTEGER,
                date DATE,
                loaded BOOLEAN
            ) ON COMMIT DROP;
            """
            buf = StringIO()
            writer = csv.writer(buf, delimiter='\t')

            for item in advert_date_dicts:
                advert_id = item["id"]
                for date in item["dates"]:
                    writer.writerow((self.store_id, advert_id, date, True))

            buf.seek(0)

            with self.db_handler.connection:
                with self.db_handler.connection.cursor() as cur:
                    cur.execute(create_temp_table_query)

                    cur.copy_from(buf,
                                  temp_table_name,
                                  sep='\t',
                                  columns=('store_id', 'advert_id', 'date',
                                           'loaded'))

                    update_query = f"""
                        UPDATE {STG_SCHEMA_NAME}.{STG_ADVERT_LOAD_INFO_TABLE_NAME} AS target
                        SET loaded = TRUE
                        FROM {temp_table_name} temp
                        WHERE target.store_id = temp.store_id
                        AND target.advert_id = temp.advert_id
                        AND target.date = temp.date;
                        """
                    cur.execute(update_query)

                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                    count = cur.fetchone()[0]

            duration = time.time() - start_time
            return f"Marked {count} rows as loaded in {duration:.2f} seconds"

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            raise RuntimeError(f"Error during marking as loaded: {str(e)}")

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

    def get_advert_load_info_status_report(self):
        query = f"""
            WITH 
            load_data AS (
                SELECT *
                FROM {STG_SCHEMA_NAME}.{STG_ADVERT_LOAD_INFO_TABLE_NAME}
                WHERE store_id = {self.store_id}
            ),
            loaded_rows as (
            	SELECT *
                FROM load_data
                WHERE loaded = true
            ),
            info_data_filtered AS (
                SELECT advert_id
                FROM {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME}
                WHERE store_id = {self.store_id}
                AND end_time >= (NOW() - INTERVAL '{ADVERT_DAYS_TO_LOAD} days')
            ),
            load_ids AS (
                SELECT DISTINCT advert_id FROM load_data
            ),
            info_ids AS (
                SELECT DISTINCT advert_id FROM info_data_filtered
            ),
            difference_ids AS (
                SELECT advert_id
                FROM load_ids
                WHERE advert_id NOT IN (SELECT advert_id FROM info_ids)
                union 
                SELECT advert_id
                FROM info_ids
                WHERE advert_id NOT IN (SELECT advert_id FROM load_ids)
            )
            SELECT
                COUNT(CASE 
                    WHEN (ld.created_at)::DATE >= (CURRENT_TIMESTAMP - INTERVAL '{ADVERT_UPDATE_SCEDUAL}')::DATE 
                    THEN 1 END) AS actual,
                (select count (*) from loaded_rows) as loaded,
                COUNT(*) AS count_all,
                (SELECT COUNT(*) FROM load_ids) AS load_advert_ids,
                (SELECT COUNT(*) FROM info_ids) AS info_advert_ids,
                (SELECT COUNT(*) FROM difference_ids) AS difference_count
            FROM load_data ld;
        """
        # (SELECT array_agg(advert_id) FROM difference_ids) AS missing_advert_ids

        status_report = self.db_handler.execute_and_fetch_single_row(query)

        actual = status_report["actual"]
        loaded = status_report["loaded"]
        count_all = status_report["count_all"]
        load_advert_ids = status_report["load_advert_ids"]
        info_advert_ids = status_report["info_advert_ids"]
        difference_count = status_report["difference_count"]

        print(f"""
        Статус загрузки данных по рекламе:
        ----------------------------------
        Актуальных записей (actual):      {actual}
        Загруженных записей (loaded):     {loaded}
        Всего записей (count_all):        {count_all}
        Advert ID в загрузках:            {load_advert_ids}
        Advert ID в информации:           {info_advert_ids}
        Расхождение по Advert ID:         {difference_count}
        """)

        return status_report

    # TODO - проверять на целевые даты еще!!!
    def check_advert_load_info_is_ok(self):

        status_report = self.get_advert_load_info_status_report()

        actual = status_report["actual"]
        loaded = status_report["loaded"]
        count_all = status_report["count_all"]
        load_advert_ids = status_report["load_advert_ids"]
        info_advert_ids = status_report["info_advert_ids"]
        difference_count = status_report["difference_count"]

        if difference_count != 0:
            # self.logger.error(
            #     source="check_advert_load_info_is_ok",
            #     message=f"difference_count != 0",
            #     store_id=self.store_id,
            # )
            return False

        if count_all == 0:
            # self.logger.error(
            #     source="check_advert_load_info_is_ok",
            #     message=f"count_all == 0",
            #     store_id=self.store_id,
            # )
            return False

        if count_all != actual:
            return False

        return True

    def check_advert_data_is_loaded(self):
        status_report = self.get_advert_load_info_status_report()

        actual = status_report["actual"]
        loaded = status_report["loaded"]
        count_all = status_report["count_all"]

        if actual == loaded and count_all != 0:
            return True

        return False

    def process(self):

        if not self.advert_list_is_ok() or not self.advert_info_is_ok():
            self.status = TaskStatus.IN_PROGRESS
            return self._make_response()

        advert_load_info_is_ok = self.check_advert_load_info_is_ok()
        if not advert_load_info_is_ok:
            gen_res = self.generate_advert_load_data()
            if not gen_res:
                self.status = TaskStatus.IN_PROGRESS
                return self._make_response()

        if self.check_advert_data_is_loaded():
            self.status = TaskStatus.SUCCESS
            return self._make_response()

        data_to_load = self.get_data_to_load_as_payload()

        data = self.get_advert_data(payload=data_to_load, )

        # print(data_to_load)

        if not isinstance(data, list):
            self.status = TaskStatus.IN_PROGRESS
            return self._make_response()

        if len(data) != 0:
            processed_data = self.process_advert_data(data=data)
            if processed_data:
                # TODO обрабатывать нормально чтобы понятно было что к чему
                # Проверять на вставку и если что пропускать
                insert_result = self.insert_advert_stat(processed_data)
                self.logger.info(
                    source="insert_advert_stat",
                    message=f"advert inserted: {insert_result}",
                    store_id=self.store_id,
                )
            else:
                self.logger.error(
                    source="insert_advert_stat",
                    message=f"processed_data is null",
                    store_id=self.store_id,
                )
                self.status = TaskStatus.IN_PROGRESS
                return self._make_response()

        self.mark_adverts_as_loaded(advert_date_dicts=data_to_load)

        if self.check_advert_data_is_loaded():
            self.status = TaskStatus.SUCCESS
            return self._make_response()

        self.status = TaskStatus.IN_PROGRESS
        return self._make_response()
