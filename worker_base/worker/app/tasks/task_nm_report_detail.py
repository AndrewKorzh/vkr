import time
from enum import Enum
import requests
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning
from io import StringIO
import csv

warnings.simplefilter('ignore', InsecureRequestWarning)

from .task_base import (TaskBase, TaskStatus, TaskResponse, RequestLimiter)

from app.worker_public_config import CORE_SCHEMA_NAME, STG_SCHEMA_NAME, STG_NM_REPORT_DETAIL_INFO_TABLE_NAME, STG_NM_REPORT_DETAIL_TABLE_NAME

NM_REPORT_DETAIL_TARGET_DATES_AMOUNT = 90


class NmReportDetailStatus(Enum):
    SUCCESS = 200
    ERROR = 500
    IN_PROGRESS = 102

    def __repr__(self):
        return f"Status.{self.name}"


NM_REPORT_SCHEDUAL = '6 hours 15 minutes'


class taskNmReportDetail(TaskBase):
    task_class_identifier = "taskNmReportDetail"

    def __init__(self, db_handler, logger, store_id, api_token, last_run_time):
        super().__init__(
            db_handler,
            logger,
            store_id,
            api_token,
            last_run_time,
        )
        self.request_limiter = RequestLimiter(max_requests=3, per_seconds=60)

    def loading_simulation(self, date, page):
        return

    def get_next_to_load(self):
        query = f"""
            select * from 
            (WITH target_dates AS (
                SELECT
                    (DATE_TRUNC('day', generate_series(
                        ((CURRENT_TIMESTAMP - interval '{NM_REPORT_SCHEDUAL}') - INTERVAL '{NM_REPORT_DETAIL_TARGET_DATES_AMOUNT} days')::date,
                        ((CURRENT_TIMESTAMP - interval '{NM_REPORT_SCHEDUAL}') - INTERVAL '1 days')::date,
                        INTERVAL '1 day'
                    )))::date AS target_date
            ),
            store_info AS (
                SELECT
                    id,
                    fact_date,
                    page,
                    is_next_page,
                    cant_be_load
                FROM
                    {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_INFO_TABLE_NAME}
                WHERE
                    store_id = {self.store_id}
            )
            SELECT
                td.target_date,
                si.id,
                si.page,
                si.is_next_page,
                si.cant_be_load
            FROM
                target_dates td
            LEFT JOIN
                store_info si
            ON
                td.target_date = si.fact_date) m
            where m.is_next_page = true or m.is_next_page isnull
            limit 1
            """

        try:
            next_to_load_info = self.db_handler.execute_and_fetch_single_row(
                query)
            if next_to_load_info:
                return {
                    "status": NmReportDetailStatus.IN_PROGRESS,
                    "target_date": str(next_to_load_info.get("target_date")),
                    "target_id": next_to_load_info.get("id"),
                    "page": next_to_load_info.get("page"),
                    "is_next_page": next_to_load_info.get("is_next_page"),
                    "cant_be_load": next_to_load_info.get("cant_be_load"),
                }
            else:
                return {"status": NmReportDetailStatus.SUCCESS}
        except Exception as e:
            return {"status": NmReportDetailStatus.ERROR, "error": f"{e}"}

    def get_nm_report_detail_data(
        self,
        date,
        page,
    ):
        api_url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"

        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "period": {
                "begin": f"{date} 00:00:00",
                "end": f"{date} 23:59:59",
            },
            "orderBy": {
                "field": "openCard",
                "mode": "desc"
            },
            "page": page,
        }
        request_is_allowed = self.request_limiter.is_request_allowed()
        if not request_is_allowed:
            print("request is not allowed")
            return None

        try:
            response = requests.post(api_url,
                                     headers=headers,
                                     data=json.dumps(payload),
                                     verify=False)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                self.request_limiter.block_for_60_seconds()
                print("block_for_60_seconds")
                return None
            else:
                self.logger.error(
                    source="taskNmReportDetail",
                    message=
                    f"request error: {response.status_code}: {response.text}",
                    store_id=self.store_id,
                )
                return None

        except Exception as e:
            self.logger.error(
                source="taskNmReportDetail",
                message=f"request error: {e}",
                store_id=self.store_id,
            )
            return None

    def insert_nm_report_detail_data(self, data_list: list[dict]) -> str:
        if not data_list:
            return "No data to insert"

        try:
            start_time = time.time()
            temp_table_name = f"temp_{STG_NM_REPORT_DETAIL_TABLE_NAME}"
            create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table_name} (
                LIKE {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_TABLE_NAME} 
                INCLUDING DEFAULTS
            ) ON COMMIT DROP;
            """
            buf = StringIO()
            writer = csv.writer(buf, delimiter='\t')

            for item in data_list:
                row = (item["date"], self.store_id, item["mnID"],
                       item.get("openCardCount",
                                0), item.get("addToCartCount", 0),
                       item.get("ordersCount", 0), item.get("ordersSumRub", 0),
                       item.get("buyoutsCount",
                                0), item.get("buyoutsSumRub",
                                             0), item.get("cancelCount", 0),
                       item.get("cancelSumRub", 0), item.get("avgPriceRub", 0))
                writer.writerow(row)

            buf.seek(0)

            with self.db_handler.connection:
                with self.db_handler.connection.cursor() as cur:
                    cur.execute(create_temp_table_query)
                    cur.copy_from(buf,
                                  temp_table_name,
                                  sep='\t',
                                  columns=('date', 'store_id', 'nm_id',
                                           'open_card_count',
                                           'add_to_cart_count', 'orders_count',
                                           'orders_sum_rub', 'buyouts_count',
                                           'buyouts_sum_rub', 'cancel_count',
                                           'cancel_sum_rub', 'avg_price_rub'))

                    insert_query = f"""
                    INSERT INTO {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_TABLE_NAME}
                    SELECT * FROM {temp_table_name}
                    ON CONFLICT (date, store_id, nm_id) DO NOTHING;
                    """
                    cur.execute(insert_query)

                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                    inserted_count = cur.fetchone()[0]

            duration = time.time() - start_time
            print(
                f"-- insert_nm_report_detail_data -- \nInserted {inserted_count} records in {duration:.2f} seconds"
            )

            return f"Successfully inserted {inserted_count} records into {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_TABLE_NAME}"

        except Exception as e:
            if self.db_handler.connection:
                self.db_handler.connection.rollback()
            print(f"Error during insert operation: {str(e)}")
            raise

    def process_nm_report_detail_data(
        self,
        data,
        date,
    ):
        keys_to_include = [
            'openCardCount', 'addToCartCount', 'ordersCount', 'ordersSumRub',
            'buyoutsCount', 'buyoutsSumRub', 'cancelCount', 'cancelSumRub',
            'avgOrdersCountPerDay', 'avgPriceRub'
        ]

        result_cards = []
        try:
            if (data):
                cards = data["data"]["cards"]
                page = data["data"]["page"]
                is_next_page = data["data"]["isNextPage"]

                for c in cards:
                    mnID = c["nmID"]
                    selectedPeriod = c["statistics"]["selectedPeriod"]
                    fiterd_selectedPeriod = {
                        key: selectedPeriod[key]
                        for key in keys_to_include if key in selectedPeriod
                    }
                    statistic = {
                        **{
                            "date": date,
                            "mnID": mnID,
                        },
                        **fiterd_selectedPeriod
                    }
                    result_cards.append(statistic)

                return {
                    "cards": result_cards,
                    "page": page,
                    "is_next_page": is_next_page,
                }
            else:
                return None
        except:
            return None

    def process(self) -> TaskResponse:
        next_to_load_info = self.get_next_to_load()
        status = next_to_load_info.get("status")
        if status == NmReportDetailStatus.SUCCESS:
            self.status = TaskStatus.SUCCESS

        elif status == NmReportDetailStatus.IN_PROGRESS:
            target_date = next_to_load_info.get("target_date")
            target_id = next_to_load_info.get("target_id")
            page = next_to_load_info.get("page")
            is_next_page = next_to_load_info.get("is_next_page")
            cant_be_load = next_to_load_info.get("cant_be_load")

            if is_next_page == None:
                page = 1

                delete_query = f"""
                    DELETE FROM {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_TABLE_NAME}
                    WHERE store_id = {self.store_id} AND date = '{target_date}';
                """
                self.db_handler.execute_query(delete_query)
                data = self.get_nm_report_detail_data(
                    date=target_date,
                    page=page,
                )
                processed_data = self.process_nm_report_detail_data(
                    data=data,
                    date=target_date,
                )
                if processed_data:
                    is_next_page = processed_data.get("is_next_page")
                    cards = processed_data.get("cards")
                    self.insert_nm_report_detail_data(data_list=cards)
                    q = f"""
                        INSERT INTO {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_INFO_TABLE_NAME}  (store_id, page, is_next_page, cant_be_load, fact_date, created_at)
                        VALUES ({self.store_id}, {page}, {is_next_page}, FALSE, '{target_date}', CURRENT_TIMESTAMP);
                    """
                    self.db_handler.execute_query(q)
            elif is_next_page == True:
                page += 1
                data = self.get_nm_report_detail_data(
                    date=target_date,
                    page=page,
                )
                processed_data = self.process_nm_report_detail_data(
                    data=data,
                    date=target_date,
                )
                is_next_page = processed_data.get("is_next_page")
                cards = processed_data.get("cards")
                self.insert_nm_report_detail_data(data_list=cards)

                q = f"""
                    UPDATE {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_INFO_TABLE_NAME}
                    SET 
                        store_id = {self.store_id},
                        page = {page},
                        is_next_page = {is_next_page},
                        cant_be_load = FALSE,
                        fact_date = '{target_date}',
                        created_at = CURRENT_TIMESTAMP
                    WHERE id = {target_id};
                """
                self.db_handler.execute_query(q)

            else:
                self.logger.error(
                    source="taskNmReportDetail",
                    message=f"is_next_page has wrong value: {is_next_page}",
                    store_id=self.store_id,
                )

        elif status == NmReportDetailStatus.ERROR:
            self.logger.error(
                source="taskNmReportDetail",
                message=f"get_next_to_load error: ERROR",
                store_id=self.store_id,
            )
        else:
            self.logger.error(
                source="taskNmReportDetail",
                message=f"unknown status from get_next_to_load: {status}",
                store_id=self.store_id,
            )

        return self._make_response(f"")
