import time
from enum import Enum
from copy import deepcopy

import requests
import pandas as pd
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

from .task_base import (TaskBase, TaskStatus, TaskResponse, RequestLimiter)

from app.worker_public_config import STG_SCHEMA_NAME, STG_CARDS_LIST_TABLE_NAME, STG_FACT_STOCK_TABLE_NAME


class taskFactStock(TaskBase):

    def __init__(self, db_handler, logger, store_id, api_token, last_run_time):
        super().__init__(
            db_handler,
            logger,
            store_id,
            api_token,
            last_run_time,
        )
        self.request_limiter = RequestLimiter(max_requests=3, per_seconds=60)

    task_class_identifier = "taskFactStock"

    def get_fact_stock_data(self, date):
        api_url = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/products/products"

        end_date = date
        start_date = date

        stockType = ""
        skipDeletedNm = False
        orderBy_field = "stockCount"
        orderBy_mode = "desc"
        availabilityFilters = [
            "actual", "balanced", "balanced", "nonActual", "nonLiquid",
            "invalidData"
        ]
        nmIDs = None
        subjectID = None
        brandName = None
        tagID = None
        limit = 1000
        offset = 0

        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "nmIDs": nmIDs,
            "subjectID": subjectID,
            "brandName": brandName,
            "tagID": tagID,
            "currentPeriod": {
                "start": start_date,
                "end": end_date
            },
            "stockType": stockType,
            "skipDeletedNm": skipDeletedNm,
            "orderBy": {
                "field": orderBy_field,
                "mode": orderBy_mode
            },
            "availabilityFilters": availabilityFilters or ["actual"],
            "limit": limit,
            "offset": offset
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        request_is_available = self.request_limiter.is_request_allowed()

        if not request_is_available:
            print("request is not available")
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
                print("request is blocked")
            else:
                self.logger.error(
                    source="taskFactStock",
                    message=
                    f"request error: {response.status_code}: {response.text}",
                    store_id=self.store_id,
                )
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(
                source="taskFactStock",
                message=f"request error: {e}",
                store_id=self.store_id,
            )
            return None

    def process_stock_data(self, data, date):
        items = data['data']['items']
        res_data = [{
            "date": date,
            "nmID": d["nmID"],
            "store_count": d["metrics"]["stockCount"],
            "to_client_count": d["metrics"]["toClientCount"],
            "from_client_count": d["metrics"]["fromClientCount"],
        } for d in items]
        return res_data

    def insert_stock_data(self, stock_data: list) -> str:
        query = f"""
            INSERT INTO {STG_SCHEMA_NAME}.{STG_FACT_STOCK_TABLE_NAME} (date, store_id, nm_id, stock_count, to_client_count, from_client_count)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        try:
            data = [(
                item["date"],
                self.store_id,
                item["nmID"],
                item["store_count"],
                item["to_client_count"],
                item["from_client_count"],
            ) for item in stock_data]

            self.db_handler.execute_many(query, data)
            return f"Successfully inserted {len(stock_data)} rows into {STG_SCHEMA_NAME}.{STG_FACT_STOCK_TABLE_NAME}"
        except Exception as e:
            self.logger.error(
                source="taskFactStock",
                message=f"Error while inserting stock data: {str(e)}",
                store_id=self.store_id,
            )
            return None

    def get_fact_stock_status_info(self):
        q = f""" 
            SELECT
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM {STG_SCHEMA_NAME}.{STG_FACT_STOCK_TABLE_NAME} sfs
                        WHERE sfs.date = CURRENT_DATE - INTERVAL '1 day' AND store_id = {self.store_id}
                    )
                    THEN 'ok'
                    ELSE 'need_load'
                END AS status,
                TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYY-MM-DD') AS target_date;
            """
        return self.db_handler.execute_and_fetch_single_row(query=q)

    def process(self):

        status_info = self.get_fact_stock_status_info()
        status = status_info["status"]
        target_date = status_info['target_date']

        if status == "need_load":
            data = self.get_fact_stock_data(target_date)
            processed_data = self.process_stock_data(data, target_date)
            insert_result = self.insert_stock_data(processed_data)
            if insert_result:
                self.status = TaskStatus.SUCCESS
                return self._make_response(f"insert_result: {insert_result}")
            else:
                self.status = TaskStatus.ERROR
                return self._make_response(f"insert_result: {insert_result}")
        elif status == "ok":
            self.status = TaskStatus.SUCCESS
            return self._make_response(f"data already inserted")
        else:
            self.logger.error(
                source="taskFactStock",
                message=f"unknown status, status_info: {status_info}",
                store_id=self.store_id,
            )
            self.status = TaskStatus.ERROR
            return self._make_response(
                f"unknown status, status_info: {status_info}")

        print()
