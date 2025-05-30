from copy import deepcopy

import requests
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

from .task_base import (
    TaskBase,
    TaskStatus,
    TaskResponse,
)

from app.worker_public_config import STG_SCHEMA_NAME, STG_CARDS_LIST_TABLE_NAME

CARDS_LIST_UPDATE_SCEDUAL = '6 hours 15 minutes'
CARDS_LIST_API_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
CARDS_LIST_API_LIMIT = 100
CARDS_LIST_API_PAYLOAD = {
    "settings": {
        "cursor": {
            "limit": CARDS_LIST_API_LIMIT
        },
        "filter": {
            "withPhoto": -1
        }
    }
}


class taskCardsList(TaskBase):
    task_class_identifier = "taskCardsList"

    def __init__(self, db_handler, logger, store_id, api_token, last_run_time):
        super().__init__(
            db_handler,
            logger,
            store_id,
            api_token,
            last_run_time,
        )

    def get_cards_list_data(self):
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

        payload = deepcopy(CARDS_LIST_API_PAYLOAD)
        cards_list_data = []
        while True:
            try:
                response = requests.post(
                    CARDS_LIST_API_URL,
                    headers=headers,
                    data=json.dumps(payload),
                    verify=False,
                )
                if response.status_code == 200:
                    data = response.json()
                    updatedAt = data.get("cursor", {}).get("updatedAt")
                    nmID = data.get("cursor", {}).get("nmID")
                    payload["settings"]["cursor"]["nmID"] = nmID
                    payload["settings"]["cursor"]["updatedAt"] = updatedAt
                    total = data.get("cursor", {}).get("total", 0)
                    if data.get("cards") and len(data.get("cards")) > 0:
                        cards_list_data.extend(data["cards"])
                    if total < CARDS_LIST_API_LIMIT:
                        break

                else:
                    self.raise_error(
                        f"request error: {response.status_code}: {response.text}",
                    )

            except Exception as e:
                self.raise_error(f"error: {e}", )
        return cards_list_data

    def get_cards_list_status_report(self):
        query = f"""
            select
                count(case when (cl.created_at)::DATE >= (CURRENT_TIMESTAMP - INTERVAL '{CARDS_LIST_UPDATE_SCEDUAL}')::DATE then 1 end) as actual,
                count(*) as count_all
            FROM {STG_SCHEMA_NAME}.{STG_CARDS_LIST_TABLE_NAME} cl
            WHERE cl.store_id = {self.store_id};
        """
        try:
            return self.db_handler.execute_and_fetch_single_row(query)
        except Exception as e:
            self.raise_error(f"error: {e}")

    def process_cards_list_data(self, cards_list_data):
        cards = []
        for card in cards_list_data:
            cards.append({
                "nm_id": card.get("nmID"),
                "store_id": self.store_id,
                "vendor_code": card.get("vendorCode"),
                "title": card.get("title"),
            })
        return cards

    # Возвращать статусы
    def insert_cards(self, cards) -> str:
        query = f"""
            INSERT INTO {STG_SCHEMA_NAME}.{STG_CARDS_LIST_TABLE_NAME} (nm_id, store_id, vendor_code, title)
            VALUES (%s, %s, %s, %s);
        """
        try:
            data = [(
                product["nm_id"],
                product["store_id"],
                product["vendor_code"],
                product["title"],
            ) for product in cards]

            self.db_handler.execute_many(query, data)

            return f"Successfully inserted/updated {len(cards)} cards in {STG_SCHEMA_NAME}.{STG_CARDS_LIST_TABLE_NAME}"
        except Exception as e:
            self.raise_error(f"Error while inserting cards: {str(e)}")

    def process(self) -> TaskResponse:
        cards_list_status_report = self.get_cards_list_status_report()

        if cards_list_status_report:
            actual = cards_list_status_report.get("actual")
            count_all = cards_list_status_report.get("count_all")

            if count_all == 0:
                print(f"No data for store {self.store_id} -> load")
            elif count_all != actual:
                self.logger.info(
                    "taskCardsList",
                    f"Not actual data found -> delete and load",
                    store_id=self.store_id,
                )
                delete_query = f"""
                    DELETE FROM {STG_SCHEMA_NAME}.{STG_CARDS_LIST_TABLE_NAME}
                    WHERE store_id = {self.store_id};
                    """
                try:
                    self.db_handler.execute_query(delete_query)
                except Exception as e:
                    self.raise_error(f"store delete error: {e}")
            else:
                self.status = TaskStatus.SUCCESS
                return self._make_response(
                    f"Cards list loaded already: {cards_list_status_report}")

        else:
            self.status = TaskStatus.ERROR
            self._make_response(f"cards_list_status_report error")
            return

        # TODO try exept
        cards_list_data = self.get_cards_list_data()
        if cards_list_data:
            cards = self.process_cards_list_data(
                cards_list_data=cards_list_data)
            insert_result = self.insert_cards(cards)
            self.status = TaskStatus.SUCCESS
        else:
            self.raise_error(f"cards_list_data is null")
            self.status = TaskStatus.IN_PROGRESS
        return self._make_response(f"insert_result: {insert_result}")
