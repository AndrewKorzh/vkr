import random
import os
from typing import List
import time
from enum import Enum

from app.worker_db_handler import WorkerDBHandler
from app.worker_logger import WorkerLogger
from app.store_process import StoreProcess, StoreProcessStatus

from app.worker_public_config import (
    CORE_SCHEMA_NAME,
    STORE_TABLE_NAME,
    STORE_PROCESS_TABLE_NAME,
    SERVICE_HEALTH_TABLE_NAME,
)

SECONDS_TO_DO_HEALTH_CHECK = 60
SECONDS_FOR_NOT_VALID_STORE = 3600
SECONDS_FOR_NOT_VALID_HEALTH_CHECK = 1200
MAX_STORES_AMOUNT = 15


class Worker:

    def __init__(self):
        self.worker_id = os.getenv("WORKER", "worker_default")
        self.version = os.getenv("VERSION", "version_default")
        self.last_health_check = None
        self.db_handler = WorkerDBHandler()
        self.logger = WorkerLogger(
            db_handler=self.db_handler,
            worker=self.worker_id,
        )
        self.stores: List[StoreProcess] = []
        self.current_store_index = 0

    def get_and_update_user_info_from_db(self) -> dict | None:
        query = f"""
            WITH blocked_store AS (
                SELECT sp.store_process_id
                FROM {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME} sp

                WHERE 
                (
                    sp.last_data_load < NOW() - ({SECONDS_FOR_NOT_VALID_STORE} * INTERVAL '1 second')
                    OR sp.last_data_load IS NULL
                )
                AND (
                    (   
                        sp.process_health_check < NOW() - ({SECONDS_FOR_NOT_VALID_HEALTH_CHECK} * INTERVAL '1 second')
                        OR sp.process_health_check IS NULL
                    ) 
                        or
                    (
                        sp.running = False
                        or sp.running IS NULL
                    )
                )
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            ) UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME} sp
            SET
                running = true,
                process_health_check = NOW(),
                last_worker_start = NOW(),
                service = '{self.worker_id}'
            FROM blocked_store
            WHERE sp.store_process_id = blocked_store.store_process_id
            RETURNING sp.*;
        """
        try:
            result = self.db_handler.execute_and_fetch_single_row(query)
        except Exception as e:
            self.logger.error(
                "get_and_update_user_info_from_db",
                f"Ошибка при выполнении запроса: {str(e)}",
            )
            return None

        return result

    def mark_process_completed(self,
                               store_process_id: int,
                               data_loaded=False) -> dict | None:

        last_data_load_query = f"last_data_load = NOW()," if data_loaded else ""

        query = f"""
        UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME}
        SET 
            running = false,
            last_worker_end = NOW(),
            {last_data_load_query}
            process_health_check = NOW()
        WHERE store_process_id = %s
        RETURNING *;
        """
        try:
            result = self.db_handler.execute_and_fetch_single_row(
                query, (store_process_id, ))
            if result:
                self.db_handler.connection.commit()
            return result
        except Exception as e:
            self.db_handler.connection.rollback()
            raise e

    def get_store(self):
        store_info = self.get_and_update_user_info_from_db()
        if not store_info:
            print("-- no stores are available")
            return None

        store_id = store_info.get("store_id")
        store_process_id = store_info.get("store_process_id")

        if not store_id or not store_process_id:
            self.logger.error(
                "get_store",
                "None store_id or store_process_id",
                {"store_info": store_info},
            )
            return None

        store_data_query = f"""
            SELECT store_name, api_token, token_is_valid, secret_key
            FROM {CORE_SCHEMA_NAME}.{STORE_TABLE_NAME}
            WHERE store_id = %s
            LIMIT 1;
        """
        try:
            store_data = self.db_handler.execute_and_fetch_single_row(
                store_data_query,
                (store_id, ),
            )

            if not store_data:
                self.logger.error(
                    "get_store",
                    f"Магазин с ID {store_id} не найден",
                    {"store_id": store_id},
                )
                return None

            token_is_valid = store_data.get("token_is_valid")

            if not token_is_valid:
                self.mark_process_completed(
                    store_process_id,
                    data_loaded=True,
                )
                self.logger.error(
                    source="get_store",
                    message=f"TOKEN IS NOT VALID",
                    store_id=store_id,
                )
                return None

            return StoreProcess(
                store_id=store_id,
                store_process_id=store_process_id,
                store_name=store_data.get("store_name"),
                api_token=store_data.get("api_token"),
                secret_key=store_data.get("secret_key"),
                db_handler=self.db_handler,
                logger=self.logger,
            )

        except Exception as e:
            self.logger.error(
                "get_store",
                f"Error while getting store data: {str(e)}",
                {"store_id": store_id},
            )
            return None

    def update_stores(self):
        if len(self.stores) < MAX_STORES_AMOUNT:
            print("-- update_stores")
            store_object = self.get_store()
            if store_object:
                self.logger.info(
                    "update_stores",
                    f"add_store",
                    store_id=store_object.store_id,
                )
                self.stores.append(store_object)
        return False

    def update_worker_health_check(self) -> bool:
        query = f"""
            INSERT INTO {CORE_SCHEMA_NAME}.{SERVICE_HEALTH_TABLE_NAME} (
                service_type, service_name, version, last_health_check, updated_at
            )
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (service_type, service_name)
            DO UPDATE SET
                last_health_check = NOW(),
                updated_at = NOW(),
                version = COALESCE(EXCLUDED.version, {SERVICE_HEALTH_TABLE_NAME}.version)
            RETURNING id;
        """
        try:
            result = self.db_handler.execute_and_fetch_single_row(
                query, ("worker", self.worker_id, self.version))
            updated = bool(result)
            return updated

        except Exception as e:
            self.logger.error(
                source="worker",
                message=f"[health_check] Error while upserting: {str(e)}")
            return False

    def update_store_health_check(self) -> bool:
        if not self.stores:
            print("-- None available process for health_check")
            return True

        process_ids = [str(store.store_process_id) for store in self.stores]
        process_ids_str = ",".join(process_ids)

        query = f"""
            UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME}
            SET 
                process_health_check = NOW()
            WHERE 
                store_process_id IN ({process_ids_str})
                AND service = %s
            RETURNING store_process_id;
        """

        try:
            result = self.db_handler.execute_and_fetch_single_row(
                query,
                (self.worker_id, ),
            )
            updated_count = len(result) if result else 0
            print(
                f"Updated health_check for {updated_count} processes: {process_ids}"
            )
            return True

        except Exception as e:
            self.logger.error(
                "health_check",
                f"Error while health_check: {str(e)}",
                {"process_ids": process_ids},
            )
            return False

    def scedualed_health_check(self):
        current_time = time.time()

        if not self.last_health_check or current_time - self.last_health_check > SECONDS_TO_DO_HEALTH_CHECK:
            self.update_store_health_check()
            self.update_worker_health_check()
            self.last_health_check = current_time

    # возвращать статусы нормально
    def run_iteration(self):
        print("- worker iter start")
        self.scedualed_health_check()
        self.update_stores()

        stores_lengt = len(self.stores)
        if stores_lengt == 0:
            time.sleep(7.5)
            return "- stores_lengt is 0"

        store_index = self.current_store_index % stores_lengt
        self.current_store_index += 1
        store = self.stores[store_index]

        try:
            store_process_response = store.store_process_iter()
        except Exception as e:
            self.logger.error(
                source="run_iteration",
                store_id=store.store_id,
                message=f"error: {e}",
            )
            return f"Error: {e}"

        if store_process_response.status == StoreProcessStatus.SUCCESS or store_process_response.status == StoreProcessStatus.ERROR:
            del self.stores[store_index]
            try:
                self.mark_process_completed(
                    store.store_process_id,
                    data_loaded=True,
                )
            except Exception as e:
                self.logger.error(
                    source="run_iteration",
                    message=f"error: {e}",
                )

            self.logger.info(
                "run_iteration",
                f"delete_store",
                store_id=store.store_id,
            )
        return "- worker iter end"
