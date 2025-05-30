import time
import os

from app.app_manager_public_config import (
    CORE_SCHEMA_NAME,
    STORE_TABLE_NAME,
    STORE_PROCESS_TABLE_NAME,
    SERVICE_HEALTH_TABLE_NAME,
)

from app.google_sheet_uploader import GoogleSheetUploader

from app.app_manager_sql_former import (insert_query)

from app.app_manager_db_hanler import AppManagerDBHandler
from app.app_manager_logger import AppManagerLogger

DATA_LOAD_SCHEDUAL = '6 hours 15 minutes'
DIM_ETL_SCHEDUAL = '6 hours 15 minutes'
PROCESS_BLOCK_HEALTH = '600 seconds'
SECONDS_TO_DO_HEALTH_CHECK = 60

VERSION = os.getenv("VERSION")
APP_MANAGER = os.getenv("APP_MANAGER")
DEFAULT_WB_TECH_TABLE_ID = os.getenv("DEFAULT_WB_TECH_TABLE_ID")
ENVIRONMENT = os.getenv("ENVIRONMENT")

SECONDS_FOR_NOT_VALID_HEALTH_CHECK = 1200


class AppManager:

    def __init__(self):
        self.db_handler = AppManagerDBHandler()
        self.logger = AppManagerLogger(self.db_handler, "app_manager")
        self.google_shet_uploader = GoogleSheetUploader(
            credentials_file="credentials.json",
            sheet_name="tech_list",
        )
        self.last_health_check = 0

    def insert_store_dim(self, store_id):
        try:
            query = insert_query(store_id=store_id)
            self.db_handler.execute_query(query)
        except Exception as e:
            self.logger.error("insert_store_dim", f"error: {e}", store_id)

    def fetch_and_lock_next_store_etl(self):
        query = f"""
            WITH next_store AS (
                SELECT *
                FROM {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME}
                WHERE
                    (
                        last_data_load IS NOT NULL
                        AND last_data_load >= (CURRENT_TIMESTAMP)::DATE + INTERVAL '{DATA_LOAD_SCHEDUAL}'
                    )
                    AND
                    (
                        (last_dm_etl IS NULL OR last_dm_etl < (CURRENT_TIMESTAMP)::DATE + INTERVAL '{DIM_ETL_SCHEDUAL}')
                    ) 
                    AND
                    (               
                        (   
                            process_health_check < NOW() - ({SECONDS_FOR_NOT_VALID_HEALTH_CHECK} * INTERVAL '1 second')
                            OR process_health_check IS NULL
                        ) 
                            or
                        (
                            running = False
                            or running IS NULL
                        )
                    )
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME} ss
            SET 
                process_health_check = CURRENT_TIMESTAMP,
                service = '{APP_MANAGER}_app_manager',
                running = true
            FROM next_store ns
            WHERE ss.store_id = ns.store_id
            RETURNING ns.*;
        """

        try:
            return self.db_handler.execute_and_fetch_single_row(query)
        except Exception as e:
            raise e

    def fetch_and_lock_next_store_table_load(self, ):
        """Забрать и заблокировать подходящую строку, обновив last_process_block"""
        query = f"""
            WITH next_store AS (
                SELECT *
                FROM {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME}
                WHERE
                    (
                        last_data_load IS NOT NULL
                        AND last_data_load >= (CURRENT_TIMESTAMP)::DATE + INTERVAL '{DATA_LOAD_SCHEDUAL}'
                    )
                    AND
                    (
                        (last_dm_etl IS NOT NULL OR last_dm_etl > (CURRENT_TIMESTAMP)::DATE + INTERVAL '{DIM_ETL_SCHEDUAL}')
                    ) 
                    AND
                    (
                        (last_client_load IS NULL OR last_client_load < (CURRENT_TIMESTAMP)::DATE + INTERVAL '{DIM_ETL_SCHEDUAL}')
                    )
                    AND
                    (               
                        (   
                            process_health_check < NOW() - ({SECONDS_FOR_NOT_VALID_HEALTH_CHECK} * INTERVAL '1 second')
                            OR process_health_check IS NULL
                        ) 
                            or
                        (
                            running = False
                            or running IS NULL
                        )
                    )
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME} ss
            SET 
                process_health_check = CURRENT_TIMESTAMP,
                service = '{APP_MANAGER}_app_manager',
                running = true
            FROM next_store ns
            WHERE ss.store_id = ns.store_id
            RETURNING ns.*;
        """
        try:

            return self.db_handler.execute_and_fetch_single_row(query)
        except Exception as e:
            raise e

    def store_table_load_finaly(self, store_id: int) -> None:
        try:
            query = f"""
                    UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME}
                    SET
                        running = false,
                        last_client_load = CURRENT_TIMESTAMP
                    WHERE store_id = {store_id};
                """

            self.db_handler.execute_query(query)

        except Exception as e:
            print(
                f"Failed to finalize store_table_load for store_id={store_id}: {e}"
            )

    def get_spreadsheet_id_by_store_id(self, store_id):
        query = f"""
            SELECT *
            FROM {CORE_SCHEMA_NAME}.{STORE_TABLE_NAME}
            WHERE
                store_id = {store_id}
            LIMIT 1
        """

        try:
            # print(query)
            return self.db_handler.execute_and_fetch_single_row(
                query)["table_id"]
        except Exception as e:
            raise e

    def update_app_manager_health_check(self) -> bool:
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
                query, (
                    "app_manager",
                    f"{APP_MANAGER}_app_manager",
                    f"{VERSION}",
                ))
            updated = bool(result)
            return updated

        except Exception as e:
            self.logger.error(
                source="worker",
                message=f"[health_check] Error while upserting: {str(e)}")
            return False

    def scedualed_health_check(self):
        current_time = time.time()

        if not self.last_health_check or current_time - self.last_health_check > SECONDS_TO_DO_HEALTH_CHECK:
            self.update_app_manager_health_check()
            self.last_health_check = current_time

    def run_iteration(self):
        try:
            self.scedualed_health_check()
            print("scedualed_health_check")
            next_etl_store = self.fetch_and_lock_next_store_etl()
            print(f"next_etl_store: {next_etl_store}")
            if next_etl_store:
                next_etl_store_id = next_etl_store["store_id"]
                self.insert_store_dim(store_id=next_etl_store_id)
                self.logger.info(
                    "insert_store_dim",
                    "data_inserted_to_dim",
                    next_etl_store_id,
                )
                print(next_etl_store_id)

            next_table_load_store = self.fetch_and_lock_next_store_table_load()
            if next_table_load_store:
                next_table_load_store_id = next_table_load_store["store_id"]

                spreadsheet_id = self.get_spreadsheet_id_by_store_id(
                    next_table_load_store_id, )

                if ENVIRONMENT == "dev":
                    spreadsheet_id = DEFAULT_WB_TECH_TABLE_ID

                upload_result = self.google_shet_uploader.upload_store_data(
                    spreadsheet_id=spreadsheet_id,
                    store_id=next_table_load_store_id,
                )
                if upload_result:
                    self.store_table_load_finaly(next_table_load_store_id)
                    self.logger.info(
                        "store_table_load",
                        "store_table_loaded_to_sheet",
                        next_table_load_store_id,
                    )
                print(next_table_load_store_id)

            if not next_etl_store and not next_table_load_store:
                time.sleep(10)
        except Exception as e:
            self.logger.error(source="app_manager", message=f"error: {e}")

        return "itreation_end"
