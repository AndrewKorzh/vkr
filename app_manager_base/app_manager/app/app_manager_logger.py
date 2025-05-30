from datetime import datetime
import json
from typing import Optional, Dict, Any
from datetime import datetime

from app.app_manager_db_hanler import AppManagerDBHandler
from app.app_manager_public_config import (CORE_SCHEMA_NAME, LOG_TABLE_NAME)


class AppManagerLogger:

    def __init__(self,
                 db_handler: AppManagerDBHandler,
                 app_manager: str,
                 schema_name: str = CORE_SCHEMA_NAME,
                 table_name: str = LOG_TABLE_NAME):

        self.db_handler = db_handler
        self.schema_name = schema_name
        self.table_name = table_name
        self.app_manager = app_manager

    def log(
        self,
        level: str,
        source: str,
        message: str,
        store_id: int = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):

        query = f"""
        INSERT INTO {self.schema_name}.{self.table_name} 
            (log_level, service, store_id, source, message, metadata)
        VALUES 
            (%s, %s, %s,%s, %s, %s)
        RETURNING log_id;
        """

        print(f"[{level.upper()} ({source})] {message}")

        try:
            metadata_json = json.dumps(metadata) if metadata else None
            self.db_handler.execute_query(
                query,
                (
                    level.upper(),
                    self.app_manager,
                    store_id,
                    source,
                    message,
                    metadata_json,
                ),
            )
            return True
        except Exception as e:
            print(f"Failed to write log to database: {str(e)}")
            return False

    def debug(self,
              source: str,
              message: str,
              store_id: int = None,
              metadata: Optional[Dict[str, Any]] = None):

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[{timestamp}] [DEBUG ({source})] store_id={store_id} {message} {metadata}"
        )

    def info(self,
             source: str,
             message: str,
             store_id: int = None,
             metadata: Optional[Dict[str, Any]] = None):
        self.log("INFO", source, message, store_id, metadata)

    def warning(self,
                source: str,
                message: str,
                store_id: int = None,
                metadata: Optional[Dict[str, Any]] = None):
        self.log("WARNING", source, message, store_id, metadata)

    def error(self,
              source: str,
              message: str,
              store_id: int = None,
              metadata: Optional[Dict[str, Any]] = None):
        self.log("ERROR", source, message, store_id, metadata)

    def critical(self,
                 source: str,
                 message: str,
                 store_id: int = None,
                 metadata: Optional[Dict[str, Any]] = None):
        self.log("CRITICAL", source, message, store_id, metadata)
