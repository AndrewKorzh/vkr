from app.app_manager_db_hanler import AppManagerDBHandler
from app.app_manager_logger import AppManagerLogger
from app.app_manager_public_config import (
    DIM_SCHEMA_NAME,
    DIM_TECH_LIST_TABLE_NAME,
)

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GoogleSheetUploader:

    def __init__(self, credentials_file: str, sheet_name: str):
        self.sheet_name = sheet_name
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets'])
            self.service = build('sheets', 'v4', credentials=self.credentials)
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize Google Sheets service: {e}")

        try:
            self.db_handler = AppManagerDBHandler()
            self.logger = AppManagerLogger(
                db_handler=self.db_handler,
                app_manager="app_manager_default",
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize database handler: {e}")

    def _check_access(self, spreadsheet_id):
        try:
            self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id).execute()
            print(f"[✓] Access to spreadsheet '{spreadsheet_id}' confirmed.")
        except HttpError as e:
            raise PermissionError(
                f"[✗] Cannot access spreadsheet '{spreadsheet_id}': {e}")
        except Exception as e:
            raise RuntimeError(
                f"[✗] Unexpected error while checking spreadsheet access: {e}")

    def _get_column_names(self, ignore_columns=[]):
        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{DIM_SCHEMA_NAME}' AND table_name = '{DIM_TECH_LIST_TABLE_NAME}'
        """
        try:
            columns = self.db_handler.fetch_all(query)
            return [c[0] for c in columns if c[0] not in ignore_columns]
        except Exception as e:
            raise RuntimeError(f"Failed to fetch column names: {e}")

    def _build_data_query(self,
                          store_id,
                          coalesce_ignore_columns,
                          ignore_columns=[]):
        columns = self._get_column_names(ignore_columns)
        select_columns = [
            f"COALESCE({col}, 0) AS {col}"
            if col not in coalesce_ignore_columns else col for col in columns
        ]
        column_str = ", ".join(select_columns)

        return f"""
            SELECT {column_str}
            FROM {DIM_SCHEMA_NAME}.{DIM_TECH_LIST_TABLE_NAME}
            WHERE store_id = {store_id}
        """

    def _ensure_sheet_exists(self, spreadsheet_id):
        try:
            sheets_metadata = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id).execute()
            sheet_titles = [
                s['properties']['title'] for s in sheets_metadata['sheets']
            ]

            if self.sheet_name not in sheet_titles:
                print(
                    f"[i] Sheet '{self.sheet_name}' not found. Creating it...")
                add_sheet_request = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': self.sheet_name
                            }
                        }
                    }]
                }
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=add_sheet_request).execute()
                print(f"[✓] Sheet '{self.sheet_name}' created.")
        except Exception as e:
            raise RuntimeError(
                f"Failed to verify or create sheet '{self.sheet_name}': {e}")

    def _clear_sheet(self, spreadsheet_id):
        try:
            self.service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, range=self.sheet_name).execute()
            print(f"[✓] Sheet '{self.sheet_name}' cleared.")
        except Exception as e:
            raise RuntimeError(
                f"Failed to clear sheet '{self.sheet_name}': {e}")

    def upload_store_data(self, spreadsheet_id: str, store_id: int):
        try:
            self._check_access(spreadsheet_id)

            query = self._build_data_query(
                store_id=store_id,
                coalesce_ignore_columns=["date", "vendor_code", "created_at"],
                ignore_columns=["id", "created_at"])
            data = self.db_handler.fetch_all_with_headers(query)

            if not data:
                print("[!] No data found to upload.")
                return False

            self._ensure_sheet_exists(spreadsheet_id)
            self._clear_sheet(spreadsheet_id)

            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=self.sheet_name,
                valueInputOption='RAW',
                body={
                    'values': data
                }).execute()

            print(
                f"[✓] Uploaded {len(data) - 1} rows to sheet '{self.sheet_name}' in spreadsheet '{spreadsheet_id}'."
            )
            return True

        except Exception as e:
            print(f"[✗] Failed to upload store data: {e}")
            self.logger.error(
                source="GoogleSheetUploader",
                message=f"error: {e}",
                store_id=store_id,
            )
            return False
