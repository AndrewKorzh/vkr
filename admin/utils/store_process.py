import requests
import uuid
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

from utils.db_handler import AdminDBHandler


def check_token_validity(api_token):
    try:
        headers = {'Authorization': api_token}
        response = requests.get(
            'https://common-api.wildberries.ru/ping',
            headers=headers,
            timeout=10,
            verify=False,
        )

        response.raise_for_status()
        data = response.json()

        return data.get('Status') == 'OK'

    except (requests.exceptions.RequestException, ValueError):
        return False


def generate_uuid():
    uuid_generated = str(uuid.uuid4())
    return uuid_generated


def insert_store(db_config,
                 schema_name,
                 table_name,
                 store_id,
                 store_name,
                 api_token,
                 secret_key,
                 table_id,
                 email=None,
                 phone=None):
    db_handler = AdminDBHandler(db_config=db_config)

    token_is_valid = check_token_validity(api_token)

    q = f"""
        INSERT INTO {schema_name}.{table_name} (store_id, store_name, api_token, token_is_valid, secret_key, table_id, email, phone)
        VALUES (%s, %s, %s, %s, %s, %s,%s, %s)
    """
    params = (
        store_id,
        store_name,
        api_token,
        token_is_valid,
        secret_key,
        table_id,
        email,
        phone,
    )

    try:
        db_handler.execute_query(q, params)
        return {
            "status": "ok",
            "secret_key": secret_key,
            "token_is_valid": token_is_valid,
        }
    except Exception as e:
        return {
            "status": "error",
            "error": e,
        }


def populate_store_process_table(
    db_config,
    schema_name,
    store_table_name,
    store_process_table_name,
):
    db_handler = AdminDBHandler(db_config=db_config)

    find_missing_users_query = f"""
        SELECT s.store_id
        FROM {schema_name}.{store_table_name} s
        LEFT JOIN {schema_name}.{store_process_table_name} sp
            ON s.store_id = sp.store_id
        WHERE sp.store_id IS NULL;
    """

    try:
        missing_users = db_handler.fetch_all(find_missing_users_query)

        if not missing_users:
            return "No missing users found."

        insert_query = f"""
            INSERT INTO {schema_name}.{store_process_table_name} (store_id, running)
            VALUES (%s, FALSE);
        """
        params = [(user[0], ) for user in missing_users]
        db_handler.execute_many(insert_query, params)

        return f"Inserted {len(missing_users)} missing users into {schema_name}.{store_process_table_name}."
    except Exception as e:
        return f"Error while populating table: {str(e)}"
