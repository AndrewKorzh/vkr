from utils.db_handler import AdminDBHandler

from admin_config import (
    CORE_SCHEMA_NAME,
    STG_SCHEMA_NAME,
    DIM_SCHEMA_NAME,
    STORE_TABLE_NAME,
    STORE_PROCESS_TABLE_NAME,
    LOG_TABLE_NAME,
    SERVICE_HEALTH_TABLE_NAME,
    STG_CARDS_LIST_TABLE_NAME,
    STG_NM_REPORT_DETAIL_INFO_TABLE_NAME,
    STG_NM_REPORT_DETAIL_TABLE_NAME,
    STG_FACT_STOCK_TABLE_NAME,
    STG_FACT_SALES_INFO_TABLE_NAME,
    STG_FACT_SALES_TABLE_NAME,
    STG_ADVERT_TYPE_MAPPING_TABLE_NAME,
    STG_ADVERT_INFO_TABLE_NAME,
    STG_ADVERT_LOAD_INFO_TABLE_NAME,
    STG_ADVERT_STAT_TABLE_NAME,
    DIM_TECH_LIST_TABLE_NAME,
)


def create_store_table(
    schema_name=CORE_SCHEMA_NAME,
    table_name=STORE_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    q = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            store_id INT PRIMARY KEY,
            store_name VARCHAR(255) NOT NULL UNIQUE,
            api_token TEXT NOT NULL UNIQUE,
            token_is_valid BOOLEAN NOT NULL,
            table_id TEXT NOT NULL UNIQUE,
            email VARCHAR(255) UNIQUE,
            phone VARCHAR(20) UNIQUE,
            secret_key TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
    """
    try:
        db_handler.execute_query(q)
        return f"Table {schema_name}.{table_name} created successfully!"
    except Exception as e:
        return f"Error while creating table: {str(e)}"


def create_service_health_table(
    schema_name=CORE_SCHEMA_NAME,
    table_name=SERVICE_HEALTH_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    q = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            service_type TEXT NOT NULL,
            service_name TEXT NOT NULL,
            version TEXT,
            last_health_check TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (service_type, service_name)
        );
    """
    try:
        db_handler.execute_query(q)
        return f"Table {schema_name}.{table_name} created successfully!"
    except Exception as e:
        return f"Error while creating table: {str(e)}"


def create_store_process_table(
    db_config,
    schema_name=CORE_SCHEMA_NAME,
    table_name=STORE_PROCESS_TABLE_NAME,
):
    db_handler = AdminDBHandler(db_config)

    q = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            store_process_id SERIAL PRIMARY KEY,
            store_id INT NOT NULL,
            running BOOLEAN NOT NULL DEFAULT FALSE,
            service TEXT,
            error TEXT,
            process_health_check TIMESTAMPTZ,
            last_worker_start TIMESTAMPTZ,
            last_worker_end TIMESTAMPTZ,
            last_data_load TIMESTAMPTZ,
            last_dm_etl TIMESTAMPTZ,
            last_client_load TIMESTAMPTZ, 
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
    """

    try:
        db_handler.execute_query(q)
        return f"Table {schema_name}.{table_name} created successfully."
    except Exception as e:
        print(e)
        return f"Error while creating table: {str(e)}"


def create_logs_table(
    schema_name=CORE_SCHEMA_NAME,
    table_name=LOG_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        log_id BIGSERIAL PRIMARY KEY,
        log_level VARCHAR(10) NOT NULL,
        service TEXT,
        store_id INT,
        source VARCHAR(255) NOT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        metadata JSONB
    );
    
    -- Индексы для быстрого поиска
    CREATE INDEX IF NOT EXISTS idx_log_level ON {schema_name}.{table_name} (log_level);
    CREATE INDEX IF NOT EXISTS idx_log_source ON {schema_name}.{table_name} (source);
    CREATE INDEX IF NOT EXISTS idx_log_created_at ON {schema_name}.{table_name} (created_at);
    """

    try:
        db_handler.execute_query(query)
        return f"Logs table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating logs table: {str(e)}"


def create_cards_list(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_CARDS_LIST_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        nm_id INTEGER NOT NULL,
        store_id INTEGER NOT NULL,
        vendor_code TEXT NOT NULL,
        title TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Индексы для ускорения поиска
    CREATE INDEX IF NOT EXISTS idx_product_nm_id ON {schema_name}.{table_name} (nm_id);
    CREATE INDEX IF NOT EXISTS idx_product_store_id ON {schema_name}.{table_name} (store_id);
    """

    try:
        db_handler.execute_query(query)
        return f"Product table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating product table: {str(e)}"


def create_nm_report_detail_info_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_NM_REPORT_DETAIL_INFO_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        store_id INTEGER NOT NULL,
        page INTEGER NOT NULL,
        is_next_page BOOLEAN NOT NULL,
        cant_be_load BOOLEAN NOT NULL,
        fact_date DATE NOT NULL,  -- Добавляем поле date в формате YYYY-MM-DD
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(store_id, fact_date)  -- Ограничение на уникальность для пары store_id и date
    );

    CREATE INDEX IF NOT EXISTS idx_nm_report_store_id ON {schema_name}.{table_name} (store_id);
    """

    try:
        db_handler.execute_query(query)
        return f"NM Report Detail Info table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating NM Report Detail Info table: {str(e)}"


def create_nm_report_detail_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_NM_REPORT_DETAIL_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        store_id INTEGER NOT NULL,
        nm_id INTEGER NOT NULL,
        open_card_count INTEGER,
        add_to_cart_count INTEGER,
        orders_count INTEGER,
        orders_sum_rub INTEGER,
        buyouts_count INTEGER,
        buyouts_sum_rub INTEGER,
        cancel_count INTEGER,
        cancel_sum_rub INTEGER,
        avg_price_rub INTEGER,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        
        -- Уникальное ограничение для работы ON CONFLICT
        CONSTRAINT uniq_stock_record UNIQUE (date, store_id, nm_id)
    );

    -- Индексы для ускорения выборок
    CREATE INDEX IF NOT EXISTS idx_detail_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_detail_nm_id ON {schema_name}.{table_name} (nm_id);
    CREATE INDEX IF NOT EXISTS idx_detail_date ON {schema_name}.{table_name} (date);
    """

    try:
        db_handler.execute_query(query)
        return f"Detail report table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating detail report table: {str(e)}"


def create_stock_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_FACT_STOCK_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        store_id INTEGER NOT NULL,
        nm_id INTEGER NOT NULL,
        stock_count INTEGER,
        to_client_count INTEGER,
        from_client_count INTEGER,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    -- Индексы для ускорения выборок
    CREATE INDEX IF NOT EXISTS idx_stock_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_stock_nm_id ON {schema_name}.{table_name} (nm_id);
    CREATE INDEX IF NOT EXISTS idx_stock_date ON {schema_name}.{table_name} (date);
    """

    try:
        db_handler.execute_query(query)
        return f"Stock report table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating stock report table: {str(e)}"


def create_fact_sales_info_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_FACT_SALES_INFO_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        store_id INTEGER PRIMARY KEY,
        last_change_date TEXT NOT NULL,
        is_final BOOLEAN NOT NULL,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    -- Индексы для ускорения выборок
    CREATE INDEX IF NOT EXISTS idx_sales_info_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_sales_info_last_change_date ON {schema_name}.{table_name} (last_change_date);
    """

    try:
        db_handler.execute_query(query)
        return f"Sales fact info table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating sales fact info table: {str(e)}"


def create_fact_sales_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_FACT_SALES_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        store_id INTEGER NOT NULL,
        sale_id TEXT NOT NULL UNIQUE,
        nm_id INTEGER NOT NULL,
        sale_type TEXT,
        date DATE NOT NULL,
        last_change_date TEXT NOT NULL,
        price_with_disc NUMERIC,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_sales_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_sales_nm_id ON {schema_name}.{table_name} (nm_id);
    CREATE INDEX IF NOT EXISTS idx_sales_date ON {schema_name}.{table_name} (date);
    """

    try:
        db_handler.execute_query(query)
        return f"Sales fact table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating sales fact table: {str(e)}"


def create_advert_type_mapping_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_ADVERT_TYPE_MAPPING_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        advert_type INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );

    INSERT INTO {schema_name}.{table_name} (advert_type, name) VALUES
        (4, 'кампания в каталоге (устаревший тип)'),
        (5, 'кампания в карточке товара (устаревший тип)'),
        (6, 'кампания в поиске (устаревший тип)'),
        (7, 'кампания в рекомендациях на главной странице (устаревший тип)'),
        (8, 'Автоматическая'),
        (9, 'Поиск+Каталог')
    ON CONFLICT (advert_type) DO UPDATE SET name = EXCLUDED.name;
    """

    try:
        db_handler.execute_query(query)
        return f"Advert type mapping table {schema_name}.{table_name} created and populated successfully."
    except Exception as e:
        return f"Error while creating/populating advert type mapping table: {str(e)}"


def create_advert_info_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_ADVERT_INFO_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        store_id INTEGER NOT NULL,
        advert_id INTEGER NOT NULL,
        advert_type INTEGER NOT NULL,
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        create_time TIMESTAMPTZ,
        change_time TIMESTAMPTZ,
        last_info_update_time TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (store_id, advert_id)
    );

    -- Индексы для ускорения запросов
    CREATE INDEX IF NOT EXISTS idx_advert_info_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_advert_info_advert_id ON {schema_name}.{table_name} (advert_id);
    CREATE INDEX IF NOT EXISTS idx_advert_info_advert_type ON {schema_name}.{table_name} (advert_type);
    CREATE INDEX IF NOT EXISTS idx_advert_info_time_range ON {schema_name}.{table_name} (start_time, end_time);
    """

    try:
        db_handler.execute_query(query)
        return f"Advert info table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating advert info table: {str(e)}"


def create_advert_load_info_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_ADVERT_LOAD_INFO_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        store_id INTEGER,
        advert_id INTEGER,
        date DATE,
        loaded BOOLEAN,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

    -- Индексы для ускорения запросов
    CREATE INDEX IF NOT EXISTS idx_advert_load_info_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_advert_load_info_advert_id ON {schema_name}.{table_name} (advert_id);
    CREATE INDEX IF NOT EXISTS idx_advert_load_info_date ON {schema_name}.{table_name} (date);
    """

    try:
        db_handler.execute_query(query)
        return f"Advert info table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating advert info table: {str(e)}"


def create_advert_stat_table(
    schema_name=STG_SCHEMA_NAME,
    table_name=STG_ADVERT_STAT_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        store_id INTEGER NOT NULL,
        advert_id INTEGER NOT NULL,
        app_type INTEGER NOT NULL,
        nm_id INTEGER NOT NULL,

        views INTEGER,
        clicks INTEGER,
        ctr NUMERIC,
        cpc NUMERIC,
        sum NUMERIC,
        atbs INTEGER,
        orders INTEGER,
        cr NUMERIC,
        shks INTEGER,
        sum_price NUMERIC,

        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

        UNIQUE (date, store_id, advert_id, app_type, nm_id)
    );

    -- Индексы для быстрого доступа
    CREATE INDEX IF NOT EXISTS idx_advert_stat_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_advert_stat_advert_id ON {schema_name}.{table_name} (advert_id);
    CREATE INDEX IF NOT EXISTS idx_advert_stat_app_type ON {schema_name}.{table_name} (app_type);
    CREATE INDEX IF NOT EXISTS idx_advert_stat_nm_id ON {schema_name}.{table_name} (nm_id);
    CREATE INDEX IF NOT EXISTS idx_advert_stat_date ON {schema_name}.{table_name} (date);
    """

    try:
        db_handler.execute_query(query)
        return f"Advert stat table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating advert stat table: {str(e)}"


def create_dim_tech_list_table(
    schema_name=DIM_SCHEMA_NAME,
    table_name=DIM_TECH_LIST_TABLE_NAME,
):
    db_handler = AdminDBHandler()
    query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        store_id INTEGER NOT NULL,
        date DATE NOT NULL,
        nm_id INTEGER NOT NULL,
        vendor_code TEXT,
        open_card_count INTEGER,
        add_to_cart_count INTEGER,
        orders_count INTEGER,
        orders_sum_rub NUMERIC,
        fact_byouts_count INTEGER,
        fact_byouts_sum NUMERIC,
        stock_count INTEGER,
        to_client_count INTEGER,
        from_client_count INTEGER,

        -- Автоматические кампании (тип 8)
        views_auto INTEGER,
        clicks_auto INTEGER,
        sum_auto NUMERIC,
        atbs_auto INTEGER,
        orders_auto INTEGER,
        shks_auto INTEGER,
        price_auto NUMERIC,

        -- Поиск + Каталог (тип 9)
        views_mix INTEGER,
        clicks_mix INTEGER,
        sum_mix NUMERIC,
        atbs_mix INTEGER,
        orders_mix INTEGER,
        shks_mix INTEGER,
        price_mix NUMERIC,


        -- Поиск (тип 6, устаревший)
        views_search INTEGER,
        clicks_search INTEGER,
        sum_search NUMERIC,
        atbs_search INTEGER,
        orders_search INTEGER,
        shks_search INTEGER,
        price_search NUMERIC,

        -- Каталог (тип 4, устаревший)
        views_cat INTEGER,
        clicks_cat INTEGER,
        sum_cat NUMERIC,
        atbs_cat INTEGER,
        orders_cat INTEGER,
        shks_cat INTEGER,
        price_cat NUMERIC,

        -- Карточка товара (тип 5, устаревший)
        views_card INTEGER,
        clicks_card INTEGER,
        sum_card NUMERIC,
        atbs_card INTEGER,
        orders_card INTEGER,
        shks_card INTEGER,
        price_card NUMERIC,

        -- Главная страница (тип 7, устаревший)
        views_main INTEGER,
        clicks_main INTEGER,
        sum_main NUMERIC,
        atbs_main INTEGER,
        orders_main INTEGER,
        shks_main INTEGER,
        price_main NUMERIC,


        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

        UNIQUE (store_id, date, nm_id)
    );

    -- Индексы для ускорения аналитических запросов
    CREATE INDEX IF NOT EXISTS idx_{table_name}_store_id ON {schema_name}.{table_name} (store_id);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_nm_id ON {schema_name}.{table_name} (nm_id);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {schema_name}.{table_name} (date);
    """

    try:
        db_handler.execute_query(query)
        return f"Store report table {schema_name}.{table_name} created successfully."
    except Exception as e:
        return f"Error while creating store report table: {str(e)}"
