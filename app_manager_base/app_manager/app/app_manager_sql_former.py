from app.app_manager_public_config import (
    CORE_SCHEMA_NAME,
    STG_SCHEMA_NAME,
    DIM_SCHEMA_NAME,
    STORE_TABLE_NAME,
    STORE_PROCESS_TABLE_NAME,
    STG_NM_REPORT_DETAIL_TABLE_NAME,
    STG_CARDS_LIST_TABLE_NAME,
    STG_FACT_STOCK_TABLE_NAME,
    STG_FACT_SALES_TABLE_NAME,
    STG_ADVERT_INFO_TABLE_NAME,
    STG_ADVERT_STAT_TABLE_NAME,
    DIM_TECH_LIST_TABLE_NAME,
)


def select_query(store_id):
    query = f"""
    WITH 
    store_input AS (
        SELECT {store_id} AS store_id
    ),
    advert_base AS (
        SELECT 
            sas.date,
            sas.nm_id,
            sai.advert_type AS advert_type,
            SUM(sas.views) AS views,
            SUM(sas.clicks) AS clicks,
            SUM(sas.sum) AS sum,
            SUM(sas.atbs) AS atbs,
            SUM(sas.orders) AS orders,
            SUM(sas.shks) AS shks,
            SUM(sas.sum_price) AS sum_price
        FROM {STG_SCHEMA_NAME}.{STG_ADVERT_STAT_TABLE_NAME} sas
        JOIN {STG_SCHEMA_NAME}.{STG_ADVERT_INFO_TABLE_NAME} sai
            ON sas.store_id = sai.store_id AND sas.advert_id = sai.advert_id
        WHERE sas.store_id = (SELECT store_id FROM store_input)
        GROUP BY sas.date, sas.nm_id, sai.advert_type
    ), 
    store_nm_report as
        (select 
        snrd."date",
        snrd.nm_id,
        scl.vendor_code,
        snrd.store_id,
        snrd.open_card_count,
        snrd.add_to_cart_count,
        snrd.orders_count,
        snrd.orders_sum_rub
        from
            {STG_SCHEMA_NAME}.{STG_NM_REPORT_DETAIL_TABLE_NAME} snrd
            join {STG_SCHEMA_NAME}.{STG_CARDS_LIST_TABLE_NAME} scl
            on scl.nm_id = snrd.nm_id
            WHERE snrd.store_id = (SELECT store_id FROM store_input)
            AND snrd."date" >= CURRENT_DATE - INTERVAL '89 days'), 
    sales_fact as
        (SELECT
            "date",
            nm_id,
            COUNT(*) FILTER (WHERE sale_type = 'S') -
            COUNT(*) FILTER (WHERE sale_type = 'R') as fact_byouts_count,
            SUM(price_with_disc) AS fact_byouts_sum
        FROM {STG_SCHEMA_NAME}.{STG_FACT_SALES_TABLE_NAME}
        WHERE store_id = (SELECT store_id FROM store_input)
        GROUP BY "date", nm_id),
    stock_fact as 
        (select * from
            {STG_SCHEMA_NAME}.{STG_FACT_STOCK_TABLE_NAME} ssf
            WHERE store_id = (SELECT store_id FROM store_input)
    ),advert_data as ( SELECT
        date,
        nm_id,
        -- Тип 8: Авто
        SUM(views) FILTER (WHERE advert_type = 8) AS views_auto,
        SUM(clicks) FILTER (WHERE advert_type = 8) AS clicks_auto,
        SUM(sum) FILTER (WHERE advert_type = 8) AS sum_auto,
        SUM(atbs) FILTER (WHERE advert_type = 8) AS atbs_auto,
        SUM(orders) FILTER (WHERE advert_type = 8) AS orders_auto,
        SUM(shks) FILTER (WHERE advert_type = 8) AS shks_auto,
        SUM(sum_price) FILTER (WHERE advert_type = 8) AS price_auto,
        -- Тип 9: Поиск+Каталог
        SUM(views) FILTER (WHERE advert_type = 9) AS views_mix,
        SUM(clicks) FILTER (WHERE advert_type = 9) AS clicks_mix,
        SUM(sum) FILTER (WHERE advert_type = 9) AS sum_mix,
        SUM(atbs) FILTER (WHERE advert_type = 9) AS atbs_mix,
        SUM(orders) FILTER (WHERE advert_type = 9) AS orders_mix,
        SUM(shks) FILTER (WHERE advert_type = 9) AS shks_mix,
        SUM(sum_price) FILTER (WHERE advert_type = 9) AS price_mix,
        -- Тип 4: Каталог
        SUM(views) FILTER (WHERE advert_type = 4) AS views_cat,
        SUM(clicks) FILTER (WHERE advert_type = 4) AS clicks_cat,
        SUM(sum) FILTER (WHERE advert_type = 4) AS sum_cat,
        SUM(atbs) FILTER (WHERE advert_type = 4) AS atbs_cat,
        SUM(orders) FILTER (WHERE advert_type = 4) AS orders_cat,
        SUM(shks) FILTER (WHERE advert_type = 4) AS shks_cat,
        SUM(sum_price) FILTER (WHERE advert_type = 4) AS price_cat,
        -- Тип 5: Карточка
        SUM(views) FILTER (WHERE advert_type = 5) AS views_card,
        SUM(clicks) FILTER (WHERE advert_type = 5) AS clicks_card,
        SUM(sum) FILTER (WHERE advert_type = 5) AS sum_card,
        SUM(atbs) FILTER (WHERE advert_type = 5) AS atbs_card,
        SUM(orders) FILTER (WHERE advert_type = 5) AS orders_card,
        SUM(shks) FILTER (WHERE advert_type = 5) AS shks_card,
        SUM(sum_price) FILTER (WHERE advert_type = 5) AS price_card,
        -- Тип 6: Поиск
        SUM(views) FILTER (WHERE advert_type = 6) AS views_search,
        SUM(clicks) FILTER (WHERE advert_type = 6) AS clicks_search,
        SUM(sum) FILTER (WHERE advert_type = 6) AS sum_search,
        SUM(atbs) FILTER (WHERE advert_type = 6) AS atbs_search,
        SUM(orders) FILTER (WHERE advert_type = 6) AS orders_search,
        SUM(shks) FILTER (WHERE advert_type = 6) AS shks_search,
        SUM(sum_price) FILTER (WHERE advert_type = 6) AS price_search,
        -- Тип 7: Главная
        SUM(views) FILTER (WHERE advert_type = 7) AS views_main,
        SUM(clicks) FILTER (WHERE advert_type = 7) AS clicks_main,
        SUM(sum) FILTER (WHERE advert_type = 7) AS sum_main,
        SUM(atbs) FILTER (WHERE advert_type = 7) AS atbs_main,
        SUM(orders) FILTER (WHERE advert_type = 7) AS orders_main,
        SUM(shks) FILTER (WHERE advert_type = 7) AS shks_main,
        SUM(sum_price) FILTER (WHERE advert_type = 7) AS price_main
    FROM advert_base
    GROUP BY date, nm_id), 
    final_data as
    (select 
            store_nm_report.store_id,
            store_nm_report."date",
            store_nm_report.nm_id,
            store_nm_report.vendor_code,
            store_nm_report.open_card_count,
            store_nm_report.add_to_cart_count,
            store_nm_report.orders_count,
            store_nm_report.orders_sum_rub,
            sales_fact.fact_byouts_count,
            sales_fact.fact_byouts_sum,
            stock_fact.stock_count,
            stock_fact.to_client_count,
            stock_fact.from_client_count,
            ---- РЕКЛАМА ----
            -- Авто --
            advert_data.views_auto,
            advert_data.clicks_auto,
            advert_data.sum_auto,
            advert_data.atbs_auto,
            advert_data.orders_auto,
            advert_data.shks_auto,
            advert_data.price_auto,
            --  Поиск + Каталог --
            advert_data.views_mix,
            advert_data.clicks_mix,
            advert_data.sum_mix,
            advert_data.atbs_mix,
            advert_data.orders_mix,
            advert_data.shks_mix,
            advert_data.price_mix,
            -- Кампания в поиске (устаревший тип) --
            advert_data.views_search,
            advert_data.clicks_search,
            advert_data.sum_search,
            advert_data.atbs_search,
            advert_data.orders_search,
            advert_data.shks_search,
            advert_data.price_search,
            -- Кампания в каталоге (устаревший тип) --
            advert_data.views_cat,
            advert_data.clicks_cat,
            advert_data.sum_cat,
            advert_data.atbs_cat,
            advert_data.orders_cat,
            advert_data.shks_cat,
            advert_data.price_cat,
            -- Кампания в карточке товара (устаревший тип) --
            advert_data.views_card,
            advert_data.clicks_card,
            advert_data.sum_card,
            advert_data.atbs_card,
            advert_data.orders_card,
            advert_data.shks_card,
            advert_data.price_card,
            -- Кампания в рекомендациях на главной странице (устаревший тип) --
            advert_data.views_main,
            advert_data.clicks_main,
            advert_data.sum_main,
            advert_data.atbs_main,
            advert_data.orders_main,
            advert_data.shks_main,
            advert_data.price_main
        from store_nm_report
        left join sales_fact
            on sales_fact.nm_id = store_nm_report.nm_id
            and sales_fact."date" = store_nm_report."date"
        left join stock_fact
            on stock_fact.nm_id = store_nm_report.nm_id
            and stock_fact."date" = store_nm_report."date"
        left join advert_data
            on advert_data.nm_id = store_nm_report.nm_id
            and advert_data."date" = store_nm_report."date")
    select 
    *
    from final_data
    order by final_data."date", final_data.nm_id
    """
    return query


def insert_query(store_id):
    query = f"""
        delete from {DIM_SCHEMA_NAME}.{DIM_TECH_LIST_TABLE_NAME}
        where store_id = {store_id};
        
        INSERT INTO {DIM_SCHEMA_NAME}.{DIM_TECH_LIST_TABLE_NAME} (
        store_id, date, nm_id, vendor_code,
        open_card_count, add_to_cart_count, orders_count, orders_sum_rub,
        fact_byouts_count, fact_byouts_sum,
        stock_count, to_client_count, from_client_count,
        views_auto, clicks_auto, sum_auto, atbs_auto, orders_auto, shks_auto, price_auto,
        views_mix, clicks_mix, sum_mix, atbs_mix, orders_mix, shks_mix, price_mix,
        views_search, clicks_search, sum_search, atbs_search, orders_search, shks_search, price_search,
        views_cat, clicks_cat, sum_cat, atbs_cat, orders_cat, shks_cat, price_cat,
        views_card, clicks_card, sum_card, atbs_card, orders_card, shks_card, price_card,
        views_main, clicks_main, sum_main, atbs_main, orders_main, shks_main, price_main
        )
        SELECT * FROM ({select_query(store_id)}) AS sub;

        UPDATE {CORE_SCHEMA_NAME}.{STORE_PROCESS_TABLE_NAME}
        SET
            last_dm_etl = CURRENT_TIMESTAMP,
            running = FALSE
        WHERE store_id = {store_id};
    """
    return query
