{{
    config(
        materialized='incremental',
        unique_key='id',
	indexes=[
      {'columns': ['id'], 'unique': True}
    ]
    )
}}

SELECT
    document_id,
-- 2022/11/22 Using ingested_at time instead of synced_at time
    ingested_at,
    JSON_EXTRACT_SCALAR(document , "$.shop.id") as shop_id,
    document,
    cluster_time,
    FALSE AS legacy
FROM `keplr-datawarehouse.mongo_mon_marche.catalog_order` co
--WHERE NOT co.deleted
	{% if is_incremental() %}
	AND COALESCE(co.ingested_at,co.synced_at) >= (SELECT MAX(synced_at) FROM {{ this }} WHERE NOT legacy)
	{% endif %}