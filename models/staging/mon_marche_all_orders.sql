{{
    config(
        materialized='incremental',
        unique_key='document_id',
	indexes=[
      {'columns': ['document_id'], 'unique': True}
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
WHERE 
	{% if is_incremental() %}
	 ingested_at >= (SELECT MAX(ingested_at) FROM {{ this }} )
	{% endif %}
