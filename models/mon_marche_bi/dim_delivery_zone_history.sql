-- There is only 'Ile de la cité' on mongo_mon_marche, this is was there is not no Rungis data. This can be changed. 
-- When the validity starts in Novembre / December - this is when the history records where initially send from mongo to mm. 

WITH delivery_zone AS (
SELECT
	cdz.id AS delivery_zone_id,
	cdz."document" ->> 'type' AS delivery_zone_type,
	cdz."document" ->> 'name' AS delivery_zone_name,
	cdz."document" ->> 'shopId' AS shop_id,
	(cdz."document" ->> 'enabled')::BOOL AS is_enabled,
	cdz.updated_at as syncro 
FROM {{ source('mongo_mon_marche', 'catalog_delivery_zone') }} cdz 
WHERE NOT cdz.deleted 
),
delivery_zone_status_change AS (
SELECT 
	dz.delivery_zone_id,
	dz.delivery_zone_type,
	dz.delivery_zone_name,
	dz.shop_id,
	dz.syncro AS validity_start_at,
	dz.is_enabled,
	COALESCE(LAG(dz.is_enabled) OVER(PARTITION BY dz.delivery_zone_id,dz.shop_id ORDER BY dz.syncro) = dz.is_enabled,
		FALSE) AS same_status_as_before
FROM delivery_zone dz
),
delivery_zone_deleted_event AS (
SELECT
    cdz.id AS delivery_zone_id,
    cdz.synced_at::TIMESTAMP AT TIME ZONE 'Etc/UTC' AT TIME ZONE 'Europe/Paris' AS deleted_at
FROM {{ source('mongo_mon_marche', 'catalog_delivery_zone') }} cdz
WHERE cdz.deleted
)
SELECT
	dzc .delivery_zone_id,
	dzc.delivery_zone_type,
	dzc.delivery_zone_name,
	dzc.shop_id,
	case when date(dzc.validity_start_at)='2020-12-03' then '2020-03-12' else date(dzc.validity_start_at) end as validity_start_at,
        least(
	    COALESCE(
	        LEAD(date(dzc.validity_start_at)) OVER(PARTITION BY dzc.delivery_zone_id,dzc.shop_id ORDER BY dzc.validity_start_at),
	        date(CURRENT_DATE AT TIME ZONE 'Europe/Paris' + INTERVAL '1 day' )
	    ),
	    dzde.deleted_at
	) AS validity_end_at,
	LEAD(dzc.validity_start_at) OVER(PARTITION BY dzc.delivery_zone_id,dzc.shop_id ORDER BY dzc.validity_start_at) IS NULL AS is_last_status,
	dzc.is_enabled,
	dzde.deleted_at IS NOT NULL AS is_deleted
FROM delivery_zone_status_change dzc
LEFT JOIN delivery_zone_deleted_event dzde ON dzde.delivery_zone_id = dzc.delivery_zone_id
WHERE NOT dzc.same_status_as_before 

