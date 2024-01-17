SELECT VendorID, AVG(fare_amount) AS average_fare
FROM data-twinkle.uber_dataset_bq.fact_table
GROUP BY VendorID;




SELECT b.payment_type_name, AVG(a.tip_amount) as average_tip_amount FROM `uber_dataset_bq.fact_table` as a
JOIN `uber_dataset_bq.payment_type_dim` as b 
ON a.payment_type_id = b.payment_type_id
GROUP BY b.payment_type_name;

SELECT a.pickup_location_id, COUNT(*) as trip_count, AVG(a.tip_amount) as average_tip_amount
FROM `uber_dataset_bq.fact_table` as a
LEFT JOIN `uber_dataset_bq.pickup_location_dim` as b
ON a.pickup_location_id = b.pickup_location_id
GROUP BY a.pickup_location_id
ORDER BY average_tip_amount asc;

CREATE OR REPLACE TABLE `data-twinkle.uber_dataset_bq.analytictable` AS (
SELECT 
    f.trip_id,
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pick.pickup_latitude,
    pick.pickup_longitude,
    drop.dropoff_latitude,
    drop.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
FROM data-twinkle.uber_dataset_bq.fact_table f
JOIN data-twinkle.uber_dataset_bq.datetime_dim d ON f.datetime_id = d.datetime_id
JOIN data-twinkle.uber_dataset_bq.passenger_count_dim p ON p.passenger_count_id = f.passenger_count_id  
JOIN data-twinkle.uber_dataset_bq.trip_distance_dim t ON t.trip_distance_id = f.trip_distance_id  
JOIN data-twinkle.uber_dataset_bq.rate_code_dim r ON r.rate_code_id = f.rate_code_id  
JOIN data-twinkle.uber_dataset_bq.pickup_location_dim pick ON pick.pickup_location_id = f.pickup_location_id
JOIN data-twinkle.uber_dataset_bq.dropoff_location_dim drop ON drop.dropoff_location_id = f.dropoff_location_id
JOIN data-twinkle.uber_dataset_bq.payment_type_dim pay ON pay.payment_type_id = f.payment_type_id );
