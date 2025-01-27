### Q1

```
docker run -it --entrypoint bash python:3.12.8

pip --version
```

Ans: pip 24.3.1

### Q3

run `docker-compose.yaml`
```
docker-compose up -d
```

load green taxi trips from October 2019
```
python3 load_data.py \
    --user root \
    --password root \
    --host localhost \
    --port 5432 \
    --db ny_taxi \
    --table green_taxi_data \
    --url https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet
```

load zones data
```
python3 load_data.py \
    --user root \
    --password root \
    --host localhost \
    --port 5432 \
    --db ny_taxi \
    --table zones \
    --url https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

```

SQL
```
SELECT 
    CASE 
        WHEN trip_distance <= 1 THEN 'Up to 1 mile'
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1 to 3 miles'
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3 to 7 miles'
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7 to 10 miles'
        ELSE 'Over 10 miles'
    END AS distance_category,
    COUNT(*) AS total_trips
FROM 
    green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01' 
    AND lpep_pickup_datetime < '2019-11-01'
    AND lpep_dropoff_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
GROUP BY 
    distance_category;
```

### Q4

```
WITH daily_max_distance AS (
    SELECT 
        DATE(lpep_pickup_datetime) AS pickup_day,
        MAX(trip_distance) AS max_distance
    FROM 
        green_taxi_data
    GROUP BY 
        DATE(lpep_pickup_datetime)
)
SELECT 
    pickup_day,
    max_distance
FROM 
    daily_max_distance
ORDER BY 
    max_distance DESC
LIMIT 1;
```

### Q5
```
SELECT 
    z."Zone" AS pickup_location,
    SUM(g."total_amount") AS total_amount
FROM 
    green_taxi_data g
JOIN 
    zones z
ON 
    g."PULocationID" = z."LocationID"
WHERE 
    DATE(g."lpep_pickup_datetime") = '2019-10-18'
GROUP BY 
    z."Zone"
HAVING 
    SUM(g."total_amount") > 13000
ORDER BY 
    total_amount DESC;
```