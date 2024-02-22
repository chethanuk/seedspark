/* **Selecting and Formatting Final Output** */
/* **Pre-calculating Weekend Trip Data** */
WITH "weekend_data" AS (
  SELECT
    toDayOfWeek("pickup_datetime") AS "day_of_week", /* Extracting Day of Week (6 for Saturday, 7 for Sunday) */
    formatDateTime("pickup_datetime", '%Y-%m') AS "year_month", /* Formatting Year and Month as a Single String */
    COUNT(*) AS "total_trips", /* Counting Total Weekend Trips */
    ROUND(COUNT(*) / COUNT(DISTINCT toDate("pickup_datetime")), 1) AS "avg_trips_per_day", /* Average Trips per Day (Total Trips / Unique Days) */
    ROUND(AVG("fare_amount"), 1) AS "avg_fare_per_trip", /* Average Fare per Trip (Rounded to One Decimal) */
    ROUND(AVG(DATE_DIFF(MINUTE, "pickup_datetime", "dropoff_datetime")), 1) AS "avg_trip_duration" /* Average Trip Duration in Minutes (Rounded to One Decimal) */
  FROM "tripdata"
  WHERE
    toDate("pickup_datetime") /* Filtering Date Range (2014-01-01 to 2016-12-31) */ BETWEEN '2014-01-01' AND '2016-12-31' /* Including Weekends Only (Saturday and Sunday) */
    AND toDayOfWeek("pickup_datetime") IN (6, 7)
  GROUP BY
    "day_of_week",
    "year_month"
)
SELECT
  "wd_saturday"."year_month",
  "wd_saturday"."avg_trips_per_day" AS "sat_avg_trip_count",
  "wd_saturday"."avg_fare_per_trip" AS "sat_avg_fare_per_trip",
  "wd_saturday"."avg_trip_duration" AS "sat_avg_trip_duration",
  "wd_sunday"."avg_trips_per_day" AS "sun_avg_trip_count",
  "wd_sunday"."avg_fare_per_trip" AS "sun_avg_fare_per_trip",
  "wd_sunday"."avg_trip_duration" AS "sun_avg_trip_duration"
FROM (
  SELECT
    *
  FROM "weekend_data"
  WHERE
    "day_of_week" = 6
) AS "wd_saturday"
JOIN (
  SELECT
    *
  FROM "weekend_data"
  WHERE
    "day_of_week" = 7
) AS "wd_sunday"
  ON "wd_saturday"."year_month" = "wd_sunday"."year_month"
ORDER BY
  "wd_saturday"."year_month"
