-- 1. How many stores does the business have and in which country?
-- The Operations team would like to know which countries we currently operate in and which country now has the most stores.

SELECT
        country_code AS country,
        COUNT(country_code) AS total_no_stores
FROM
        dim_store_details
GROUP BY
        country_code
ORDER BY
        total_no_stores DESC;


-- 2. Which locations currently have the most stores?
-- The business stakeholders would like to know which locations currently have the most stores.
-- They would like to close some stores before opening more in other locations.
-- Find out which locations have the most stores currently.

SELECT
        locality,
        COUNT(store_code) AS total_no_stores
FROM
        dim_store_details
GROUP BY
        locality
ORDER BY
        total_no_stores DESC,
        locality
LIMIT 7;


-- 3. Which months produce the largest amounts of sales?
-- Query the database to find out which months have produced the most sales.

-- The orders_table has all the orders made. Each has a date_uuid.
-- In the dim_date_times table the month is logged for each date_uuid.
-- The date_uuid foreign key corresponds to the primary key in the dim_date_times table.
-- The orders_table has the product_code and product_quantity
-- The product_code joins onto the dim_products table which has the price of the product

WITH cte AS (
        SELECT
                month,
                (dim_products.product_price * orders_table.product_quantity) AS total_payment_taken
        FROM
                orders_table
        LEFT JOIN
                dim_products ON orders_table.product_code = dim_products.product_code
        INNER JOIN
                dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)
SELECT
        ROUND(SUM(total_payment_taken)::numeric,2) AS total_sales,
        month
FROM
        cte
GROUP BY
        month
ORDER BY
        total_sales DESC
LIMIT 6;


-- 4. The company is looking to increase its online sales.
-- They want to know how many sales are happening online vs offline.
-- Calculate how many products were sold and the amount of sales made for online and offline purchases.

SELECT
    COUNT(date_uuid) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE WHEN store_code IN (
        SELECT
            store_code
        FROM
            dim_store_details
        WHERE
            locality IS NULL
    ) THEN 'Web' ELSE 'Offline' END AS location
FROM
    orders_table
GROUP BY
    location;

--5. What percentage of sales come through each type of store?
-- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
-- Find out the total and percentage of sales coming from each of the different store types.

WITH revenue_per_store AS (
    SELECT
            SUM(dim_products.product_price * orders_table.product_quantity) AS total_revenue,
            store_code
    FROM
            orders_table
    LEFT JOIN
            dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY
            store_code
), revenue_by_store_type AS (
        SELECT
                store_type,
                ROUND(SUM(total_revenue)::numeric,2) as total_sales_per_store
        FROM
                revenue_per_store
        LEFT JOIN
                dim_store_details ON revenue_per_store.store_code = dim_store_details.store_code
        GROUP BY
                store_type
)
SELECT
        store_type, total_sales_per_store AS total_sales,
        ROUND(((total_sales_per_store/(SELECT SUM(total_sales_per_store) FROM revenue_by_store_type))*100)::numeric,2) AS "percentage_total(%)"
FROM
        revenue_by_store_type
GROUP BY
        store_type,
        total_sales
ORDER BY
        total_sales DESC;


-- 6. Which month in which year produced the highest cost of sales?
-- The company stakeholders want assurances that the company has been doing well recently.
-- Find which months in which years have had the most sales historically.

SELECT
        SUM((ROUND(dim_products.product_price::numeric,2) * orders_table.product_quantity)) AS total_sales,
        year,
        month
FROM
        orders_table
JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
JOIN
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
        year, month
ORDER BY
        total_sales DESC
LIMIT 10;


-- 7. What is the staff headcount?
-- The operations team would like to know the overall staff numbers in each location around the world.
-- Perform a query to determine the staff numbers in each of the countries the company sells in.

SELECT
        SUM(staff_numbers) AS total_staff_numbers,
        country_code
FROM
        dim_store_details
GROUP BY
        country_code
ORDER BY
        total_staff_numbers DESC;


-- 8. Which German store is selling the most?
-- The sales team is looking to expand their territory in Germany.
-- Determine which type of store is generating the most sales in Germany

SELECT
        ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric,2) AS total_sales,
        store_type,
        country_code
FROM
        orders_table
LEFT JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
LEFT JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE
        country_code = 'DE'
GROUP BY
        store_type,
        country_code
ORDER BY
        total_sales;


-- 9. How quickly is the company making sales?
-- Sales would like to get an accurate metric for how quickly the company is making sales.
-- Determine the average time taken between each sale grouped by year.

WITH lead_datetimes AS (
    SELECT
            year,
            datetime,
            LEAD(datetime, 1) OVER (
                    PARTITION BY year
                    ORDER BY datetime
            ) AS next_datetime
    FROM
            dim_date_times
)
SELECT
        year,
        AVG(next_datetime - datetime) AS actual_time_taken
FROM
        lead_datetimes
GROUP BY
        year
ORDER BY
        actual_time_taken DESC;

-- 9a. Trying the above using the original separate columns - same results

WITH datetimes AS (
    SELECT
            year,
            ((make_date(CAST(year as integer), CAST(month as integer), CAST(day as integer))) + CAST(timestamp AS time)) as datetime
    FROM
            dim_date_times
), lead_datetimes AS (
    SELECT
            year,
            datetime,
            LEAD(datetime, 1) OVER (
                    PARTITION BY year
                    ORDER BY datetime
            ) AS next_datetime
    FROM
            datetimes
)
SELECT
        year,
        AVG(next_datetime - datetime) AS actual_time_taken
FROM
        lead_datetimes
GROUP BY
        year
ORDER BY
        actual_time_taken DESC;
