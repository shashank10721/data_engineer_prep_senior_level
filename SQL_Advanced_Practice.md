# 🎯 SQL Advanced Practice for Senior Data Engineers
## 100+ Interview Questions with Solutions

---

## 📋 Quick Navigation
1. [Basic to Intermediate](#basic-to-intermediate)
2. [Window Functions](#window-functions)
3. [Complex Joins](#complex-joins)
4. [Subqueries & CTEs](#subqueries-ctes)
5. [Date/Time Operations](#datetime-operations)
6. [String Manipulations](#string-manipulations)
7. [Performance Optimization](#performance-optimization)
8. [Real Interview Questions](#real-interview-questions)

---

# Basic to Intermediate

### Q1: Find duplicate records
```sql
-- Method 1: Using GROUP BY
SELECT email, COUNT(*) as count
FROM users
GROUP BY email
HAVING COUNT(*) > 1;

-- Method 2: Using window function
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) as rn
    FROM users
) t
WHERE rn > 1;

-- Method 3: Self join
SELECT DISTINCT u1.*
FROM users u1
JOIN users u2 
    ON u1.email = u2.email 
    AND u1.id <> u2.id;
```

### Q2: Delete duplicate records keeping one
```sql
-- Keep record with lowest ID
DELETE FROM users
WHERE id NOT IN (
    SELECT MIN(id)
    FROM users
    GROUP BY email
);

-- Using CTE and ROW_NUMBER (PostgreSQL, SQL Server)
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) as rn
    FROM users
)
DELETE FROM cte WHERE rn > 1;
```

### Q3: Find Nth highest value
```sql
-- 2nd highest salary
SELECT MAX(salary) as second_highest
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

-- Generic Nth highest (N=5)
SELECT DISTINCT salary
FROM (
    SELECT salary,
           DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) t
WHERE rank = 5;

-- Using OFFSET (MySQL, PostgreSQL)
SELECT DISTINCT salary
FROM employees
ORDER BY salary DESC
LIMIT 1 OFFSET 4;  -- For 5th highest (N-1)
```

### Q4: Find employees without any sales
```sql
-- Method 1: LEFT JOIN
SELECT e.employee_id, e.name
FROM employees e
LEFT JOIN sales s ON e.employee_id = s.employee_id
WHERE s.sale_id IS NULL;

-- Method 2: NOT EXISTS
SELECT e.employee_id, e.name
FROM employees e
WHERE NOT EXISTS (
    SELECT 1 
    FROM sales s 
    WHERE s.employee_id = e.employee_id
);

-- Method 3: NOT IN
SELECT employee_id, name
FROM employees
WHERE employee_id NOT IN (
    SELECT DISTINCT employee_id 
    FROM sales 
    WHERE employee_id IS NOT NULL
);
```

### Q5: Calculate cumulative sum
```sql
-- Running total of sales by date
SELECT 
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date) as running_total
FROM sales
ORDER BY sale_date;

-- Running total by product
SELECT 
    sale_date,
    product_id,
    amount,
    SUM(amount) OVER (
        PARTITION BY product_id 
        ORDER BY sale_date
    ) as running_total_by_product
FROM sales;
```

---

# Window Functions

### Q6: Ranking functions comparison
```sql
SELECT 
    employee_id,
    department,
    salary,
    -- ROW_NUMBER: Unique rank (1,2,3,4,5...)
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num,
    
    -- RANK: Same rank for ties, skips next (1,2,2,4,5...)
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
    
    -- DENSE_RANK: Same rank for ties, no skip (1,2,2,3,4...)
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank,
    
    -- NTILE: Divide into N groups
    NTILE(4) OVER (PARTITION BY department ORDER BY salary DESC) as quartile
FROM employees;
```

### Q7: LAG and LEAD functions
```sql
-- Compare with previous and next month
SELECT 
    month,
    revenue,
    -- Previous month
    LAG(revenue, 1) OVER (ORDER BY month) as prev_month_revenue,
    
    -- Next month
    LEAD(revenue, 1) OVER (ORDER BY month) as next_month_revenue,
    
    -- Month-over-month change
    revenue - LAG(revenue, 1) OVER (ORDER BY month) as mom_change,
    
    -- % change
    ROUND(
        (revenue - LAG(revenue, 1) OVER (ORDER BY month)) * 100.0 / 
        LAG(revenue, 1) OVER (ORDER BY month),
        2
    ) as mom_pct_change,
    
    -- Year over year (12 months ago)
    revenue - LAG(revenue, 12) OVER (ORDER BY month) as yoy_change
FROM monthly_revenue
ORDER BY month;
```

### Q8: Moving averages
```sql
SELECT 
    date,
    stock_price,
    -- 7-day moving average
    AVG(stock_price) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as ma_7day,
    
    -- 30-day moving average
    AVG(stock_price) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as ma_30day,
    
    -- Centered moving average (3 days before and after)
    AVG(stock_price) OVER (
        ORDER BY date 
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) as centered_ma_7day
FROM stock_prices
ORDER BY date;
```

### Q9: First and last value in window
```sql
SELECT 
    employee_id,
    month,
    sales,
    -- First sale of the year
    FIRST_VALUE(sales) OVER (
        PARTITION BY YEAR(month)
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as first_month_sales,
    
    -- Last sale of the year
    LAST_VALUE(sales) OVER (
        PARTITION BY YEAR(month)
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_month_sales,
    
    -- Compare to first month
    sales - FIRST_VALUE(sales) OVER (
        PARTITION BY YEAR(month)
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as diff_from_first_month
FROM employee_sales;
```

### Q10: Percentile calculations
```sql
SELECT 
    employee_id,
    salary,
    -- Percentile rank
    PERCENT_RANK() OVER (ORDER BY salary) as percentile_rank,
    
    -- Cumulative distribution
    CUME_DIST() OVER (ORDER BY salary) as cumulative_dist,
    
    -- Which percentile bucket (decile)
    NTILE(10) OVER (ORDER BY salary) as decile,
    
    -- Compare to median
    salary - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) OVER () as diff_from_median
FROM employees;
```

---

# Complex Joins

### Q11: Self-join for hierarchical data
```sql
-- Find employees and their managers
SELECT 
    e.employee_id,
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary,
    -- Level difference
    e.salary - m.salary as salary_difference
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- Find all levels of management chain
WITH RECURSIVE management_chain AS (
    -- Anchor: Start with a specific employee
    SELECT 
        employee_id,
        name,
        manager_id,
        0 as level,
        CAST(name AS VARCHAR(1000)) as chain
    FROM employees
    WHERE employee_id = 100
    
    UNION ALL
    
    -- Recursive: Get managers
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        mc.level + 1,
        CONCAT(mc.chain, ' <- ', e.name)
    FROM employees e
    JOIN management_chain mc ON e.employee_id = mc.manager_id
)
SELECT * FROM management_chain;
```

### Q12: Multiple table joins with aggregations
```sql
-- Sales analysis with customer, product, and sales data
SELECT 
    c.customer_name,
    c.region,
    p.product_category,
    COUNT(DISTINCT s.order_id) as total_orders,
    COUNT(DISTINCT p.product_id) as unique_products,
    SUM(s.quantity) as total_quantity,
    SUM(s.quantity * s.unit_price) as total_revenue,
    AVG(s.quantity * s.unit_price) as avg_order_value,
    -- First and last order dates
    MIN(s.order_date) as first_order,
    MAX(s.order_date) as last_order,
    DATEDIFF(MAX(s.order_date), MIN(s.order_date)) as customer_lifetime_days
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
GROUP BY c.customer_name, c.region, p.product_category
HAVING SUM(s.quantity * s.unit_price) > 10000
ORDER BY total_revenue DESC;
```

### Q13: CROSS JOIN for generating combinations
```sql
-- Generate all possible product-store combinations
SELECT 
    p.product_id,
    p.product_name,
    s.store_id,
    s.store_name,
    COALESCE(i.stock_quantity, 0) as current_stock
FROM products p
CROSS JOIN stores s
LEFT JOIN inventory i 
    ON p.product_id = i.product_id 
    AND s.store_id = i.store_id;

-- Generate date range
WITH RECURSIVE date_range AS (
    SELECT DATE('2024-01-01') as date
    UNION ALL
    SELECT DATE_ADD(date, INTERVAL 1 DAY)
    FROM date_range
    WHERE date < '2024-12-31'
)
SELECT * FROM date_range;
```

### Q14: Advanced OUTER JOIN scenarios
```sql
-- Find customers with no orders in last 90 days
SELECT 
    c.customer_id,
    c.customer_name,
    MAX(o.order_date) as last_order_date,
    DATEDIFF(CURRENT_DATE, MAX(o.order_date)) as days_since_last_order
FROM customers c
LEFT JOIN orders o 
    ON c.customer_id = o.customer_id
    AND o.order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
GROUP BY c.customer_id, c.customer_name
HAVING MAX(o.order_date) IS NULL 
    OR DATEDIFF(CURRENT_DATE, MAX(o.order_date)) > 90;

-- FULL OUTER JOIN to find mismatches
SELECT 
    COALESCE(e.employee_id, t.employee_id) as employee_id,
    e.name,
    t.clock_in_time,
    CASE 
        WHEN e.employee_id IS NULL THEN 'Timesheet without employee'
        WHEN t.employee_id IS NULL THEN 'Employee without timesheet'
        ELSE 'Matched'
    END as status
FROM employees e
FULL OUTER JOIN timesheets t ON e.employee_id = t.employee_id;
```

---

# Subqueries & CTEs

### Q15: Correlated subqueries
```sql
-- Employees earning above their department average
SELECT 
    e.employee_id,
    e.name,
    e.department,
    e.salary,
    (SELECT AVG(salary) 
     FROM employees e2 
     WHERE e2.department = e.department) as dept_avg_salary,
    e.salary - (SELECT AVG(salary) 
                FROM employees e2 
                WHERE e2.department = e.department) as diff_from_avg
FROM employees e
WHERE e.salary > (
    SELECT AVG(salary)
    FROM employees e2
    WHERE e2.department = e.department
);
```

### Q16: Multiple CTEs
```sql
-- Sales analysis with multiple CTEs
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        product_id,
        SUM(quantity * unit_price) as revenue
    FROM sales
    GROUP BY DATE_TRUNC('month', order_date), product_id
),
product_ranks AS (
    SELECT 
        month,
        product_id,
        revenue,
        RANK() OVER (PARTITION BY month ORDER BY revenue DESC) as rank
    FROM monthly_sales
),
top_products AS (
    SELECT *
    FROM product_ranks
    WHERE rank <= 10
)
SELECT 
    tp.month,
    p.product_name,
    tp.revenue,
    tp.rank,
    -- Compare to previous month
    LAG(tp.revenue) OVER (
        PARTITION BY tp.product_id 
        ORDER BY tp.month
    ) as prev_month_revenue
FROM top_products tp
JOIN products p ON tp.product_id = p.product_id
ORDER BY tp.month, tp.rank;
```

### Q17: Recursive CTEs for hierarchical data
```sql
-- Calculate total subordinates for each manager
WITH RECURSIVE subordinate_count AS (
    -- Anchor: Direct reports
    SELECT 
        manager_id,
        employee_id,
        1 as level
    FROM employees
    WHERE manager_id IS NOT NULL
    
    UNION ALL
    
    -- Recursive: Indirect reports
    SELECT 
        sc.manager_id,
        e.employee_id,
        sc.level + 1
    FROM employees e
    JOIN subordinate_count sc ON e.manager_id = sc.employee_id
)
SELECT 
    e.employee_id,
    e.name,
    COUNT(sc.employee_id) as total_subordinates,
    MAX(sc.level) as max_depth
FROM employees e
LEFT JOIN subordinate_count sc ON e.employee_id = sc.manager_id
GROUP BY e.employee_id, e.name
ORDER BY total_subordinates DESC;
```

### Q18: Scalar subqueries in SELECT
```sql
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    -- Customer's total orders
    (SELECT COUNT(*) 
     FROM orders o2 
     WHERE o2.customer_id = o.customer_id) as customer_total_orders,
    
    -- Customer's first order date
    (SELECT MIN(order_date) 
     FROM orders o2 
     WHERE o2.customer_id = o.customer_id) as customer_first_order,
    
    -- Order number for this customer
    (SELECT COUNT(*) 
     FROM orders o2 
     WHERE o2.customer_id = o.customer_id 
     AND o2.order_date <= o.order_date) as order_sequence
FROM orders o;
```

---

# Date/Time Operations

### Q19: Advanced date calculations
```sql
-- Various date manipulations
SELECT 
    order_date,
    
    -- Date parts
    YEAR(order_date) as year,
    QUARTER(order_date) as quarter,
    MONTH(order_date) as month,
    WEEK(order_date) as week,
    DAY(order_date) as day,
    DAYOFWEEK(order_date) as day_of_week,
    DAYNAME(order_date) as day_name,
    
    -- First/last day of month
    DATE_TRUNC('month', order_date) as first_day_of_month,
    LAST_DAY(order_date) as last_day_of_month,
    
    -- First day of year
    DATE_TRUNC('year', order_date) as first_day_of_year,
    
    -- Add/subtract intervals
    DATE_ADD(order_date, INTERVAL 30 DAY) as plus_30_days,
    DATE_SUB(order_date, INTERVAL 1 MONTH) as minus_1_month,
    
    -- Difference from today
    DATEDIFF(CURRENT_DATE, order_date) as days_ago,
    
    -- Business days (approximate - excluding weekends)
    CASE 
        WHEN DAYOFWEEK(order_date) IN (1, 7) THEN 0
        ELSE 1
    END as is_business_day
FROM orders;
```

### Q20: Session analysis with time gaps
```sql
-- Define sessions (gap > 30 minutes = new session)
WITH event_sessions AS (
    SELECT 
        user_id,
        event_time,
        event_type,
        -- Mark session start
        CASE 
            WHEN LAG(event_time) OVER (
                PARTITION BY user_id 
                ORDER BY event_time
            ) IS NULL THEN 1
            WHEN TIMESTAMPDIFF(MINUTE, 
                LAG(event_time) OVER (
                    PARTITION BY user_id 
                    ORDER BY event_time
                ),
                event_time
            ) > 30 THEN 1
            ELSE 0
        END as is_session_start
    FROM user_events
),
session_ids AS (
    SELECT 
        *,
        SUM(is_session_start) OVER (
            PARTITION BY user_id 
            ORDER BY event_time
        ) as session_id
    FROM event_sessions
)
SELECT 
    user_id,
    session_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    TIMESTAMPDIFF(MINUTE, MIN(event_time), MAX(event_time)) as session_duration_minutes,
    COUNT(*) as events_in_session,
    COUNT(DISTINCT event_type) as unique_event_types
FROM session_ids
GROUP BY user_id, session_id
HAVING COUNT(*) > 1
ORDER BY user_id, session_start;
```

### Q21: Time-based cohort analysis
```sql
-- User retention cohort analysis
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(signup_date)) as cohort_month
    FROM users
    GROUP BY user_id
),
user_activities AS (
    SELECT 
        a.user_id,
        uc.cohort_month,
        DATE_TRUNC('month', a.activity_date) as activity_month,
        TIMESTAMPDIFF(MONTH, uc.cohort_month, DATE_TRUNC('month', a.activity_date)) as months_since_signup
    FROM activities a
    JOIN user_cohorts uc ON a.user_id = uc.user_id
)
SELECT 
    cohort_month,
    months_since_signup,
    COUNT(DISTINCT user_id) as active_users,
    -- Cohort size
    FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_signup
    ) as cohort_size,
    -- Retention rate
    ROUND(
        COUNT(DISTINCT user_id) * 100.0 / 
        FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (
            PARTITION BY cohort_month 
            ORDER BY months_since_signup
        ),
        2
    ) as retention_rate
FROM user_activities
GROUP BY cohort_month, months_since_signup
ORDER BY cohort_month, months_since_signup;
```

---

# String Manipulations

### Q22: String functions and pattern matching
```sql
SELECT 
    -- Case conversion
    UPPER(name) as uppercase_name,
    LOWER(email) as lowercase_email,
    INITCAP(name) as title_case,
    
    -- Substring
    SUBSTRING(email, 1, POSITION('@' IN email) - 1) as username,
    SUBSTRING(email, POSITION('@' IN email) + 1) as domain,
    
    -- Length
    LENGTH(name) as name_length,
    CHAR_LENGTH(name) as char_count,
    
    -- Trim
    TRIM(name) as trimmed_name,
    LTRIM(name) as left_trimmed,
    RTRIM(name) as right_trimmed,
    
    -- Replace
    REPLACE(phone, '-', '') as phone_no_dashes,
    
    -- Concatenate
    CONCAT(first_name, ' ', last_name) as full_name,
    CONCAT_WS(', ', city, state, country) as location,
    
    -- Pattern matching
    CASE 
        WHEN email LIKE '%@gmail.com' THEN 'Gmail'
        WHEN email LIKE '%@yahoo.com' THEN 'Yahoo'
        WHEN email LIKE '%@company.com' THEN 'Corporate'
        ELSE 'Other'
    END as email_type,
    
    -- Regex matching (PostgreSQL)
    CASE 
        WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$' 
        THEN 'Valid'
        ELSE 'Invalid'
    END as email_validity
FROM customers;
```

### Q23: Parse and transform complex strings
```sql
-- Parse JSON/delimited strings
SELECT 
    id,
    data_string,
    -- Split by delimiter (assumes comma-separated values)
    SPLIT_PART(data_string, ',', 1) as value1,
    SPLIT_PART(data_string, ',', 2) as value2,
    SPLIT_PART(data_string, ',', 3) as value3,
    
    -- Extract numbers from string
    REGEXP_REPLACE(data_string, '[^0-9]', '', 'g') as only_numbers,
    
    -- Extract words
    REGEXP_REPLACE(data_string, '[^a-zA-Z\s]', '', 'g') as only_letters,
    
    -- Parse URL parts
    SPLIT_PART(url, '/', 3) as domain,
    SPLIT_PART(url, '?', 2) as query_string
FROM data_table;

-- Advanced: Parse nested data
SELECT 
    user_id,
    -- Parse pipe-delimited list of items
    UNNEST(STRING_TO_ARRAY(items, '|')) as item
FROM orders;
```

---

# Performance Optimization

### Q24: Index usage and query optimization
```sql
-- ❌ BAD: Doesn't use index (function on indexed column)
SELECT * FROM orders
WHERE YEAR(order_date) = 2024;

-- ✅ GOOD: Uses index
SELECT * FROM orders
WHERE order_date >= '2024-01-01' 
  AND order_date < '2025-01-01';

-- ❌ BAD: OR conditions may not use index
SELECT * FROM customers
WHERE city = 'New York' OR city = 'Los Angeles';

-- ✅ GOOD: IN clause can use index
SELECT * FROM customers
WHERE city IN ('New York', 'Los Angeles');

-- ❌ BAD: Leading wildcard prevents index use
SELECT * FROM products
WHERE product_name LIKE '%phone%';

-- ✅ GOOD: Prefix search uses index
SELECT * FROM products
WHERE product_name LIKE 'iPhone%';
```

### Q25: Optimize subqueries
```sql
-- ❌ BAD: Correlated subquery runs for each row
SELECT 
    e.employee_id,
    e.name,
    e.salary,
    (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e.department) as avg_salary
FROM employees e
WHERE e.salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e.department);

-- ✅ GOOD: Join with pre-computed aggregates
WITH dept_avg AS (
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
)
SELECT 
    e.employee_id,
    e.name,
    e.salary,
    d.avg_salary
FROM employees e
JOIN dept_avg d ON e.department = d.department
WHERE e.salary > d.avg_salary;
```

### Q26: Execution plan analysis
```sql
-- Explain query execution plan
EXPLAIN ANALYZE
SELECT 
    c.customer_name,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.customer_name
HAVING COUNT(o.order_id) > 5;

/*
Look for:
1. Seq Scan (table scan) - Bad for large tables
2. Index Scan - Good
3. Hash Join vs Nested Loop - Hash join better for large tables
4. Sort operations - Expensive
5. High cost estimates
*/
```

---

# Real Interview Questions

### Q27: Find consecutive dates
```sql
-- Find customers who made purchases on consecutive days
WITH daily_purchases AS (
    SELECT DISTINCT 
        customer_id,
        DATE(purchase_date) as purchase_date
    FROM purchases
),
consecutive_check AS (
    SELECT 
        customer_id,
        purchase_date,
        LAG(purchase_date) OVER (
            PARTITION BY customer_id 
            ORDER BY purchase_date
        ) as prev_purchase_date,
        DATEDIFF(
            purchase_date,
            LAG(purchase_date) OVER (
                PARTITION BY customer_id 
                ORDER BY purchase_date
            )
        ) as days_diff
    FROM daily_purchases
)
SELECT DISTINCT customer_id
FROM consecutive_check
WHERE days_diff = 1;
```

### Q28: Calculate median (without PERCENTILE function)
```sql
-- Calculate median using window functions
WITH ranked_salaries AS (
    SELECT 
        salary,
        ROW_NUMBER() OVER (ORDER BY salary) as row_num,
        COUNT(*) OVER () as total_count
    FROM employees
)
SELECT 
    AVG(salary) as median_salary
FROM ranked_salaries
WHERE row_num IN (
    FLOOR((total_count + 1) / 2.0),
    CEIL((total_count + 1) / 2.0)
);
```

### Q29: Find gaps in sequence
```sql
-- Find missing invoice numbers
WITH RECURSIVE all_numbers AS (
    SELECT 1 as num
    UNION ALL
    SELECT num + 1
    FROM all_numbers
    WHERE num < (SELECT MAX(invoice_number) FROM invoices)
)
SELECT 
    an.num as missing_invoice_number
FROM all_numbers an
LEFT JOIN invoices i ON an.num = i.invoice_number
WHERE i.invoice_number IS NULL;
```

### Q30: Pivot table without PIVOT function
```sql
-- Convert rows to columns manually
SELECT 
    product_id,
    product_name,
    SUM(CASE WHEN MONTH(sale_date) = 1 THEN amount ELSE 0 END) as jan_sales,
    SUM(CASE WHEN MONTH(sale_date) = 2 THEN amount ELSE 0 END) as feb_sales,
    SUM(CASE WHEN MONTH(sale_date) = 3 THEN amount ELSE 0 END) as mar_sales,
    SUM(CASE WHEN MONTH(sale_date) = 4 THEN amount ELSE 0 END) as apr_sales,
    SUM(CASE WHEN MONTH(sale_date) = 5 THEN amount ELSE 0 END) as may_sales,
    SUM(CASE WHEN MONTH(sale_date) = 6 THEN amount ELSE 0 END) as jun_sales,
    SUM(amount) as total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE YEAR(sale_date) = 2024
GROUP BY product_id, product_name;
```

### Q31: Three-way join comparison
```sql
-- Find products bought together
SELECT 
    p1.product_name as product_1,
    p2.product_name as product_2,
    COUNT(DISTINCT o1.order_id) as times_bought_together,
    COUNT(DISTINCT c.customer_id) as unique_customers
FROM order_items o1
JOIN order_items o2 
    ON o1.order_id = o2.order_id 
    AND o1.product_id < o2.product_id  -- Avoid duplicate pairs
JOIN products p1 ON o1.product_id = p1.product_id
JOIN products p2 ON o2.product_id = p2.product_id
JOIN orders ord ON o1.order_id = ord.order_id
JOIN customers c ON ord.customer_id = c.customer_id
GROUP BY p1.product_name, p2.product_name
HAVING COUNT(DISTINCT o1.order_id) >= 10
ORDER BY times_bought_together DESC;
```

### Q32: Running difference
```sql
-- Calculate running difference from first value
SELECT 
    date,
    value,
    FIRST_VALUE(value) OVER (ORDER BY date) as first_value,
    value - FIRST_VALUE(value) OVER (ORDER BY date) as diff_from_start,
    -- Percentage change from start
    ROUND(
        (value - FIRST_VALUE(value) OVER (ORDER BY date)) * 100.0 /
        FIRST_VALUE(value) OVER (ORDER BY date),
        2
    ) as pct_change_from_start
FROM metrics
ORDER BY date;
```

### Q33: Find outliers (IQR method)
```sql
-- Detect outliers using Interquartile Range
WITH quartiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) as q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) as q3,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) - 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) as iqr
    FROM transactions
)
SELECT 
    t.*,
    CASE 
        WHEN t.amount < (q.q1 - 1.5 * q.iqr) THEN 'Lower Outlier'
        WHEN t.amount > (q.q3 + 1.5 * q.iqr) THEN 'Upper Outlier'
        ELSE 'Normal'
    END as outlier_status
FROM transactions t
CROSS JOIN quartiles q;
```

### Q34: Complex aggregation with ROLLUP
```sql
-- Hierarchical totals
SELECT 
    COALESCE(region, 'ALL REGIONS') as region,
    COALESCE(country, 'ALL COUNTRIES') as country,
    COALESCE(city, 'ALL CITIES') as city,
    SUM(sales) as total_sales,
    COUNT(*) as num_transactions
FROM sales_data
GROUP BY ROLLUP(region, country, city)
ORDER BY region, country, city;
```

### Q35: Rate limiting / Throttling detection
```sql
-- Find users exceeding rate limits (>100 requests per minute)
WITH request_rates AS (
    SELECT 
        user_id,
        DATE_TRUNC('minute', request_time) as minute,
        COUNT(*) as request_count
    FROM api_requests
    WHERE request_time >= NOW() - INTERVAL '1 hour'
    GROUP BY user_id, DATE_TRUNC('minute', request_time)
)
SELECT 
    user_id,
    COUNT(*) as violations,
    MAX(request_count) as max_requests_per_minute,
    MIN(minute) as first_violation,
    MAX(minute) as last_violation
FROM request_rates
WHERE request_count > 100
GROUP BY user_id
ORDER BY violations DESC;
```

---

# 🎯 Practice Challenges

## Challenge 1: E-commerce Analytics
Given tables: `users`, `orders`, `order_items`, `products`

Write queries to find:
1. Top 10 customers by lifetime value
2. Month-over-month growth rate
3. Products frequently bought together
4. Customer retention cohorts
5. Average order value by customer segment

## Challenge 2: Time Series Analysis
Given table: `sensor_readings` (sensor_id, timestamp, value)

Calculate:
1. 5-minute moving average
2. Anomalies (values > 2 standard deviations from mean)
3. Gap detection (missing readings)
4. Interpolate missing values
5. Peak usage hours

## Challenge 3: Social Network Analysis
Given tables: `users`, `friendships`, `posts`, `likes`, `comments`

Find:
1. Users with most connections (friends of friends)
2. Most engaging content
3. User influence score
4. Community detection
5. Activity timeline

---

# 💡 SQL Interview Tips

1. **Always ask clarifying questions**
   - Table structure?
   - Data volume?
   - Expected output format?
   - Performance requirements?

2. **Think about edge cases**
   - NULL values
   - Duplicates
   - Empty results
   - Date boundaries

3. **Consider performance**
   - Use indexes
   - Avoid N+1 queries
   - Minimize subqueries
   - Use appropriate joins

4. **Write readable code**
   - Proper indentation
   - Meaningful aliases
   - Comments for complex logic
   - CTEs for clarity

5. **Test your queries**
   - Start with small dataset
   - Verify edge cases
   - Check performance
   - Validate results

---

## 🚀 Next Steps

1. Practice these queries on real databases
2. Time yourself (aim for 10-15 minutes per query)
3. Optimize slow queries
4. Explain your thought process out loud
5. Review execution plans

Good luck with your SQL interviews! 🎉

