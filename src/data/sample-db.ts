// Sample dataset and example queries for the in-browser SQL playground (SQLite dialect).
export const SEED = `
CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, city TEXT);
INSERT INTO departments VALUES (1,'Data','Tehran'),(2,'Web','Tehran'),(3,'Mobile','Isfahan'),(4,'Finance','Shiraz');

CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, manager_id INTEGER, salary INTEGER, hired_on TEXT);
INSERT INTO employees VALUES
 (1,'Ali',1,NULL,15000,'2019-03-01'),(2,'Sara',1,1,12000,'2020-06-15'),(3,'Reza',1,1,12000,'2021-01-10'),
 (4,'Nima',1,2,10000,'2022-09-01'),(5,'Maryam',2,1,11000,'2020-02-20'),(6,'Kian',2,5,8000,'2023-04-01'),
 (7,'Parisa',2,5,8000,'2023-07-12'),(8,'Dara',3,1,9500,'2021-11-05'),(9,'Neda',3,8,7000,'2024-01-15'),
 (10,'Omid',NULL,1,6500,'2024-03-01'),(11,'Hana',1,2,9000,'2022-05-30'),(12,'Babak',2,5,7000,'2022-08-22');

CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, country TEXT, signup_date TEXT);
INSERT INTO customers VALUES
 (1,'Alpha Co','IR','2024-01-05'),(2,'Beta Ltd','DE','2024-01-20'),(3,'Gamma Inc','IR','2024-02-11'),
 (4,'Delta GmbH','DE','2024-02-25'),(5,'Epsilon','TR','2024-03-03'),(6,'Zeta','IR','2024-03-18'),
 (7,'Eta LLC','AE','2024-04-02'),(8,'Theta','TR','2024-04-21');

CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, status TEXT, amount REAL);
INSERT INTO orders VALUES
 (1,1,'2024-01-10','paid',120.0),(2,1,'2024-02-14','paid',80.5),(3,2,'2024-01-25','paid',300.0),
 (4,3,'2024-02-15','refunded',45.0),(5,3,'2024-03-02','paid',99.9),(6,2,'2024-03-11','paid',210.0),
 (7,4,'2024-03-01','paid',150.0),(8,1,'2024-03-20','paid',60.0),(9,5,'2024-03-22','pending',500.0),
 (10,6,'2024-04-02','paid',75.0),(11,2,'2024-04-05','paid',130.0),(12,4,'2024-04-09','paid',90.0),
 (13,1,'2024-04-15','paid',220.0),(14,5,'2024-04-18','paid',310.0),(15,6,'2024-05-01','refunded',60.0),
 (16,3,'2024-05-06','paid',180.0),(17,2,'2024-05-12','paid',95.0),(18,1,'2024-05-20','paid',140.0);

CREATE TABLE daily_sales (day TEXT PRIMARY KEY, amount INTEGER);
INSERT INTO daily_sales VALUES
 ('2024-06-01',10),('2024-06-02',20),('2024-06-03',5),('2024-06-05',15),('2024-06-06',0),
 ('2024-06-07',30),('2024-06-08',25),('2024-06-09',40),('2024-06-10',35),('2024-06-12',50);

CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, event_date TEXT, event_type TEXT);
INSERT INTO events VALUES
 (1,1,'2024-07-01','login'),(2,1,'2024-07-02','login'),(3,1,'2024-07-03','login'),(4,1,'2024-07-06','login'),(5,1,'2024-07-07','login'),
 (6,2,'2024-07-01','login'),(7,2,'2024-07-03','login'),(8,2,'2024-07-04','login'),(9,2,'2024-07-04','login'),
 (10,3,'2024-07-05','login'),(11,3,'2024-07-06','login'),(12,3,'2024-07-07','login'),(13,3,'2024-07-08','login');
`;

export interface Example { id: string; title: { en: string; fa: string }; sql: string }
export const EXAMPLES: Example[] = [
  { id: 'second-highest', title: { en: 'Second highest salary', fa: 'دومین حقوق بالا' },
    sql: `-- Classic: second highest distinct salary
SELECT MAX(salary) AS second_highest
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);` },
  { id: 'top-n-dept', title: { en: 'Top 2 salaries per department', fa: 'دو حقوق بالا در هر دپارتمان' },
    sql: `-- DENSE_RANK keeps ties; ROW_NUMBER would not
SELECT * FROM (
  SELECT d.name AS dept, e.name, e.salary,
         DENSE_RANK() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) AS rnk
  FROM employees e JOIN departments d ON d.id = e.dept_id
)
WHERE rnk <= 2
ORDER BY dept, rnk, name;` },
  { id: 'anti-join', title: { en: 'Customers with no paid orders', fa: 'مشتری‌های بدون سفارش پرداخت‌شده' },
    sql: `SELECT c.id, c.name
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'paid'
WHERE o.id IS NULL;` },
  { id: 'running-total', title: { en: 'Running total per customer', fa: 'جمع تجمعی هر مشتری' },
    sql: `SELECT customer_id, order_date, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders
WHERE status = 'paid'
ORDER BY customer_id, order_date;` },
  { id: 'mom', title: { en: 'Month-over-month growth (LAG)', fa: 'رشد ماه به ماه (LAG)' },
    sql: `WITH monthly AS (
  SELECT substr(order_date, 1, 7) AS month, SUM(amount) AS revenue
  FROM orders WHERE status = 'paid' GROUP BY 1
)
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / LAG(revenue) OVER (ORDER BY month), 1) AS growth_pct
FROM monthly;` },
  { id: 'moving-avg', title: { en: 'Moving average and the gaps trap', fa: 'میانگین متحرک و تله‌ی روزهای جاافتاده' },
    sql: `-- daily_sales is missing 2024-06-04 and 2024-06-11.
-- ROWS counts rows, not calendar days, so this "3-day" average is wrong around gaps:
SELECT day, amount,
       ROUND(AVG(amount) OVER (ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS avg_3_rows
FROM daily_sales;` },
  { id: 'org-chart', title: { en: 'Org chart with a recursive CTE', fa: 'چارت سازمانی با CTE بازگشتی' },
    sql: `WITH RECURSIVE chain(id, name, manager_id, depth, path) AS (
  SELECT id, name, manager_id, 0, name FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.name, e.manager_id, c.depth + 1, c.path || ' > ' || e.name
  FROM employees e JOIN chain c ON e.manager_id = c.id
)
SELECT depth, path FROM chain ORDER BY path;` },
  { id: 'islands', title: { en: 'Gaps and islands: login streaks', fa: 'جزیره‌ها: توالی ورود پیاپی' },
    sql: `-- Consecutive-day streaks per user. Subtracting a row number turns a run of
-- consecutive dates into one constant "group key".
WITH d AS (SELECT DISTINCT user_id, event_date FROM events),
g AS (
  SELECT user_id, event_date,
         julianday(event_date) - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) AS grp
  FROM d
)
SELECT user_id, MIN(event_date) AS streak_start, MAX(event_date) AS streak_end, COUNT(*) AS days
FROM g GROUP BY user_id, grp
ORDER BY user_id, streak_start;` },
  { id: 'dedup', title: { en: 'Deduplicate with ROW_NUMBER', fa: 'حذف تکراری با ROW_NUMBER' },
    sql: `-- Keep one login row per user per day (the earliest id)
SELECT id, user_id, event_date FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY id) AS rn FROM events
)
WHERE rn = 1
ORDER BY user_id, event_date;` },
  { id: 'pct-total', title: { en: 'Share of total revenue by country', fa: 'سهم هر کشور از کل درآمد' },
    sql: `SELECT c.country,
       SUM(o.amount) AS revenue,
       ROUND(100.0 * SUM(o.amount) / SUM(SUM(o.amount)) OVER (), 1) AS pct_of_total
FROM orders o JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'paid'
GROUP BY c.country
ORDER BY revenue DESC;` },
];
