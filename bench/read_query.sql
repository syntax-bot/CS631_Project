SELECT category, count(*), round(avg(amount), 2) FROM orders GROUP BY category ORDER BY category;
