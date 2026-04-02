-- Run this while replication is active to see CDC in action.
-- Each statement produces WAL events that get replicated to Iceberg.

-- INSERTs
INSERT INTO orders (customer, product, quantity, price, status) VALUES
    ('frank',  'widget-c', 10,  19.99, 'pending'),
    ('grace',  'gadget-z',  2, 199.99, 'shipped'),
    ('heidi',  'widget-a',  7,  29.99, 'pending');

-- UPDATEs (produce equality delete + new data row in Iceberg)
UPDATE orders SET status = 'shipped',  quantity = quantity + 1 WHERE customer = 'bob';
UPDATE orders SET status = 'delivered', price = price * 0.9   WHERE customer = 'eve';
UPDATE orders SET product = 'widget-a-v2'                     WHERE product = 'widget-a';

-- DELETEs (produce equality delete files in Iceberg)
DELETE FROM orders WHERE customer = 'charlie';

-- Verify source state
SELECT count(*) AS total_orders FROM orders;
SELECT status, count(*) AS cnt FROM orders GROUP BY status ORDER BY status;
