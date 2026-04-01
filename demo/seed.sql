-- Create a sample table
CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    customer   VARCHAR(100) NOT NULL,
    product    VARCHAR(100) NOT NULL,
    quantity   INT NOT NULL,
    price      NUMERIC(10,2) NOT NULL,
    status     VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Insert initial data
INSERT INTO orders (customer, product, quantity, price, status) VALUES
    ('alice',   'widget-a', 3,  29.99, 'shipped'),
    ('bob',     'widget-b', 1,  49.99, 'pending'),
    ('charlie', 'gadget-x', 2, 149.99, 'shipped'),
    ('diana',   'widget-a', 5,  29.99, 'delivered'),
    ('eve',     'gadget-y', 1, 299.99, 'pending');
