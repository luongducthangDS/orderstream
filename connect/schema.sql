-- OLTP source: an online shop. Every change is captured by Debezium through logical replication.
CREATE TABLE customers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(id),
    status      TEXT NOT NULL CHECK (status IN ('created', 'paid', 'shipped', 'delivered', 'cancelled')),
    amount_vnd  BIGINT NOT NULL CHECK (amount_vnd > 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- full row images on UPDATE/DELETE, so the sink never has to guess
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
