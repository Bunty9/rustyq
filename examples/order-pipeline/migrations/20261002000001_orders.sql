-- The application's own tables. The file name carries a TIMESTAMP version on
-- purpose: rustyq's migrations occupy versions 1, 2, ... in the shared
-- `_sqlx_migrations` table, and a sequential `0001_*` file would collide.

CREATE TABLE orders (
    id           uuid PRIMARY KEY,
    email        text NOT NULL,
    amount_cents int  NOT NULL CHECK (amount_cents > 0),
    status       text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid')),
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- PK (order_id, template): "this email was sent" is recorded at most once.
CREATE TABLE sent_emails (
    order_id uuid NOT NULL REFERENCES orders (id),
    template text NOT NULL,
    sent_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, template)
);

-- PK order_id: one charge per order, our idempotency key.
CREATE TABLE charges (
    order_id     uuid PRIMARY KEY REFERENCES orders (id),
    amount_cents int NOT NULL,
    charged_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE daily_reports (
    id            bigserial PRIMARY KEY,
    order_count   bigint NOT NULL,
    revenue_cents bigint NOT NULL,
    generated_at  timestamptz NOT NULL DEFAULT now()
);
