DROP PUBLICATION IF EXISTS demo_customer_actions_pub;
DROP TABLE IF EXISTS public.customer_actions;

CREATE TABLE public.customer_actions (
  action_id bigint PRIMARY KEY,
  customer_key varchar NOT NULL,
  action_type varchar NOT NULL,
  amount numeric(12, 2) NOT NULL,
  action_ts timestamp NOT NULL,
  channel varchar NOT NULL,
  team varchar NOT NULL
);

ALTER TABLE public.customer_actions REPLICA IDENTITY FULL;
CREATE PUBLICATION demo_customer_actions_pub FOR TABLE public.customer_actions;
