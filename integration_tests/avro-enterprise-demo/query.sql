FLUSH;

SELECT
  customer_key,
  full_name,
  status,
  segment,
  preferred_contact_text
FROM demo_ops.customer_profile_current
ORDER BY customer_key;

SELECT
  customer_key,
  balance,
  tags
FROM demo_ops.customer_profile_complex
ORDER BY customer_key;
