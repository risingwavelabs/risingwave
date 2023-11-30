CREATE MODEL mindsdb.home_rentals_model
FROM example_data
  (SELECT * FROM home_rentals)
PREDICT rental_price;
