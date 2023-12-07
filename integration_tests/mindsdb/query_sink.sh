QUERY='SELECT rental_price FROM home_rentals_model WHERE number_of_bathrooms = 2 AND sqft = 1000;'
psql -h localhost -p 55432 -U mindsdb -d mindsdb -c "$QUERY"