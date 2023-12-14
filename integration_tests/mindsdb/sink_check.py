import subprocess

# The model creation may take a long time. Our estimate is 30 seconds. But it can be longer in lower-perf machines.

sql = "SELECT rental_price FROM home_rentals_model WHERE number_of_bathrooms = 2 AND sqft = 1000;"

subprocess.run(["psql", "-h", "localhost", "-p", "55432", "-U", "mindsdb", "-d", "mindsdb", "-c", sql], check=True)
