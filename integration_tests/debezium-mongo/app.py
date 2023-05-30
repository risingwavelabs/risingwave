import os
import pymongo
from faker import Faker

# To check the data through mongosh or mongo, run the following command:
#  > mongosh mongodb://admin:admin123@127.0.0.1:27017
#  > rs0 [direct: primary] test> use random_data
#  > rs0 [direct: primary] random_data> db.users.find()
#  > rs0 [direct: primary] random_data> db.users.count()

# Connect to MongoDB
mongo_host = os.environ["MONGO_HOST"]
mongo_port = os.environ["MONGO_PORT"]
mongo_db_name = os.environ["MONGO_DB_NAME"]

url = f"mongodb://{mongo_host}:{mongo_port}"
client = pymongo.MongoClient(url)
db = client[mongo_db_name]

# Generate random data
fake = Faker()
collection = db["users"]

for _ in range(1000):
    user_data = {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
    }
    collection.insert_one(user_data)

# Count the number of records in the collection
total_records = collection.count_documents({})

# Close the MongoDB connection
client.close()
print(f"Random data generated and inserted into MongoDB: {url}")

# Print insertion summary
print("Insertion summary:")
print(f"Total records in the collection: {total_records}")
