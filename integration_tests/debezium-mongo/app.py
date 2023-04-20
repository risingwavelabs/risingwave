import os
import pymongo
from faker import Faker

# Connect to MongoDB
mongo_host = os.environ["MONGO_HOST"]
mongo_port = os.environ["MONGO_PORT"]
mongo_db_name = os.environ["MONGO_DB_NAME"]
mongo_username = os.environ["MONGO_USERNAME"]
mongo_password = os.environ["MONGO_PASSWORD"]

client = pymongo.MongoClient(
    f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.8.0"
)
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

print("Random data generated and inserted into MongoDB.")
