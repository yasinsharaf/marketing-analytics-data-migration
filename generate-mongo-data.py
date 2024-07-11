import pymongo
from pymongo import MongoClient
import random
import string
import sys

# MongoDB Atlas connection URI
mongo_uri = "mongodb+srv://datashiptest:testing%40123@source2-mongodb.e4alhqk.mongodb.net/?retryWrites=true&w=majority&appName=source2-mongodb"
database_name = "marketing_mongo"  # Replace with your actual database name

# Function to generate random data
def generate_data(num_records, avg_record_size):
    data = []
    for _ in range(num_records):
        record = {
            "data": ''.join(random.choices(string.ascii_letters + string.digits, k=avg_record_size * 1024 - 20))
        }
        data.append(record)
    return data

# Function to insert data into MongoDB
def insert_data(collection_name, data):
    try:
        client = MongoClient(mongo_uri)
        db = client[database_name]  # Accessing the specified database
        collection = db[collection_name]
        collection.insert_many(data)
        print(f"Inserted {len(data)} records into '{collection_name}' collection.")
    except pymongo.errors.PyMongoError as e:
        print(f"Error inserting data into '{collection_name}' collection:", e)
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    try:
        # Customer Profiles: 100MB
        customer_data = generate_data(10000, 10)  # 10,000 records, average size 10KB
        insert_data("customer_profiles", customer_data)

        # Product Catalogs: 100MB
        product_data = generate_data(5000, 20)  # 5,000 records, average size 20KB
        insert_data("product_catalogs", product_data)

        # Campaign Metadata: 100MB
        campaign_data = generate_data(2000, 50)  # 2,000 records, average size 50KB
        insert_data("campaign_metadata", campaign_data)

    except Exception as e:
        print("An error occurred during data generation and insertion:", e)
