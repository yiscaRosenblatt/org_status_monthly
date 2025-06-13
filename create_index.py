from pymongo import MongoClient

MONGO_URI = "mongodb+srv://yiscarose:Z754IlLTUkiqfpIZ@by-staging.dxg0wdb.mongodb.net/"
client = MongoClient(MONGO_URI)
db = client["by-reporting"]
coll = db["org_status_monthly"]
coll.create_index([("org_id", 1), ("year_month", 1)], unique=True)
print("אינדקס ייחודי נוצר!")
