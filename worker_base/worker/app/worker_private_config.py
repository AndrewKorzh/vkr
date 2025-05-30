import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    print("can't load env")

DB_CONFIG = {
    "dbname": os.getenv("DBNAME"),
    "user": os.getenv("DBUSER"),
    "password": os.getenv("PASSWORD"),
    "host": os.getenv("HOST"),
    "port": os.getenv("PORT")
}

print(DB_CONFIG)
