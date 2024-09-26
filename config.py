import os
from dotenv import load_dotenv

def load_env():
    load_dotenv()

def get_db_params():
    return {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }

def get_local_db_params():
    return {
        "dbname": os.getenv("DB_NAME_LOCAL"),
        "user": os.getenv("DB_USER_LOCAL"),
        "password": os.getenv("DB_PASSWORD_LOCAL"),
        "host": os.getenv("DB_HOST_LOCAL"),
        "port": os.getenv("DB_PORT_LOCAL")
    }