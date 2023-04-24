from fastapi import FastAPI
from decouple import config
import psycopg2

DB_NAME = config("DB_NAME")
DB_USER = config("DB_USER")
DB_PORT = config("DB_PORT")

db_connection = psycopg2.connect("dbname={database} user={username} port={port}".format(database=DB_NAME, username=DB_USER, port=DB_PORT))
db_cursor = db_connection.cursor()

app = FastAPI()

@app.get("/")
async def root():
  return {"message": "Hello World"}

@app.get("/test")
async def test():
  test_data = db_cursor.execute("SELECT * FROM listings")
  return {"payload": test_data} 