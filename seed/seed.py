"""Pulls sample data from csvs into the database for easy DB setup and resetting"""
import asyncio
import csv
from functools import partial
from datetime import datetime
from decouple import config
import psycopg2


open_utf8 = partial(open, encoding="UTF-8")

DB_NAME = config("DB_NAME")
DB_USER = config("DB_USER")
DB_PORT = config("DB_PORT")

db_connection = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} port={DB_PORT}")
db_cursor = db_connection.cursor()


def format_cell(cell):
    """Ensures a given csv cell is formatted correctly for data ingestion"""
    result = cell if cell != "" else "NULL"

    if result == "NULL":
        return result

    return f"'{result.strip()}'"


def format_date_cell(date_cell):
    """Ensures a given csv date cell is formatted correctly for data ingestion"""
    if date_cell == "":
        return "NULL"

    return f"'{datetime.strptime(date_cell, '%b %d').date()}'"


async def clear_db():
    """Initially clear any data that's already in the database"""
    db_cursor.execute("DROP TABLE IF EXISTS listings ")
    db_cursor.execute("DROP TABLE IF EXISTS traits ")
    db_cursor.execute("DROP TABLE IF EXISTS listings_traits ")
    db_cursor.execute("DROP TABLE IF EXISTS groups ")
    db_cursor.execute("DROP TABLE IF EXISTS groups_users ")
    db_cursor.execute("DROP TABLE IF EXISTS groups_listings ")


async def generate_tables():
    """Create all of the tables the app needs"""
    db_cursor.execute(
        """
        CREATE TABLE listings(
            id INT GENERATED ALWAYS AS IDENTITY,
            name TEXT,
            bedrooms INT,
            bathrooms FLOAT,
            price INT NOT NULL,
            sqft INT,
            available_date DATE,
            is_available BOOLEAN,
            building_type TEXT NOT NULL,
            deposit INT,
            notes TEXT,
            parking_spaces INT,
            address TEXT NOT NULL,
            is_bookmarked BOOLEAN,
            city TEXT NOT NULL,
            PRIMARY KEY(id)
        )
        """
    )

    db_cursor.execute(
        """
        CREATE TABLE traits(
            id INT GENERATED ALWAYS AS IDENTITY,
            name TEXT NOT NULL,
            PRIMARY KEY(id)
        )
        """
    )

    db_cursor.execute(
        """
        CREATE TABLE listings_traits(
            listing_id INT NOT NULL,
            trait_id INT NOT NULL,
            CONSTRAINT fk_listing
            FOREIGN KEY(listing_id)
                REFERENCES listings(id),
            CONSTRAINT fk_trait
            FOREIGN KEY(trait_id)
                REFERENCES traits(id)
        )
        """
    )

    db_cursor.execute(
        """
        CREATE TABLE groups(
            id INT GENERATED ALWAYS AS IDENTITY,
            name TEXT NOT NULL,
            PRIMARY KEY(id)
        )
        """
    )

    db_cursor.execute(
        """
        CREATE TABLE groups_users(
            group_id INT NOT NULL,
            user_id INT NOT NULL,
            CONSTRAINT fk_group
            FOREIGN KEY(group_id)
                REFERENCES groups(id)
        )
        """
    )

    db_cursor.execute(
        """
        CREATE TABLE groups_listings(
            group_id INT NOT NULL,
            listing_id INT NOT NULL,
            CONSTRAINT fk_group
            FOREIGN KEY(group_id)
                REFERENCES groups(id),
            CONSTRAINT fk_listing
            FOREIGN KEY(listing_id)
                REFERENCES listings(id)
        )
        """
    )


async def ingest_data():
    """Pulls sample data from local CSV files into the newly initialized database"""
    print("ingesting data")
    with open("sample_listings.csv", newline="", encoding="utf-8") as csvfile:
        filereader = csv.reader(csvfile)
        # skipping header row
        next(filereader)
        for row in filereader:
            print("row: ", row)
            db_cursor.execute(
                f"""
                INSERT INTO listings(name, bedrooms, bathrooms, price, sqft, available_date, building_type, deposit, is_available, notes, parking_spaces, address, city) 
                VALUES ({format_cell(row[12])}, {format_cell(row[0])}, {format_cell(row[1])}, {format_cell(row[2])}, {format_cell(row[3])}, {format_date_cell(row[4])}, {format_cell(row[5])}, {format_cell(row[6])}, {format_cell(row[7])}, {format_cell(row[8])}, {format_cell(row[9])}, {format_cell(row[10])}, {format_cell(row[11])});
            """
            )


async def seed():
    """Main execution loop for the seed script"""
    print("Clearing existing data")
    await clear_db()
    print("Database cleared")
    print("Re-initializing database")
    await generate_tables()
    print("Database initialized")
    print("Ingesting sample data")
    await ingest_data()
    print("Sample data loaded. Complete!")
    db_connection.commit()


# Confirm the user wants to run the destructive seeding process
is_ready = input(
    """This operation will delete any data currently in the database.
       Are you sure you want to do this? Input y/n (yes/no) to confirm."""
)

while is_ready != "y" and is_ready != "n":
    is_ready = input("Invalid input. Please input y/n to confirm the seed operation")

if is_ready == "y":
    asyncio.run(seed())
