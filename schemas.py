import psycopg2

# pSQL CONNECTION
conn = psycopg2.connect('postgres://wweudhzd:35Y84jhbI-O0xN-NvUd0NZOsPyyUWN7P@suleiman.db.elephantsql.com/wweudhzd')
cursor = conn.cursor()

# SCHEMAS FOR: TEMPERATURE(API), COUNTRY EMISSIONS(MONGODB), OIL PRICES(LOCAL CSV)
createTableQuery = """
-- Temperature data from the API
CREATE TABLE temperature_data (
    id SERIAL PRIMARY KEY,
    time VARCHAR(255),
    station DECIMAL,
    land DECIMAL
);

-- Emissions data from MongoDB
CREATE TABLE emissions_data (
    id SERIAL PRIMARY KEY,
    country VARCHAR(255),
    emissions_1990 DECIMAL,
    emissions_2005 DECIMAL,
    emissions_2017 DECIMAL,
    percent_of_world VARCHAR(255),
    change_percent DECIMAL,
    per_land_area DECIMAL,
    per_capita DECIMAL,
    total_including_lucf DECIMAL,
    total_excluding_lucf DECIMAL
);

-- Oil price data from CSV file
CREATE TABLE oil_prices (
    id SERIAL PRIMARY KEY,
    date DATE,
    price DECIMAL,
    percent_change DECIMAL,
    change DECIMAL
);
"""

# Execute the SQL statement
try:
    cursor.execute(createTableQuery)
    conn.commit()  # Commit the transaction
except Exception as e:
    print(f"error occurred: {e}")
finally:
    cursor.close()
    conn.close()



