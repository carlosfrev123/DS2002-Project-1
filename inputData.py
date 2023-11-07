import requests
import pymongo
import pandas as pd
import psycopg2

# pSQL comm to insert from mongo
mongoInsert = """
INSERT INTO emissions_data (
    country, 
    emissions_1990, 
    emissions_2005, 
    emissions_2017, 
    percent_of_world, 
    change_percent, 
    per_land_area, 
    per_capita, 
    total_including_lucf, 
    total_excluding_lucf
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# pSQL CONNECTION
pg_conn = psycopg2.connect('postgres://wweudhzd:35Y84jhbI-O0xN-NvUd0NZOsPyyUWN7P@suleiman.db.elephantsql.com/wweudhzd')
pg_cursor = pg_conn.cursor()

# GLOBAL WARMING API FETCH
try:
    response = requests.get('https://global-warming.org/api/temperature-api')
    temperature_data = response.json()['result']

    for record in temperature_data:
        pg_cursor.execute(
            'INSERT INTO temperature_data (time, station, land) VALUES (%s, %s, %s)',
            (record['time'], record['station'], record['land'])
        )
    pg_conn.commit()
except Exception as e:
    print(f"An error occurred with the Global Warming API fetch: {e}")
    pg_conn.rollback()

# MONGODB FETCH


# @TODO make function as we need to convert nulls and non float types to float to fit the schema
def toFloat(s):
    try:
        return float(s) if s != "" else None
    except (ValueError, TypeError):
        return None

# @TODO func to clean up percentages and convert to float/numeric
def clean_percentage(perc):
    try:
        return float(perc.replace('%', '')) / 100 if perc and perc.endswith('%') else None
    except (ValueError, TypeError):
        return None

mongo_client=None
try:
    # MONGODB CONNECTION STRING
    mongo_client = pymongo.MongoClient('dontstealmypasswordanddata', tls=True,
    tlsAllowInvalidCertificates=True)
    mongo_db = mongo_client['GlobalWarmingByCountryDB']
    mongo_collection = mongo_db['emissions_data']
    emissions_data = mongo_collection.find()

    for record in emissions_data:
        pg_cursor.execute(mongoInsert, (
            record['Country'],
            toFloat(record.get('Fossil CO2 emissions 1990 (Mt CO2)')),
            toFloat(record.get('Fossil CO2 emissions 2005 (Mt CO2)')),
            toFloat(record.get('Fossil CO2 emissions 2017 (Mt CO2)')),
            clean_percentage(record.get('2017 (% of world)')),
            clean_percentage(record.get('2017 vs 1990: change (%)')),
            toFloat(record.get('Per land area (t CO2/km2/yr)')),
            toFloat(record.get('Per capita (t CO2/cap/yr)')),
            toFloat(record.get('Total including LUCF (Mt CO2)')),
            toFloat(record.get('Total excluding LUCF (Mt CO2)')),
        ))
    pg_conn.commit()
except Exception as e:
    print(f"mongoDB FETCH ERROR {e}")
    if pg_conn:
        pg_conn.rollback()
finally:
    if mongo_client:
      mongo_client.close()

# CSV DATA FETCH
try:
    df = pd.read_csv('./crude-oil-price.csv')

    for index, row in df.iterrows():
        pg_cursor.execute(
            'INSERT INTO oil_prices (date, price, percent_change, change) VALUES (%s, %s, %s, %s)',
            (pd.to_datetime(row['date']).strftime('%Y-%m-%d'), row['price'], row['percentChange'], row['change'])
        )
    pg_conn.commit()
except Exception as e:
    print("csv fetch error from 91: change ")
    pg_conn.rollback()

# REMEMBER TO CLOSE THE CONNECTIONS CARLOS
pg_cursor.close()
pg_conn.close()
