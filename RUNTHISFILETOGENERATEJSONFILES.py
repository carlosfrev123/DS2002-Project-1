import psycopg2
import json

# pSQL CONNECTION 
# you can steal this password and user - ur gonna need it to re generate the JSON files
conn = psycopg2.connect('postgres://wweudhzd:35Y84jhbI-O0xN-NvUd0NZOsPyyUWN7P@suleiman.db.elephantsql.com/wweudhzd')
cursor = conn.cursor()

# query to get avg temperature and oil price each year
PTquery = """
WITH temperature_avg AS (
    SELECT CAST(FLOOR(CAST(time AS FLOAT)) AS INTEGER) AS year, AVG(CAST(station AS FLOAT)) AS average_temperature
    FROM temperature_data
    GROUP BY year
),
oil_price_avg AS (
    SELECT EXTRACT(YEAR FROM date::date) AS year, AVG(price::numeric) AS average_price
    FROM oil_prices
    GROUP BY year
)
SELECT 
    t.year, 
    t.average_temperature, 
    o.average_price
FROM 
    temperature_avg t
JOIN 
    oil_price_avg o ON t.year = o.year
WHERE 
    t.year BETWEEN 1983 AND 2023
ORDER BY 
    t.year;
"""

# query to get average country emissions, price and temperature 90s 05 and 2017
ETPquery = """
WITH emissions_avg AS (
    SELECT 
        AVG(CAST(emissions_1990 AS FLOAT)) AS avg_emissions_1990, 
        AVG(CAST(emissions_2005 AS FLOAT)) AS avg_emissions_2005, 
        AVG(CAST(emissions_2017 AS FLOAT)) AS avg_emissions_2017
    FROM emissions_data
),
temperature_avg AS (
    SELECT 
        CAST(FLOOR(CAST(time AS FLOAT)) AS INTEGER) AS year, 
        AVG(CAST(station AS FLOAT)) AS average_temperature
    FROM temperature_data
    GROUP BY CAST(FLOOR(CAST(time AS FLOAT)) AS INTEGER)
    HAVING CAST(FLOOR(CAST(time AS FLOAT)) AS INTEGER) IN (1990, 2005, 2017)
),
oil_price_avg AS (
    SELECT 
        EXTRACT(YEAR FROM date::date) AS year, 
        AVG(price::numeric) AS average_price
    FROM oil_prices
    GROUP BY EXTRACT(YEAR FROM date::date)
    HAVING EXTRACT(YEAR FROM date::date) IN (1990, 2005, 2017)
)
SELECT 
    t.year,
    e.avg_emissions_1990,
    e.avg_emissions_2005,
    e.avg_emissions_2017,
    t.average_temperature,
    o.average_price
FROM 
    temperature_avg t
JOIN 
    oil_price_avg o ON t.year = o.year
CROSS JOIN 
    emissions_avg e;
"""
# try catch/ except finally 
# need to execute queries and read outputs into JSON files
# may need to set up some structure for the output into JSON
try:
    # QUERY 1 output
    cursor.execute(PTquery)
    yearROWS = cursor.fetchall()
    resultEYR = [
        {
            "year": row[0],
            "average_temperature": float(row[1]),
            "average_oil_price": float(row[2])
        } for row in yearROWS
    ]
    # JSON file output
    with open('PT.json', 'w') as json_file:
        json.dump(resultEYR, json_file, indent=4)
    print("JSON file for oil price and temp generated")

    # QUERY 2 output
    cursor.execute(ETPquery)
    rows_selected_years = cursor.fetchall()
    
    # for json output set mini schema
    years = {
        1990: {"average_emissions": None, "average_temperature": None, "average_oil_price": None},
        2005: {"average_emissions": None, "average_temperature": None, "average_oil_price": None},
        2017: {"average_emissions": None, "average_temperature": None, "average_oil_price": None}
    }
    
    for row in rows_selected_years:
        year = row[0]
        years[year]["average_emissions"] = float(row[1 if year == 1990 else 2 if year == 2005 else 3])
        years[year]["average_temperature"] = float(row[4])
        years[year]["average_oil_price"] = float(row[5])
    
    final_result = [dict(year=year, **data) for year, data in years.items()]

    # JSON file generation
    with open('ETP.json', 'w') as json_file:
        json.dump(final_result, json_file, indent=4)
    print("JSON file YTP generated")

except Exception as e:
    print(f"error with queries: {e}")

finally:
    cursor.close()
    conn.close()
