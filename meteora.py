import requests
import petl
import pymssql
import configparser
import csv

#get data from config file
config = configparser.ConfigParser()

try:
    config.read('config.ini')
except Exception as e:
    print(e)

# get station list
stationsTable = petl.io.csv.fromcsv('data/Station Inventory En.csv')
server = config['DEFAULT']['server']
database = config['DEFAULT']['database']

# get station data from petl table
stations = petl.util.base.dicts(stationsTable)
for station in stations:
    # get station ID
    stationID = station['Station ID']
    # get years for station from petl table
    first_year = station['First Year']
    last_year = station['Last Year']
    # get data for each year
    for year in range(int(first_year), int(last_year)+1):
        # get data for each month
        for month in range(1, 13):
                print('processing ' + station['Station ID'] + ' ' + str(year) + '-' + str(month) + '...')
                url = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=%s&Year=%s&Month=%s&timeframe=2&submit=Download+Data' % (station['Station ID'], year, month)
                data = requests.get(url).text
                # petl doesn't like dealing with CSVs in memory, so we'll use the csv module to read the data into a dictionary
                csv_data = csv.DictReader(data.splitlines())
                conn = pymssql.connect(server=server, database=database)
                cursor = conn.cursor()
                # insert data into SQL database
                for row in csv_data:
                    # create SQL query
                    query = 'INSERT INTO weather.dbo.observations (%s) VALUES (%s)' % ('climate_id, [name], longitude, latitude, observation_date, max_temp, min_temp, rain_mm, snow_mm, precip_mm, snow_accumulation, max_gust', " try_cast('" + str(row['Climate ID'])+"' as varchar(20)),try_cast('" + str(row['Station Name'])+"' as varchar(255)),try_cast('" + str(row['Longitude (x)'])+"' as float), try_cast('" + str(row['Latitude (y)'])+"' as float),try_cast('" + str(row['Date/Time'])+"' as date),try_cast('" + str(row['Max Temp (°C)'])+"' as float),try_cast('" + str(row['Min Temp (°C)'])+"' as float), try_cast('" + str(row['Total Rain (mm)']) +"' as float),try_cast('" + str(row['Total Snow (cm)']) +"' as float),try_cast('" + str(row['Total Precip (mm)'])+"' as float),try_cast('" + str(row['Snow on Grnd (cm)'])+"' as float),try_cast('" + str(row['Spd of Max Gust (km/h)']) + "' as float)")
                    # execute query
                    cursor.execute(query, tuple(row.values()))
                    # commit changes
                    conn.commit()
                # close connection
                conn.close()