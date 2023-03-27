import requests
import petl
import pymssql
import configparser

#get data from config file
config = configparser.ConfigParser()

# get station list
stationsTable = petl.io.csv.fromcsv('data/Station Inventory En.csv')
try:
    config.read('config.ini')
except Exception as e:
    print(e)

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
                url = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=%s&Year=%s&Month=%s&timeframe=2&submit=Download+Data' % (station['Station ID'], year, month)
                data = requests.get(url).text
                # write data to a temporary file 
                with open('data/temp.csv', 'w') as f:
                    f.write(data)
                # strip empty lines from file
                with open('data/temp.csv', 'r') as f:
                    lines = f.readlines()
                with open('data/temp.csv', 'w') as f:
                    for line in lines:
                        if line.strip():
                            f.write(line)
                # write data to a file - output to file is necessary because petl fails to read data from a string
                fileData = petl.io.csv.fromcsv('data/temp.csv')
                # write data to SQL database
                # initiate connection
                conn = pymssql.connect(server='localhost', database='weather')
                cursor = conn.cursor()
                # create petl table from data in temporary file
                fileData = petl.io.csv.fromcsv('data/temp.csv')
                # slice table to remove rows we don't need
                cutTable = petl.transform.basics.cut(fileData,'Climate ID','Station Name','Longitude (x)','Latitude (y)','Date/Time','Max Temp (°C)','Min Temp (°C)','Total Rain (mm)','Total Snow (cm)','Total Precip (mm)','Snow on Grnd (cm)','Spd of Max Gust (km/h)')
                # get data
                outputData = petl.util.base.dicts(cutTable)
                # insert data into SQL database
                for row in outputData:
                    # create SQL query
                    query = 'INSERT INTO weather.dbo.observations (%s) VALUES (%s)' % ('climate_id, [name], longitude, latitude, observation_date, max_temp, min_temp, rain_mm, snow_mm, precip_mm, snow_accumulation, max_gust', "'" + str(row['Climate ID'])+"','" + str(row['Station Name'])+"','" + str(row['Longitude (x)'])+"','" + str(row['Latitude (y)'])+"','" + str(row['Date/Time'])+"','" + str(row['Max Temp (°C)'])+"','" + str(row['Min Temp (°C)'])+"','" + str(row['Total Rain (mm)']) +"','" + str(row['Total Snow (cm)']) +"','" + str(row['Total Precip (mm)'])+"','" + str(row['Snow on Grnd (cm)'])+"','" + str(row['Spd of Max Gust (km/h)']) + "'")
                    # execute query
                    cursor.execute(query, tuple(row.values()))
                    # commit changes
                    conn.commit()
                # close connection
                conn.close()