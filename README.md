# Meteora
 
A Python script to collect Canadian weather station data from a Statistics Canada API.

## Usage

* Download the repository and install the required Python modules. 
* Create a database on your SQL Server and run the *sql/DDL.sql* script to create the destination table. 
* Edit the config.ini file to point to your SQL Server (with Windows built-in authentication). 
* Modify the CSV file *data/Station Inventory EN.csv* for the subset of stations and years you are interested in.
* Run the script. 

**The complete set of weather station data is very large, proceed with caution.**