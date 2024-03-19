# Whale Sightings

#### Data engineering project focused on processing and managing whale sighting data. It sends requests to the Ocean Biodiversity Information System's API to extract relevant data, then validates, processes and cleans it, and finally stores it in a MySQL database.


### Tools

* _Python_
* _Docker_
* _Pandas_
* _GeoPandas_
* _Pydantic_
* _MySQL_
* _Typer_


### Instructions
#### Step 1 Build the etl and db services:
Using a terminal and in the root project directory, run the command:
```bash
$ docker compose build
```

#### Step 2 Start up db service container in detached mode to receive data:
```bash
$ docker compose up -d db
```

#### Step 3 Run etl service container script (removing it after it exits):
```
$ docker compose run --rm etl beluga_whale
Running pipeline
[INFO ][2024-03-19 19:06:16,813][obis:0093] : Getting records for beluga_whale
[INFO ][2024-03-19 19:06:18,840][obis:0106] : Total Records: 5222
[INFO ][2024-03-19 19:06:18,840][obis:0146] : Sending /occurrence request for 1932-01-01-2021-12-31
[INFO ][2024-03-19 19:06:27,386][obis:0174] : Saving json response to data/beluga_whale/1932-01-01--2021-12-31.json
[INFO ][2024-03-19 19:06:29,578][validate:0195] : Validated: 5216, Errors: 6
[INFO ][2024-03-19 19:06:30,481][cleaner:0325] : 6/6 errors processed
[INFO ][2024-03-19 19:06:30,514][cleaner:0368] : 1170 duplicate rows removed
[INFO ][2024-03-19 19:06:30,519][cleaner:0029] : Loading ocean shapefile..
[INFO ][2024-03-19 19:07:28,548][cleaner:0216] : Performing geodata operations..
[INFO ][2024-03-19 19:08:02,669][cleaner:0409] : Saving dataframe to data/beluga_whale/1932-10-13--2021-08-21.csv
[INFO ][2024-03-19 19:08:02,889][storage:0034] : Creating MySQL connection..
[INFO ][2024-03-19 19:08:02,898][storage:0145] : Inserting rows.
[INFO ][2024-03-19 19:08:28,230][storage:0154] : Inserts completed.
[INFO ][2024-03-19 19:08:28,231][storage:0050] : Connection closed.
```

#### Step 4 View data in MySQL database:
Open the db service container's shell
```
$ docker compose exec db /bin/bash
```

Login to MySQL
```
bash-4.4# mysql -p<yourpassword>
```

Select the database
```
mysql> use whale_sightings;
```

Query a table
```
mysql> SELECT * FROM species;

+--------+-----------------------+----------------+
| id     | speciesName           | vernacularName |
+--------+-----------------------+----------------+
| 137090 | Balaenoptera musculus | Blue Whale     |
| 137115 | Delphinapterus leucas | Beluga Whale   |
| 137116 | Monodon monoceros     | Narwhal        |
+--------+-----------------------+----------------+
3 rows in set (0.00 sec)

mysql> SELECT * FROM locations;

+----+----------------------+
| id | waterBody            |
+----+----------------------+
|  0 | NULL                 |
|  1 | Arctic Ocean         |
|  5 | Indian Ocean         |
|  8 | Mediterranean Region |
|  3 | North Atlantic Ocean |
|  2 | North Pacific Ocean  |
|  6 | South Atlantic Ocean |
|  7 | South Pacific Ocean  |
|  4 | Southern Ocean       |
+----+----------------------+
9 rows in set (0.01 sec)
```

#### Step 5 Stop services and remove containers once finished:
```
docker compose down
```

## Citations
OBIS 2024 Ocean Biodiversity Information System.  
Intergovernmental Oceanographic Commission of UNESCO.  
[www.obis.org](www.obis.org.)  

Flanders Marine Institute (2021). Global Oceans and Seas, version 1.  
Available online at https://www.marineregions.org/. https://doi.org/10.14284/542.


## License
[MIT](https://github.com/jarretjeter/whale-sightings/blob/main/LICENSE.txt)

Contact me here on github or at jarretjeter@gmail.com if you have any questions or issues.