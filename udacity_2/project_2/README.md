
# 1. Summary of the project for the Sparkify startup

- Learning the insight from the collected data about songs and user activity (which are .json files from the music streaming app). The insight from the data can help build the Machine Learning algorithm for the song suggestion based on each user's pattern and also help organize all valuable data for later use.

- **More detail:** The project is extracting data from each JSON file in the song_data (each song information) and the log_data (each day event in November of 2018 about the interaction of each user). The extracted data will be inserted into five tables inside the Sparkify database (using the PostgreSQL database) as the Star Schema relationship between tables:

1. (Table 1st dimension table) The songs table contains information about each song as song_id, title, artirst_id, year, and duration. 
2. (Table 2nd dimension table) The artist's table: contains information about each artist as artist_id, name, location, latitude, and longitude.
3. (Table 3rd dimension table) The user's table: contains information about each user as user_id, first_name, last_name, gender, and level (as free user or paid user).
4. (Table 4th dimension table) The time table contains the information about start_time, hour, day, week, month, and year for each event on each specific day relating to each user, song, and artist information. 
5. (table 5th fact table): The songplays table shows the whole related information as the correlation from the above four tables: start_time, user_id, level, song_id, artist_id, session_id, location, and user_agent. 

# 2. Running the Python scripts

Implementing all Python files in the terminal:

- Run the create_tables.py including:
    - create_database(): Function to create a database "sparkifydb" and create a connection to that database
    - drop_tables(): Function to drop tables if existing and calling the script from the sql_queries.py to generate this function.
    - create_tables(): Function to create new tables. Calling the script from the sql_queries.py to generate this function.
    
- Run the etl.py including:
    - process_song_file(): This function retrieves the data from each JSON file in the song_data folder, then inserts those data into the songs table and the artist's table.
    - process_log_file(): This function retrieves the data from each JSON file in the log_data folder, then inserts those data into the time table, users, and the songplays table.
    - process_data(): This will implement all the files from the data file.

# 3. Files in the repository 

The data folder contains the song_data folder and log_data folder:

- song_data folder: contains JSON files, each file containing all information describing each song.

- log_data folder: contains JSON files, each file containing all information related to each day in November of 2018.





