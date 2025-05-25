# Imports all the needed library
import os #library to interact with the operating system
import glob #library to interact with the data files
import psycopg2 #library to interact with the postgreSQL database system
import pandas as pd #library to inreact with structuring data
import json #library to interact with the json file
from sql_queries import * # the model contain related SQL functions
 

def process_song_file(cur, filepath):
    """
    Description: 
       - Description: This function helps to read and retrieve data from all JSON files in the song_data folder, assign the "df" as the               data frame to hold all the extracted data, then assign the "song_data" and "artist_data" as two lists carry only the needed data           for the table insertion, and then execute the inserting query from sql_queries to insert data from the song_data list to the               songs table and the artist_data list to the artists table in the database.
       
     Arguments:
         cur: the cursor object.
         filepath: the directory to the song_data folder.
         
     Returns:
         None   
     """
   
    df = pd.read_json(filepath,lines=True)
    
    # Assigns a song_data as a list
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # Assigns a artist_data as a list
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    
    

def process_log_file(cur, filepath):
    """
    Description: 
       - This function helps to read and retrieve data from all JSON files in the log_data folder. It assigns the "df" as the data frame to          hold all the extracted data, then assigns the "time_df" and "user_df" as two smaller data frames to have the needed data for the            table insertion, and then uses the for loop function in python to iterate each row of the data frame and execute the inserting              query from sql_queries to insert each row data from time_df to the time table and from user_df to the users table in the database.
       
       - The insertion process for the songplays table is started by passing the (song, artist, and length) from the df in the                      "song_select" query to retrieve the song_id and artist_id as the other two data to insert into the songplays table in the database          along with others data.
       
     Arguments:
         cur: the cursor object.
         filepath: the directory to the log_data folder.
         
     Returns:
         None   
     """
    # Read files
    df = pd.read_json(filepath,lines=True)

    # Re-assigned the df to filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # Converts the 'ts' column from integer to datetime datatype.
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = []
    for index, row in df.iterrows():
        # From each row in the df extract the value data and time in the 'ts' column.
        time_data.append([row['ts'],
                          row['ts'].hour,
                          row['ts'].day,
                          row['ts'].weekofyear,
                          row['ts'].month,
                          row['ts'].year,
                          row['ts'].weekday()])
        
    # Assigns needed data from the 'ts' column to the new list variable column_labels name.     
    column_labels = ['start_time','hour','day','week_of_year','month','year','weekday']
    # Converts the colunn_labels list into the new DataFrame time_df
    time_df = pd.DataFrame(time_data, columns=column_labels)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
       

    # Creates user_df for insert into the users table
    user_df = df[['userId','firstName','lastName','gender','level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        

    # Process to insert into the songplays table.
    for index, row in df.iterrows():
        # Iterate through df to get data for song, artist, and length then pass it into the song_select query
        cur.execute(song_select, (row.song, row.artist, row.length)) 
        # Uses the cur.fetchone() function to assign the above results (song_id and artist_id) to the variable results
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        # Assigns the songgplay_data as the list contains the extracted values needed for the insertion into the songplay table.
        songplay_data = [
            row['ts'],
            row['userId'],
            row['level'],
            songid,
            artistid,
            row['sessionId'],
            row['location'],
            row['userAgent']]
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: 
       -  This will create the connection to the sparkifydb, create the cursor to interact with the database, and then generate the                   process_song_file and process_log_file; at the end, close the connection with the database.

    Arguments:
       None

    Returns:
        None
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
   
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)

    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()