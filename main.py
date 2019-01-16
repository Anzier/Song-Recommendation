import sqlite3
from sqlite3 import Error
import re
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def create_connection(db_file):
    try:
        connect = sqlite3.connect(db_file)
        return connect
    except Error as e:
        print(e)
    return None


#function retrieves, modifies, and replaces rows of database to simplify name and artist columns
# removes text contained in (*) or [*] and inserts into the details column
def filter_details(conn):
    query = conn.execute("SELECT * FROM SongTable where song like '%(%)%' or song like '%[%]%' or artist like '%(%)%' or artist like '%[%]%';");
    fixed_rows = []

    for row in query:
        # regex: split text with brackets and parentheses from remaining text
        # example: "new [track 5] song (instrumental)" ->  ["new","song","[track 5]","(instrumental)"]
        song_split = re.split(r'(\([^()]+\))|(\[[^[]+\])', row[1])
        details = row[3]
        song = ''
        for i in song_split:
            if i is not None:
                if len(i) > 0 and (i[0] == '(' or i[0] == '['):
                    details += i
                elif len(i) > 0:
                    song += i

        artist_split = re.split(r'(\([^()]+\))|(\[[^[]+\])', row[2])
        artist = ''
        for i in artist_split:
            if i is not None:
                if len(i) > 0 and (i[0] == '(' or i[0] == '['):
                    details += i
                elif len(i) > 0:
                    artist += i
        fixed_rows.append([row[0], song.strip(), artist.strip(), details.strip()])
    conn.execute("DELETE FROM SongTable where song like '%(%)%' or song like '%[%]%' or artist like '%(%)%' or artist like '%[%]%';");

    for item in fixed_rows:
        insert_statement = "insert into SongTable (userID, song, artist, details) values (?, ?, ?, ?);";
        conn.execute(insert_statement, (item[0], item[1], item[2], item[3]));


#re.sub(r'([^\s\w]|_)+', '', row[1])  only retains characters that are alphanum or whitespace

# (\([^()]+\)) capture group with ( match anything that isnt ( or ) then )

def find_similarity(source_list, compare_list):
    src_dict = {}
    cmp_dict = {}
    main_dict = {}

    # store frequency of each term in corresponding dict. main is union of both
    for item in source_list:
        if not item in src_dict:
            src_dict[item] = 1
        if not item in main_dict:
            main_dict[item] = 1

    for item in compare_list:
        if not item in cmp_dict:
            cmp_dict[item] = 1
        if not item in main_dict:
            main_dict[item] = 1

    src_list = []
    cmp_list = []
    for key, value in main_dict.iteritems():
        if key in cmp_dict.keys():
            cmp_list.append(1)
        else:
            cmp_list.append(0)
        if key in src_dict.keys():
            src_list.append(1)
        else:
            src_list.append(0)

    src_array = np.array([src_list])
    cmp_array = np.array([cmp_list])

    return cosine_similarity(cmp_array, src_array)[0][0]


def find_most_similar(source_list, compare_lists, quantity):
    comparison_results = []
    #store tuples containing id and similarity score of each list to the source list
    for i in range(0,len(compare_lists)):
        comparison_results.append(
            (i, find_similarity(source_list, compare_lists[i]))
        );
    return sorted(comparison_results, reverse =True, key=lambda x: x[1])

def main():
    database ="/home/nebrya/PycharmProjects/musicpredict/songs.db";

    # create a database connection
    conn = create_connection(database)

    #filter_details(conn)


    query = conn.execute("SELECT userID, song, artist FROM SongTable order by userID limit 500;");
    query_list = list(query.fetchall())


    #group all rows into lists by userID
    playlist_list = [[]]
    j = 0
    for i in range(0, len(query_list)):
        if i > 0:
            if query_list[i][0] == query_list[i-1][0]:#if the userID has changed
                playlist_list[j].append(query_list[i])#append next row into same list
            else:#userID not matched
                playlist_list.append([query_list[i]]) #begin new array in playlist[j]
                j += 1
        else:
            playlist_list[j].append(query_list[i])


    #fixed_rows = []

    #for row in query:
    #    aa= row[0]

    ref_songs = ["why", "should", "I", "cry", "when", "angels", "deserve", "to", "die"]
    temp_songs = [["what", "is", "not", "wrong", "with", "this", "to", "die"],
                  ["this","is","the","time"],
                  ["where","should","we"],
                  ["why","not"],
                  ["what","the","fuck","is","going","on","right","here"]
                 ]

    print "similarity of ref_songs to temp_songs: "
    for item in find_most_similar(playlist_list[1], playlist_list[2:], 5):
        print "playlist ID:",item[0], "score:", item[1]


    conn.commit()


    #TODO: attempt with database, iteratively pull small loads into memory and process scores
    #ensure query either pulls one id at a time, or replace iterator id with database id

    #once it works with entire database and finds most similar playlists ranked descending,
    #tally up the most commmonly occurring songs out of the top n playlists, list those songs descending

if __name__ == '__main__':
    main()
