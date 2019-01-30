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

    conn.commit()


#re.sub(r'([^\s\w]|_)+', '', row[1])  only retains characters that are alphanum or whitespace

# (\([^()]+\)) capture group with ( match anything that isnt ( or ) then )

#compare one single playlist against another for similarity
def find_similarity(source_list, compare_list):

    #hash maps store (song, artist) tuples as keys
    src_dict = {}
    cmp_dict = {}
    main_dict = {}

    # store frequency of each term in corresponding dict. main is union of both
    for item in source_list:
        pair = (item[1], item[2])
        if not pair in src_dict:
            src_dict[pair] = 1
        if not pair in main_dict:
            main_dict[pair] = 1

    for item in compare_list:
        pair = (item[1], item[2])
        if not pair in cmp_dict:
            cmp_dict[pair] = 1
        if not pair in main_dict:
            main_dict[pair] = 1

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
    cs = cosine_similarity(cmp_array, src_array)[0][0]
    return 100 * cs * cs / len(compare_list)


#perform a batch job of similarity comparisons between playlists, returns map of results
def find_most_similar(source_list, compare_lists, quantity):
    playlist_scores = {}
    for i in range(0,len(compare_lists)):
        playlist_scores[compare_lists[i][0][0]] = find_similarity(source_list, compare_lists[i])

    return playlist_scores


#compare playlists and display most similar to user in ranked order
def show_similar_playlists(source_list, compare_lists, quantity):
    comparison_results = []
    #store tuples containing id and similarity score of each list to the source list
    for i in range(0,len(compare_lists)):
        comparison_results.append(
            (compare_lists[i][0][0], find_similarity(source_list, compare_lists[i]))
        )
    comparison_results = sorted(comparison_results, reverse =True, key=lambda x: x[1])

    print "similarity of ref_songs to temp_songs: "
    for item in comparison_results[0:quantity]:
        print "playlist ID:",item[0], "score:", item[1]


def get_best_songs(ref_playlist, cmp_playlists, quantity):
    song_score = {}
    scored_playlists = find_most_similar(ref_playlist, cmp_playlists, quantity)

    #sum together the frequencies of each song multiplied by their respective score coefficient
    # song score = (0.2653)1 + (0.7829)1 + ...
    #so the songs which occur most frequently and in the most relevant playlists will float to the top

    for playlist in cmp_playlists:
        for song_obj in playlist:
            pair =(song_obj[1],song_obj[2]) #(track, artist)
            if pair in song_score:
                song_score[pair] += scored_playlists[song_obj[0]]
            else:
                song_score[pair] = scored_playlists[song_obj[0]]

    #filter out every song already provided in the source playlist
    for song_obj in ref_playlist:
        pair = (song_obj[1], song_obj[2])
        if pair in song_score:
            del song_score[pair]

    best_songs = song_score.items()
    return sorted(best_songs, reverse=True, key=lambda x: x[1])[0:quantity]


#convert query object  into list, then make a list of sublists grouped by userID to create "playlists"
def bundle_query_to_playlist(q):
    query_list = list(q.fetchall())
    # group all rows from database into lists by userID
    playlist_list = [[]]
    j = 0
    for i in range(0, len(query_list)):
        if i > 0:
            if query_list[i][0] == query_list[i - 1][0]:  # if the userID has changed
                playlist_list[j].append(query_list[i])  # append next row into same list
            else:  # userID not matched
                playlist_list.append([query_list[i]])  # begin new array in playlist[j]
                j += 1
        else:
            playlist_list[j].append(query_list[i])

    return playlist_list

def main():

    #databases SongTable table follows column format (int userID, text song, text artist, text details)
    database_path ="/home/user/Projects/musicpredict/songs.db";

    conn = create_connection(database_path)
    #filter_details(conn) # use this initially to clean dataset in database

    best_songs= []
    num_results = 20
    query = conn.execute("SELECT userID, song, artist FROM SongTable where userID = 0;");

    max_index = conn.execute("SELECT MAX(userID) from SongTable;").fetchall()[0][0];
    source_playlist = bundle_query_to_playlist(query)[0]

    block = 10000
    # process in ranges of 10,000 at a time to conserve memory
    for i in range(0,max_index/block +1):
        query = conn.execute("SELECT userID, song, artist FROM SongTable where userID > ? and userID <= ? order by userID;", (i*block, (i+1)*block));
        print "checkpoint - finished round ", i
        bulk_playlists = bundle_query_to_playlist(query)
        best_songs += get_best_songs(source_playlist, bulk_playlists, num_results)

    best_songs = sorted(best_songs, reverse=True, key=lambda x: x[1])[0:num_results*5]
    for song_obj in best_songs:
         print "SONG:", song_obj[0][0], "ARTIST:", song_obj[0][1], "SCORE:", song_obj[1]



    #TODO:  write functions to find most recommended artist instead of song

if __name__ == '__main__':
    main()
