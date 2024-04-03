import pandas as pd
from extract import extract_data


def transform():
    recently_played = extract_data()
    album_list = []
    for row in recently_played['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                         'total_tracks': album_total_tracks, 'url': album_url}
        album_list.append(album_element)

    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    for item in recently_played['items']:
        for key, value in item.items():
            if key == "track":
                for data_point in value['artists']:
                    id_list.append(data_point['id'])
                    name_list.append(data_point['name'])
                    url_list.append(data_point['external_urls']['spotify'])
    artist_dict = {'artist_id': id_list, 'name': name_list, 'url': url_list}
    # Creating the Track(Song) Data Structure:
    song_list = []
    for row in recently_played['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_time_played = row['played_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                        'popularity': song_popularity, 'date_time_played': song_time_played, 'album_id': album_id,
                        'artist_id': artist_id
                        }
        song_list.append(song_element)
    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])
    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    # Song Dataframe
    song_df = pd.DataFrame.from_dict(song_list)
    # date_time_played is an object (data type) changing to a timestamp
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    # Convertir les timestamps en chaînes de caractères
    song_df['date_time_played'] = song_df['date_time_played'].dt.strftime('%Y-%m-%d %H:%M:%S')
    song_df['unique_identifier'] = song_df['song_id'] + "-" + song_df['date_time_played']
    # Sélection des colonnes nécessaires
    song_df = song_df[
        ['unique_identifier', 'song_id', 'song_name', 'duration_ms', 'url', 'popularity', 'date_time_played',
         'album_id', 'artist_id']]
    #print(song_df['date_time_played'])
    song_df.to_csv(r"/Users/macbookair/Documents/data_ingenieur/test.csv")
    return song_df,album_df, artist_df
#print(transform()[1])
