import spotipy
from spotipy.oauth2 import SpotifyOAuth

import sys

def extract_data():
    spotify_client_id = ""
    spotify_client_secret = ""
    spotify_redirect_url = "http://localhost:8080"

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                   client_secret=spotify_client_secret,
                                                   redirect_uri=spotify_redirect_url,
                                                   scope="user-read-recently-played"))
    recently_played = sp.current_user_recently_played(limit=50)
    if len(recently_played) == 0:
        sys.exit("No results recieved from Spotify")
    else:
       return recently_played
#print(extract_data())


