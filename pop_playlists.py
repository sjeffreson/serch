import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

scope = "user-library-read"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def get_artists_from_tracks(tracks, batch_size=50):
    # Extract artist IDs, names, popularity from saved tracks
    artist_ids = []
    for item in tracks:
        track = item['track']
        if track is None:
            continue
        for artist in track['artists']:
            artist_ids.append(artist['id'])
    artist_ids = list(set(artist_ids))

    # Fetch artist information in batches, according to API rate limits
    artist_names, artist_pop = [], []
    for i in range(0, len(artist_ids), batch_size):
        batch_artist_ids = list(artist_ids)[i:i + batch_size]
        artists_info = sp.artists(batch_artist_ids)['artists']
        for artist_info in artists_info:
            artist_id = artist_info['id']
            artist_names.append(artist_info['name'])
            artist_pop.append(artist_info['popularity'])

    return artist_ids, artist_names, artist_pop

def get_tracks_from_playlist_set(playlists_items, limit=50, offset=0, batch_size=50, all_tracks=[]):
    for playlist in playlists_items:
        print(f"Playlist: {playlist['name']}")
        offset = 0
        while True:
            playlist_tracks = sp.playlist_tracks(playlist['id'], limit=limit, offset=offset)
            all_tracks.extend(playlist_tracks['items'])
            offset += limit
            if len(playlist_tracks['items']) < limit:
                break  # Reached the end of saved tracks

    return all_tracks

def get_user_playlists_pop():

    # Retrieve all saved tracks for the current user by handling
    all_saved_tracks = []
    offset = 0
    limit = 50  # Maximum limit per request

    print(f"Playlist: Liked songs")
    while True:
        playlist_tracks = sp.current_user_saved_tracks(limit=limit, offset=offset)
        all_saved_tracks.extend(playlist_tracks['items'])
        offset += limit
        if len(playlist_tracks['items']) < limit:
            break  # Reached the end of saved tracks

    # same for all of my playlists
    playlists = sp.current_user_playlists()
    pub_playlists_items = [playlist for playlist in playlists['items'] if playlist['public']]
    all_tracks = get_tracks_from_playlist_set(pub_playlists_items, limit=limit, offset=offset, all_tracks=all_saved_tracks)

    return get_artists_from_tracks(all_tracks)

def get_spotify_featured_playlists_pop():

    # Retrieve featured playlists
    featured_playlists = sp.featured_playlists()
    pub_playlists_items = [playlist for playlist in featured_playlists['playlists']['items'] if playlist['public']]
    all_tracks = get_tracks_from_playlist_set(pub_playlists_items)

    return get_artists_from_tracks(all_tracks)

def get_given_playlist_pop(playlist_id):

    # Retrieve all saved tracks for the current user by handling
    all_saved_tracks = []
    offset = 0
    limit = 50  # Maximum limit per request

    while True:
        playlist_tracks = sp.playlist_tracks(playlist_id, limit=limit, offset=offset)
        all_saved_tracks.extend(playlist_tracks['items'])
        offset += limit
        if len(playlist_tracks['items']) < limit:
            break  # Reached the end of saved tracks

    return get_artists_from_tracks(all_saved_tracks)

def fetch_random_artists(num_artists=1000):
    all_artists = []
    offset = 0
    limit = 50  # Adjust the limit as needed

    while True:
        # Perform search query to fetch artists
        results = sp.search(q='a', type='artist', limit=limit, offset=offset)
        artists = results['artists']['items']
        all_artists.extend(artists)
        offset += limit
        if (len(artists) < limit) or (len(all_artists) >= num_artists):
            break
    
    results = [artist for artist in results if artist['type'] == 'artist']

    return all_artists

def get_random_artists_pop(num_artists=1000):
    all_artists = fetch_random_artists(num_artists)
    artist_ids = [artist['id'] for artist in all_artists]
    artist_names = [artist['name'] for artist in all_artists]
    artist_pop = [artist['popularity'] for artist in all_artists]

    return artist_ids, artist_names, artist_pop

if __name__ == "__main__":
    #artist_ids, artist_names, artist_pop = get_spotify_featured_playlists_pop()
    #artist_ids, artist_names, artist_pop = get_user_playlists_pop()
    #artist_ids, artist_names, artist_pop = get_given_playlist_pop('0mUvry1g2VavEPfpRT6iuh')
    artist_ids, artist_names, artist_pop = get_random_artists_pop()
    for name, pop in zip(artist_names, artist_pop):
        print(f"{name} - Popularity: {pop}")