import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

scope = "user-library-read playlist-read-private"
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

def get_all_user_playlists():
    '''Retrieve all current-user playlists, public and private'''

    playlists = []
    offset = 0
    limit = 50  # Maximum limit per request
    while True:
        results = sp.current_user_playlists(limit=limit, offset=offset)
        playlists.extend(results['items'])
        offset += len(results['items'])
        if len(results['items']) < limit:
            break
    return playlists

def search_playlists_exact(query, limit=50):
    '''Search for playlists by name'''
    results = sp.search(q='playlist:"' + query + '"', type='playlist', limit=limit)
    playlists = results['playlists']['items']
    exact_match_playlists = [playlist for playlist in playlists if query in playlist['name']]

    return exact_match_playlists

def search_playlists(query, limit=50):
    '''Search for playlists by name'''
    results = sp.search(q=query, type='playlist', limit=limit)
    playlists = results['playlists']['items']
    match_playlists = [playlist for playlist in playlists if any(word in playlist['name'] for word in query.split())]

    return match_playlists

def get_user_playlists_pop():
    '''Get names and popularity of artists in all public user playlists, plus Liked Songs'''

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
    playlists = get_all_user_playlists()
    pub_playlists_items = [playlist for playlist in playlists if playlist['public']]
    all_tracks = get_tracks_from_playlist_set(pub_playlists_items, limit=limit, offset=offset, all_tracks=all_saved_tracks)

    return get_artists_from_tracks(all_tracks)

def get_spotify_featured_playlists_pop():
    '''Get names and popularity of artists in Spotify Featured Playlists'''
    featured_playlists = sp.featured_playlists()
    pub_playlists_items = [playlist for playlist in featured_playlists['playlists']['items'] if playlist['public']]
    all_tracks = get_tracks_from_playlist_set(pub_playlists_items)

    return get_artists_from_tracks(all_tracks)

def get_spotify_dscvr_wkly_playlists_pop():
    '''Get names and popularity of artists in Spotify Discover Weekly Playlists'''
    playlists = get_all_user_playlists()
    discover_weekly_playlists = [playlist for playlist in playlists if playlist['name'] == 'Discover Weekly']
    all_tracks = get_tracks_from_playlist_set(discover_weekly_playlists)

    return get_artists_from_tracks(all_tracks)

def get_spotify_new_rls_playlists_pop():
    '''Get names and popularity of artists in Spotify New Releases Playlists'''
    new_releases_playlists = search_playlists("New Releases", limit=50)
    spotify_playlists = [
        playlist for playlist in new_releases_playlists
        if playlist['owner']['id'] == 'spotify'
        and 'classical' not in playlist['name'].lower()
    ]
    all_tracks = get_tracks_from_playlist_set(spotify_playlists)

    return get_artists_from_tracks(all_tracks)

def get_given_playlist_pop(playlist_id):
    '''Get names and popularity of artists in for a given playlist from any user,
    public or private'''

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

if __name__ == "__main__":
    #artist_ids, artist_names, artist_pop = get_spotify_featured_playlists_pop()
    #artist_ids, artist_names, artist_pop = get_spotify_dscvr_wkly_playlists_pop()
    artist_ids, artist_names, artist_pop = get_spotify_new_rls_playlists_pop()
    #artist_ids, artist_names, artist_pop = get_user_playlists_pop()
    #artist_ids, artist_names, artist_pop = get_given_playlist_pop('0mUvry1g2VavEPfpRT6iuh')
    for name, pop in zip(artist_names, artist_pop):
        print(f"{name} - Popularity: {pop}")