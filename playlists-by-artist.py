import spotipy
from spotipy.oauth2 import SpotifyOAuth

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

'''
Snippet from ChatGPT to get (mainly) user-created playlists containing
a specific artist by ignoring all playlists owned by Spotify or known
entities.
'''

# Authenticate with Spotify's API
client_credentials_manager = SpotifyClientCredentials(client_id='YOUR_CLIENT_ID', client_secret='YOUR_CLIENT_SECRET')
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Search for the artist
artist_name = 'ARTIST_NAME'
results = sp.search(q='artist:' + artist_name, type='artist')
artist = results['artists']['items'][0]

# Retrieve playlists containing the artist
playlists = sp.artist_playlists(artist['id'])

# Print information about each user-created playlist
for playlist in playlists['items']:
    # Check if the playlist owner is not Spotify or a known entity
    if playlist['owner']['id'] != 'spotify':
        print('Playlist Name:', playlist['name'])
        print('Playlist ID:', playlist['id'])
        print('Owner:', playlist['owner']['display_name'])
        print('---')