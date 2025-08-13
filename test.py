import requests

def get_spotify_image_url(track_id):
    url = f"https://open.spotify.com/oembed?url=spotify:track:{track_id}"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
    data = response.json()
    return data["thumbnail_url"]

if __name__ == "__main__":
    track_id = input("Enter Spotify track ID: ").strip()
    image_url = get_spotify_image_url(track_id)
    print(f"Image URL: {image_url}")
