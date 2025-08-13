import json
import requests
import time
from pathlib import Path
from datetime import datetime

def get_spotify_image_url(track_id):
    url = f"https://open.spotify.com/oembed?url=spotify:track:{track_id}"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
    data = response.json()
    return data["thumbnail_url"]

def extract_track_id_from_uri(track_uri):
    if track_uri and track_uri.startswith("spotify:track:"):
        return track_uri.replace("spotify:track:", "")
    return None

def main():
    print(" Cover Art URL Updater")
    print("=" * 50)
    
    metadata_folder = Path("consolidated_music/metadata")
    songs_db_path = metadata_folder / "songs_database.json"
    
    if not songs_db_path.exists():
        print(f" Songs database not found at: {songs_db_path}")
        return
    
    print(f" Loading songs database...")
    
    with open(songs_db_path, 'r', encoding='utf-8') as f:
        database = json.load(f)
    
    songs_dict = database.get('songs', {})
    total_songs = len(songs_dict)
    
    print(f" Total songs in database: {total_songs}")
    
    songs_without_cover = []
    songs_with_cover = 0
    
    for song_id, song_data in songs_dict.items():
        metadata = song_data.get('metadata', {})
        cover_art_url = metadata.get('cover_art_url', '').strip()
        
        if not cover_art_url:
            songs_without_cover.append((song_id, song_data))
        else:
            songs_with_cover += 1
    
    print(f" Songs with cover art: {songs_with_cover}")
    print(f" Songs without cover art: {len(songs_without_cover)}")
    
    if not songs_without_cover:
        print(" All songs already have cover art URLs!")
        return
    
    print(f"\n Preview of songs needing cover art (first 3):")
    for i, (song_id, song_data) in enumerate(songs_without_cover[:3], 1):
        metadata = song_data.get('metadata', {})
        track_name = metadata.get('track_name', 'Unknown')
        artists_string = metadata.get('artists_string', 'Unknown')
        track_uri = metadata.get('track_uri', 'No URI')
        
        print(f"   {i}. {track_name} by {artists_string}")
        print(f"      Track URI: {track_uri}")
    
    confirm = input(f"\n Add cover art URLs for {len(songs_without_cover)} songs? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print(" Operation cancelled")
        return
    
    backup_path = songs_db_path.with_suffix('.backup.json')
    import shutil
    shutil.copy2(songs_db_path, backup_path)
    print(f" Created backup: {backup_path}")
    
    print(f"\n Processing {len(songs_without_cover)} songs...")
    
    updated_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, (song_id, song_data) in enumerate(songs_without_cover, 1):
        metadata = song_data.get('metadata', {})
        track_name = metadata.get('track_name', 'Unknown')
        artists_string = metadata.get('artists_string', 'Unknown')
        track_uri = metadata.get('track_uri', '')
        
        print(f"\n [{i}/{len(songs_without_cover)}] {track_name} by {artists_string}")
        
        track_id = extract_track_id_from_uri(track_uri)
        
        if not track_id:
            print(f"     No valid track URI - skipping")
            skipped_count += 1
            continue
        
        print(f"    Track ID: {track_id}")
        
        try:
            cover_art_url = get_spotify_image_url(track_id)
            
            song_data['metadata']['cover_art_url'] = cover_art_url
            song_data['metadata']['cover_art_updated_at'] = datetime.now().isoformat()
            updated_count += 1
            print(f"    Added: {cover_art_url}")
            
        except Exception as e:
            failed_count += 1
            print(f"    Failed: {e}")
        
        time.sleep(0.5)
        
        if i % 10 == 0:
            print(f"    Saving progress...")
            database['last_updated'] = datetime.now().isoformat()
            with open(songs_db_path, 'w', encoding='utf-8') as f:
                json.dump(database, f, indent=2, ensure_ascii=False)
    
    database['last_updated'] = datetime.now().isoformat()
    database['cover_art_update_summary'] = {
        'last_update': datetime.now().isoformat(),
        'songs_updated': updated_count,
        'songs_failed': failed_count,
        'songs_skipped': skipped_count,
        'total_processed': len(songs_without_cover)
    }
    
    with open(songs_db_path, 'w', encoding='utf-8') as f:
        json.dump(database, f, indent=2, ensure_ascii=False)
    
    print(f"\n Summary:")
    print(f"    Updated: {updated_count}")
    print(f"    Failed: {failed_count}")
    print(f"     Skipped: {skipped_count}")
    print(f"    Database saved!")

if __name__ == "__main__":
    main()
