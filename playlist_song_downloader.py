#!/usr/bin/env python3
"""
Playlist Song Downloader
========================
This script takes a playlist name, finds all songs from that playlist,
checks if they exist locally, and downloads any missing songs.
"""

import json
import os
import sys
import subprocess
import time
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple

# === CONFIGURATION ===
class Config:
    # Download settings
    AUDIO_QUALITY = '192K'
    MAX_RETRIES = 3
    DOWNLOAD_DELAY = 1  # Seconds between downloads
    
    # Paths
    CONSOLIDATED_FOLDER = "consolidated_music"
    SONGS_FOLDER = os.path.join(CONSOLIDATED_FOLDER, "songs")
    METADATA_FOLDER = os.path.join(CONSOLIDATED_FOLDER, "metadata")
    
    # Database files
    SONGS_DB_FILE = os.path.join(METADATA_FOLDER, "songs_database.json")
    PLAYLISTS_DB_FILE = os.path.join(METADATA_FOLDER, "playlists_database.json")
    MAPPING_DB_FILE = os.path.join(METADATA_FOLDER, "song_playlist_mapping.json")

# === UTILITY FUNCTIONS ===
def install_required_packages():
    """Install required packages if not available"""
    try:
        import yt_dlp
        print("✅ yt-dlp is available")
    except ImportError:
        print("📦 Installing yt-dlp...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yt-dlp"])
        print("✅ yt-dlp installed successfully")
    
    try:
        import requests
        print("✅ requests is available")
    except ImportError:
        print("📦 Installing requests...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
        print("✅ requests installed successfully")

def check_prerequisites():
    """Check if required tools are available"""
    print("🔧 Checking prerequisites...")
    
    # Check ffmpeg
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("   ✅ ffmpeg found")
        else:
            print("   ❌ ffmpeg not working properly")
            return False
    except FileNotFoundError:
        print("   ❌ ffmpeg not found - please install ffmpeg")
        print("      Download from: https://ffmpeg.org/download.html")
        return False
    
    install_required_packages()
    return True

def sanitize_filename(filename):
    """Remove invalid characters from filename"""
    try:
        if not filename or not str(filename).strip():
            return "unknown_file"
        
        filename = str(filename).strip()
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filename = re.sub(r'[^\w\s-]', '', filename)
        filename = re.sub(r'[-\s]+', '-', filename)
        result = filename.strip('-')[:100]
        
        return result if result else "unknown_file"
    except Exception as e:
        print(f"   ⚠️  Error sanitizing filename '{filename}': {e}")
        return "unknown_file"

def load_json_file(file_path: str) -> dict:
    """Load JSON file with error handling"""
    try:
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return {}
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return {}

def save_json_file(file_path: str, data: dict):
    """Save JSON file with error handling"""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"❌ Error saving {file_path}: {e}")
        return False

def download_song(track_name: str, artists_string: str, song_id: str, output_folder: str) -> bool:
    """Download a song using yt-dlp"""
    try:
        import yt_dlp
        
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Create search query
        search_query = f"{track_name} {artists_string}"
        
        # Configure yt-dlp options for MP3 download only
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_folder, f'{song_id}.%(ext)s'),
            'extractaudio': True,
            'audioformat': 'mp3',
            'audioquality': Config.AUDIO_QUALITY,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': Config.AUDIO_QUALITY,
            }],
            'quiet': True,
            'no_warnings': True
        }
        
        print(f"   🔍 Searching for: {search_query}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Search for the song
            info = ydl.extract_info(f"ytsearch1:{search_query}", download=True)
            
            if info and 'entries' in info and len(info['entries']) > 0:
                entry = info['entries'][0]
                print(f"   ✅ Downloaded: {entry.get('title', 'Unknown')}")
                return True
            else:
                print(f"   ❌ No results found for: {search_query}")
                return False
                
    except Exception as e:
        print(f"   ❌ Download failed for {track_name}: {e}")
        return False

# === MAIN FUNCTIONS ===
class PlaylistSongDownloader:
    def __init__(self):
        self.songs_db = {}
        self.playlists_db = {}
        self.mapping_db = {}
        self.load_databases()
    
    def load_databases(self):
        """Load all required databases"""
        print("📚 Loading databases...")
        
        # Load songs database
        self.songs_db = load_json_file(Config.SONGS_DB_FILE)
        songs_count = len(self.songs_db.get('songs', {}))
        print(f"   📜 Songs database: {songs_count} songs")
        
        # Load playlists database
        self.playlists_db = load_json_file(Config.PLAYLISTS_DB_FILE)
        playlists_count = len(self.playlists_db.get('playlists', {}))
        print(f"   📋 Playlists database: {playlists_count} playlists")
        
        # Load mapping database
        self.mapping_db = load_json_file(Config.MAPPING_DB_FILE)
        mapping_count = len(self.mapping_db.get('mapping', {}))
        print(f"   🔗 Mapping database: {mapping_count} song mappings")
    
    def find_playlist_songs(self, playlist_name: str) -> List[str]:
        """Find all song IDs for a given playlist name"""
        print(f"🔍 Searching for playlist: '{playlist_name}'")
        
        song_ids = []
        mapping = self.mapping_db.get('mapping', {})
        
        # Search through all song mappings
        for song_id, playlists in mapping.items():
            if playlist_name.lower() in [p.lower() for p in playlists]:
                song_ids.append(song_id)
        
        print(f"   📝 Found {len(song_ids)} songs in playlist '{playlist_name}'")
        return song_ids
    
    def check_local_files(self, song_ids: List[str]) -> Tuple[List[str], List[str]]:
        """Check which songs exist locally and which are missing"""
        existing_songs = []
        missing_songs = []
        
        print("🔍 Checking local files...")
        
        for song_id in song_ids:
            file_path = os.path.join(Config.SONGS_FOLDER, f"{song_id}.mp3")
            if os.path.exists(file_path):
                existing_songs.append(song_id)
            else:
                missing_songs.append(song_id)
        
        print(f"   ✅ Existing songs: {len(existing_songs)}")
        print(f"   ❌ Missing songs: {len(missing_songs)}")
        
        return existing_songs, missing_songs
    
    def get_song_metadata(self, song_id: str) -> Optional[dict]:
        """Get song metadata from the songs database"""
        songs = self.songs_db.get('songs', {})
        song_info = songs.get(song_id)
        
        if song_info:
            metadata = song_info.get('metadata', {})
            return {
                'track_name': metadata.get('track_name', 'Unknown Track'),
                'artists_string': metadata.get('artists_string', 'Unknown Artist'),
                'album_name': metadata.get('album_name', 'Unknown Album'),
                'duration_formatted': metadata.get('duration_formatted', '0:00')
            }
        return None
    
    def download_missing_songs(self, missing_song_ids: List[str]) -> int:
        """Download all missing songs"""
        if not missing_song_ids:
            print("✅ No songs to download!")
            return 0
        
        print(f"🎵 Starting download of {len(missing_song_ids)} missing songs...")
        successful_downloads = 0
        
        for i, song_id in enumerate(missing_song_ids, 1):
            print(f"\n📥 [{i}/{len(missing_song_ids)}] Processing: {song_id}")
            
            # Get song metadata
            metadata = self.get_song_metadata(song_id)
            
            if not metadata:
                print(f"   ❌ No metadata found for {song_id}")
                continue
            
            track_name = metadata['track_name']
            artists_string = metadata['artists_string']
            
            print(f"   🎵 {track_name} by {artists_string}")
            
            # Attempt download
            if download_song(track_name, artists_string, song_id, Config.SONGS_FOLDER):
                successful_downloads += 1
                
                # Update song database with download info
                self.update_song_download_status(song_id, True)
            else:
                # Mark as failed
                self.update_song_download_status(song_id, False)
            
            # Small delay between downloads
            time.sleep(Config.DOWNLOAD_DELAY)
        
        print(f"\n📊 Download Summary:")
        print(f"   ✅ Successful: {successful_downloads}")
        print(f"   ❌ Failed: {len(missing_song_ids) - successful_downloads}")
        
        return successful_downloads
    
    def update_song_download_status(self, song_id: str, success: bool):
        """Update download status in songs database"""
        songs = self.songs_db.get('songs', {})
        if song_id in songs:
            if 'download_info' not in songs[song_id]:
                songs[song_id]['download_info'] = {}
            
            songs[song_id]['download_info'].update({
                'status': 'completed' if success else 'failed',
                'file_path': os.path.join(Config.SONGS_FOLDER, f"{song_id}.mp3") if success else None,
                'downloaded_at': datetime.now().isoformat() if success else None
            })
    
    def update_playlist_database(self, playlist_name: str, song_ids: List[str], successful_downloads: int):
        """Update or create playlist entry in playlists database"""
        print(f"💾 Updating playlist database for '{playlist_name}'...")
        
        # Check if playlist already exists
        playlists = self.playlists_db.get('playlists', {})
        playlist_key = playlist_name.lower().replace(' ', '').replace('-', '')
        
        # Count existing successful downloads
        existing_successful = 0
        for song_id in song_ids:
            file_path = os.path.join(Config.SONGS_FOLDER, f"{song_id}.mp3")
            if os.path.exists(file_path):
                existing_successful += 1
        
        # Create or update playlist entry
        playlist_entry = {
            'name': playlist_name,
            'total_tracks': len(song_ids),
            'successful_downloads': existing_successful,
            'source_url': f'manual_playlist_{playlist_name}',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'songs': song_ids,
            'unique_song_count': len(song_ids),
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
        
        # If playlist exists, preserve some original data
        if playlist_key in playlists:
            existing = playlists[playlist_key]
            playlist_entry['created_at'] = existing.get('created_at', playlist_entry['created_at'])
            if 'source_url' in existing and not existing['source_url'].startswith('manual_playlist_'):
                playlist_entry['source_url'] = existing['source_url']
        
        playlists[playlist_key] = playlist_entry
        self.playlists_db['playlists'] = playlists
        
        # Update totals
        self.playlists_db['total_playlists'] = len(playlists)
        self.playlists_db['last_updated'] = datetime.now().isoformat()
        
        print(f"   ✅ Updated playlist '{playlist_name}' with {len(song_ids)} songs")
    
    def save_databases(self):
        """Save all databases"""
        print("💾 Saving databases...")
        
        success_count = 0
        
        if save_json_file(Config.SONGS_DB_FILE, self.songs_db):
            success_count += 1
            print("   ✅ Songs database saved")
        
        if save_json_file(Config.PLAYLISTS_DB_FILE, self.playlists_db):
            success_count += 1
            print("   ✅ Playlists database saved")
        
        if save_json_file(Config.MAPPING_DB_FILE, self.mapping_db):
            success_count += 1
            print("   ✅ Mapping database saved")
        
        if success_count == 3:
            print("💾 All databases saved successfully!")
        else:
            print(f"⚠️  Only {success_count}/3 databases saved successfully")
    
    def process_playlist(self, playlist_name: str):
        """Main function to process a playlist"""
        print(f"\n🎵 Processing Playlist: '{playlist_name}'")
        print("=" * 60)
        
        # Find all songs in the playlist
        song_ids = self.find_playlist_songs(playlist_name)
        
        if not song_ids:
            print(f"❌ No songs found for playlist '{playlist_name}'")
            print("💡 Make sure the playlist name matches exactly (case-insensitive)")
            return
        
        # Check which songs exist locally
        existing_songs, missing_songs = self.check_local_files(song_ids)
        
        # Download missing songs
        successful_downloads = 0
        if missing_songs:
            successful_downloads = self.download_missing_songs(missing_songs)
        
        # Update playlist database
        self.update_playlist_database(playlist_name, song_ids, successful_downloads)
        
        # Save all databases
        self.save_databases()
        
        print(f"\n🎉 Playlist Processing Complete!")
        print(f"   📋 Playlist: {playlist_name}")
        print(f"   📊 Total songs: {len(song_ids)}")
        print(f"   ✅ Already existing: {len(existing_songs)}")
        print(f"   📥 Successfully downloaded: {successful_downloads}")
        print(f"   ❌ Failed downloads: {len(missing_songs) - successful_downloads}")

def main():
    """Main function"""
    print("🎵 Playlist Song Downloader")
    print("=" * 50)
    print("This tool will download missing songs from a specific playlist")
    print()
    
    # Check prerequisites
    if not check_prerequisites():
        print("❌ Prerequisites not met. Please install required tools.")
        return
    
    # Get playlist name from user
    print("📋 Available playlists (examples from your mapping):")
    print("   - anirudhravichander")
    print("   - Tamil-Songs-Best-of-All-Time")
    print("   - Anirudh-all-songs-tamil")
    print("   - Best tamil songs of all time")
    print("   - All in one")
    print()
    
    while True:
        playlist_name = input("Enter playlist name (or 'quit' to exit): ").strip()
        
        if playlist_name.lower() == 'quit':
            print("👋 Goodbye!")
            return
        
        if not playlist_name:
            print("❌ Please enter a playlist name")
            continue
        
        # Initialize downloader
        downloader = PlaylistSongDownloader()
        
        # Process the playlist
        downloader.process_playlist(playlist_name)
        
        # Ask if user wants to process another playlist
        print("\n" + "=" * 50)
        another = input("Do you want to process another playlist? (y/n): ").strip().lower()
        if another not in ['y', 'yes']:
            break
    
    print("👋 Thank you for using Playlist Song Downloader!")

if __name__ == "__main__":
    main()
