#!/usr/bin/env python3
"""
Batch Artist Scraper
This script processes multiple artist IDs continuously, opening tabs for each and collecting data.
"""

import json
import threading
import time
import os
import re
import subprocess
import sys
import requests
import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gzip
import brotli

# === CONFIGURATION ===
class Config:
    # Spotify settings
    ARTIST_IDS = []  # Will be set by user input
    TARGET_API_URL = "https://api-partner.spotify.com/pathfinder/v2/query"
    
    # Scrolling settings
    SCROLL_PAUSE_TIME = 2
    AUTO_SCROLL_ENABLED = True
    SCROLL_PIXELS = 800
    
    # Download settings
    AUDIO_QUALITY = '192K'
    MAX_RETRIES = 3
    DOWNLOAD_DELAY = 1  # Seconds between downloads
    
    # Metadata settings
    DOWNLOAD_COVER_ART = True
    COVER_ART_SIZE = 640  # Preferred size (640x640, 300x300, or 64x64)
    
    # Error handling settings
    SKIP_INVALID_TRACKS = True
    MIN_TRACK_NAME_LENGTH = 1
    MIN_ARTIST_NAME_LENGTH = 1
    
    # Consolidation settings
    CONSOLIDATED_FOLDER = "consolidated_music"
    ENABLE_SMART_DEDUPLICATION = True
    
    # Test folder for captured data
    TEST_FOLDER = "test"
    
    # Batch processing settings
    DELAY_BETWEEN_ARTISTS = 3  # Seconds between processing different artists
    MAX_SCROLL_ATTEMPTS = 100  # Maximum scroll attempts per artist

# === GLOBAL VARIABLES ===
captured_data = []
all_artist_tracks = []
seen_requests = set()
stop_capture = False
auto_scroll_active = False
current_artist_id = ""

# === SMART SONG MANAGER CLASS ===
class SmartSongManager:
    def __init__(self, consolidated_folder: str = "consolidated_music"):
        self.consolidated_folder = Path(consolidated_folder)
        self.songs_folder = self.consolidated_folder / "songs"
        self.metadata_folder = self.consolidated_folder / "metadata"
        
        # Create directories if they don't exist
        self.songs_folder.mkdir(parents=True, exist_ok=True)
        self.metadata_folder.mkdir(parents=True, exist_ok=True)
        
        # Load existing databases
        self.existing_songs = {}  # song_id -> song_info
        self.existing_playlists = {}  # playlist_id -> playlist_info
        self.existing_artists = {}  # artist_uri -> artist_info
        self.uri_to_song_id = {}  # track_uri -> song_id
        self.name_artist_to_song_id = {}  # normalized_name_artist -> song_id
        
        self.load_existing_databases()
    
    def load_existing_databases(self):
        """Load existing songs, playlists, and artists databases"""
        # Load songs database
        songs_db_path = self.metadata_folder / 'songs_database.json'
        if songs_db_path.exists():
            try:
                with open(songs_db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_songs = data.get('songs', {})
                    
                    for song_id, song_info in existing_songs.items():
                        self.existing_songs[song_id] = song_info
                        
                        # Build lookup tables
                        metadata = song_info.get('metadata', {})
                        track_uri = metadata.get('track_uri', '')
                        if track_uri:
                            self.uri_to_song_id[track_uri] = song_id
                        
                        # Create name+artist lookup
                        track_name = metadata.get('track_name', '').lower().strip()
                        artists = metadata.get('artists_string', '').lower().strip()
                        if track_name and artists:
                            key = f"{track_name}|{artists}"
                            self.name_artist_to_song_id[key] = song_id
                
                print(f"📚 Loaded {len(self.existing_songs)} existing songs from database")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing songs database: {e}")
        
        # Load playlists database
        playlists_db_path = self.metadata_folder / 'playlists_database.json'
        if playlists_db_path.exists():
            try:
                with open(playlists_db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.existing_playlists = data.get('playlists', {})
                
                print(f"📚 Loaded {len(self.existing_playlists)} existing playlists from database")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing playlists database: {e}")
        
        # Load artists database
        artists_db_path = self.metadata_folder / 'artists_database.json'
        if artists_db_path.exists():
            try:
                with open(artists_db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.existing_artists = data.get('artists', {})
                
                print(f"📚 Loaded {len(self.existing_artists)} existing artists from database")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing artists database: {e}")
        else:
            print("🆕 No existing artists database found - starting fresh")
    
    def generate_song_id(self, track_name: str, artists: str) -> str:
        """Generate a unique ID for a song based on track name and artists"""
        clean_string = f"{track_name}_{artists}".lower()
        clean_string = re.sub(r'[^a-z0-9_]', '', clean_string)
        hash_object = hashlib.md5(clean_string.encode())
        return f"song_{hash_object.hexdigest()[:12]}"
    
    def find_existing_song(self, track_info: dict) -> Optional[Tuple[str, dict]]:
        """
        Find existing song in database
        Returns: (song_id, song_info) if found, None otherwise
        """
        track_uri = track_info.get('track_uri', '')
        track_name = track_info.get('track_name', '').lower().strip()
        artists = track_info.get('artists_string', '').lower().strip()
        
        # First check by URI (most reliable)
        if track_uri and track_uri in self.uri_to_song_id:
            song_id = self.uri_to_song_id[track_uri]
            return song_id, self.existing_songs[song_id]
        
        # Then check by name + artists
        if track_name and artists:
            key = f"{track_name}|{artists}"
            if key in self.name_artist_to_song_id:
                song_id = self.name_artist_to_song_id[key]
                return song_id, self.existing_songs[song_id]
        
        return None
    
    def add_playlist_to_song(self, song_id: str, playlist_id: str):
        """Add playlist ID to existing song without replacing other playlists"""
        if song_id in self.existing_songs:
            current_playlists = self.existing_songs[song_id].get('playlists', [])
            if playlist_id not in current_playlists:
                current_playlists.append(playlist_id)
                self.existing_songs[song_id]['playlists'] = current_playlists
                print(f"   ✅ Added playlist {playlist_id} to existing song {song_id}")
                return True
            else:
                print(f"   ℹ️  Song {song_id} already has playlist {playlist_id}")
                return False
        return False
    
    def store_artist_info(self, artist_uri: str, artist_name: str, playlist_key: str):
        """Store artist information in artists database"""
        if artist_uri in self.existing_artists:
            # Update existing artist
            if playlist_key not in self.existing_artists[artist_uri].get('playlist_ids', []):
                self.existing_artists[artist_uri]['playlist_ids'].append(playlist_key)
                self.existing_artists[artist_uri]['last_updated'] = datetime.now().isoformat()
        else:
            # Create new artist entry
            self.existing_artists[artist_uri] = {
                'name': artist_name,
                'uri': artist_uri,
                'playlist_ids': [playlist_key],
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }

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

def safe_get(data, *keys, default="Unknown"):
    """Safely navigate nested dictionaries with fallback"""
    try:
        result = data
        for key in keys:
            if isinstance(result, dict) and key in result:
                result = result[key]
            else:
                return default
        return result if result is not None and str(result).strip() else default
    except:
        return default

def download_song(track_name: str, artists_string: str, song_id: str, output_folder: Path) -> bool:
    """Download a song using yt-dlp"""
    try:
        import yt_dlp
        
        # Create search query
        search_query = f"{track_name} {artists_string}"
        
        # Configure yt-dlp options for MP3 download only
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': str(output_folder / f'{song_id}.%(ext)s'),
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

# === SPOTIFY CAPTURE FUNCTIONS ===
def decode_response_body(response):
    """Decode response body handling different compression formats"""
    try:
        body = response.body
        if not body:
            return ""
        
        encoding = response.headers.get('content-encoding', '').lower()
        
        if encoding == 'gzip':
            body = gzip.decompress(body)
        elif encoding == 'br':
            body = brotli.decompress(body)
        elif encoding == 'deflate':
            import zlib
            body = zlib.decompress(body)
        
        try:
            return body.decode('utf-8')
        except UnicodeDecodeError:
            return body.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"[!] Error decoding response body: {e}")
        return ""

def parse_json_response(body_text):
    """Try to parse response as JSON"""
    try:
        return json.loads(body_text)
    except json.JSONDecodeError:
        return body_text

def is_artist_discography_response(parsed_response):
    """Check if the response contains artist discography data"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            album_union = data.get('albumUnion', {})
            return album_union.get('__typename') == 'Album'
        return False
    except:
        return False

def extract_tracks_from_response(parsed_response):
    """Extract the tracks array from artist discography response"""
    try:
        if isinstance(parsed_response, dict):
            data = parsed_response.get('data', {})
            album_union = data.get('albumUnion', {})
            tracks_v2 = album_union.get('tracksV2', {})
            items = tracks_v2.get('items', [])
            return items
    except:
        pass
    return []

def request_interceptor(request):
    """Intercept HTTP requests to capture Spotify API calls"""
    global captured_data, all_artist_tracks, seen_requests, stop_capture, current_artist_id
    
    try:
        if stop_capture:
            return
        
        if Config.TARGET_API_URL in request.url:
            request_hash = hashlib.md5(f"{request.url}{request.body}".encode()).hexdigest()
            
            if request_hash not in seen_requests:
                seen_requests.add(request_hash)
                captured_data.append({
                    'url': request.url,
                    'method': request.method,
                    'headers': dict(request.headers),
                    'body': request.body.decode('utf-8') if request.body else None,
                    'timestamp': datetime.now().isoformat(),
                    'hash': request_hash,
                    'artist_id': current_artist_id
                })
                
                print(f"[+] Captured request #{len(captured_data)} for artist {current_artist_id}")
                
    except Exception as e:
        print(f"[!] Error in request interceptor: {e}")

def response_interceptor(request, response):
    """Intercept HTTP responses to capture Spotify API data"""
    global captured_data, all_artist_tracks, stop_capture, current_artist_id
    
    try:
        if stop_capture:
            return
        
        if Config.TARGET_API_URL in request.url and response.status_code == 200:
            body_text = decode_response_body(response)
            
            if body_text:
                parsed_response = parse_json_response(body_text)
                
                # Check if this is artist discography data
                if is_artist_discography_response(parsed_response):
                    tracks = extract_tracks_from_response(parsed_response)
                    print(f"[+] Found {len(tracks)} tracks for artist {current_artist_id}")
                    
                    for track_item in tracks:
                        track = track_item.get('track', {})
                        if track:
                            all_artist_tracks.append(track)
                
    except Exception as e:
        print(f"[!] Error in response interceptor: {e}")

def wait_for_manual_scroll(artist_id):
    """Wait for user to manually scroll and press Enter"""
    global stop_capture
    
    print(f"\n� Manual Scroll Instructions for Artist {artist_id}:")
    print("   1. Manually scroll down the page to load all tracks")
    print("   2. Keep scrolling until you see all songs from the artist")
    print("   3. The script will capture track data as you scroll")
    print("   4. Press Enter when you've captured all tracks")
    
    print(f"\n⌨️  Press Enter when you're done scrolling for artist {artist_id}...")
    input()
    
    stop_capture = True
    print(f"✅ Manual scrolling completed for artist {artist_id}")
    print(f"🎵 Total tracks captured: {len(all_artist_tracks)}")

def get_multiple_artist_ids():
    """Get multiple artist IDs from user input"""
    print("🎵 Batch Spotify Artist Discography Scraper")
    print("=" * 60)
    print("This tool will scrape all songs from multiple artists' discographies")
    print()
    print("You can provide:")
    print("1. Multiple Artist IDs separated by commas")
    print("2. Multiple URLs separated by commas")
    print("3. Mix of both")
    print()
    print("Example:")
    print("Artist IDs: 4zCH9qm4R2DADamUHMCa6O, 1vCWHaC5f2uS3yhpwWbIA6")
    print("URLs: https://open.spotify.com/artist/4zCH9qm4R2DADamUHMCa6O, https://open.spotify.com/artist/1vCWHaC5f2uS3yhpwWbIA6")
    print()
    
    while True:
        artist_input = input("Enter Artist IDs or URLs (comma-separated): ").strip()
        
        if not artist_input:
            print("❌ Please provide at least one artist ID or URL")
            continue
        
        # Split by comma and clean up
        artist_entries = [entry.strip() for entry in artist_input.split(',') if entry.strip()]
        
        if not artist_entries:
            print("❌ No valid entries found")
            continue
        
        artist_ids = []
        
        for entry in artist_entries:
            # Extract artist ID from URL if full URL is provided
            if "open.spotify.com/artist/" in entry:
                try:
                    artist_id = entry.split('/artist/')[1].split('/')[0].split('?')[0]
                    artist_ids.append(artist_id)
                    print(f"✅ Extracted Artist ID: {artist_id}")
                except:
                    print(f"❌ Could not extract artist ID from URL: {entry}")
                    continue
            else:
                # Assume it's already an artist ID
                if len(entry) == 22 and entry.isalnum():
                    artist_ids.append(entry)
                    print(f"✅ Valid Artist ID: {entry}")
                else:
                    print(f"❌ Invalid artist ID format: {entry}")
                    continue
        
        if artist_ids:
            print(f"\n📊 Total valid artist IDs: {len(artist_ids)}")
            confirm = input(f"Process {len(artist_ids)} artists? (y/N): ").strip().lower()
            if confirm in ['y', 'yes']:
                return artist_ids
            else:
                print("Let's try again...")
                continue
        else:
            print("❌ No valid artist IDs found. Please try again.")
            continue

def get_artist_name_from_database(artist_id: str) -> str:
    """Get artist name from artists database if available"""
    try:
        artists_db_path = Path(Config.CONSOLIDATED_FOLDER) / "metadata" / "artists_database.json"
        if artists_db_path.exists():
            with open(artists_db_path, 'r', encoding='utf-8') as f:
                artists_db = json.load(f)
                artist_uri = f"spotify:artist:{artist_id}"
                if artist_uri in artists_db.get('artists', {}):
                    stored_name = artists_db['artists'][artist_uri].get('name', '')
                    if stored_name and stored_name != 'Unknown Artist':
                        print(f"📚 Found existing artist in database: {stored_name}")
                        return stored_name
    except Exception as e:
        print(f"⚠️  Could not load artist name from database: {e}")
    
    return ""

def process_artist_tracks(artist_name: str, artist_id: str):
    """Process captured artist tracks and save to database"""
    global all_artist_tracks
    
    if not all_artist_tracks:
        print(f"❌ No tracks found to process for artist: {artist_name}")
        return
    
    print(f"\n🎵 Processing {len(all_artist_tracks)} tracks for artist: {artist_name}")
    
    song_manager = SmartSongManager()
    
    # Create artist playlist entry using artist ID as key for uniqueness
    playlist_key = f"artist_{artist_id}"
    playlist_name = f"{artist_name} - Discography"
    
    print(f"🆔 Using playlist key: {playlist_key}")
    print(f"📋 Playlist name: {playlist_name}")
    
    processed_tracks = []
    song_ids = []
    new_songs_to_download = []
    existing_songs_updated = 0
    
    # Get main artist info for storage
    main_artist_uri = ""
    if all_artist_tracks:
        first_track = all_artist_tracks[0]
        artists_data = safe_get(first_track, 'artists', 'items', default=[])
        for artist in artists_data:
            if safe_get(artist, 'profile', 'name') == artist_name:
                main_artist_uri = safe_get(artist, 'uri', default='')
                break
    
    for track_data in all_artist_tracks:
        try:
            # Extract track information
            track_name = safe_get(track_data, 'name', default='Unknown Track')
            track_uri = safe_get(track_data, 'uri', default='')
            duration_ms = safe_get(track_data, 'duration', 'totalMilliseconds', default=0)
            
            # Extract artists information
            artists_data = safe_get(track_data, 'artists', 'items', default=[])
            artists_info = []
            artists_names = []
            
            for artist in artists_data:
                artist_name_individual = safe_get(artist, 'profile', 'name', default='Unknown Artist')
                artist_uri = safe_get(artist, 'uri', default='')
                
                artists_info.append({
                    'name': artist_name_individual,
                    'uri': artist_uri
                })
                artists_names.append(artist_name_individual)
                
                # Store artist info in artists database
                song_manager.store_artist_info(artist_uri, artist_name_individual, playlist_key)
            
            artists_string = ', '.join(artists_names)
            
            # Create track metadata
            track_info = {
                'track_name': track_name,
                'artists_string': artists_string,
                'artists_info': artists_info,
                'track_uri': track_uri,
                'duration_ms': duration_ms,
                'album_name': 'Artist Discography',
                'track_number': len(processed_tracks) + 1
            }
            
            # Generate song ID
            song_id = song_manager.generate_song_id(track_name, artists_string)
            
            # Check if song already exists
            existing_song = song_manager.find_existing_song(track_info)
            
            if existing_song:
                # Song exists, add playlist ID to it
                existing_song_id, existing_song_info = existing_song
                if song_manager.add_playlist_to_song(existing_song_id, playlist_key):
                    existing_songs_updated += 1
                song_ids.append(existing_song_id)
                print(f"   🔄 Updated existing song: {track_name} by {artists_string}")
            else:
                # New song, create entry and mark for download
                song_entry = {
                    'metadata': track_info,
                    'playlists': [playlist_key],
                    'download_info': {
                        'status': 'pending',
                        'file_path': None,
                        'file_size': None,
                        'quality': Config.AUDIO_QUALITY,
                        'downloaded_at': None
                    },
                    'added_at': datetime.now().isoformat()
                }
                
                song_manager.existing_songs[song_id] = song_entry
                new_songs_to_download.append((song_id, track_name, artists_string))
                song_ids.append(song_id)
                print(f"   ✅ New song added: {track_name} by {artists_string}")
            
            processed_tracks.append(track_info)
            
        except Exception as e:
            print(f"   ❌ Error processing track: {e}")
            continue
    
    # Create playlist entry
    successful_downloads = sum(1 for song_id in song_ids 
                             if song_manager.existing_songs.get(song_id, {}).get('download_info', {}).get('status') == 'completed')
    
    playlist_entry = {
        'name': playlist_name,
        'total_tracks': len(song_ids),
        'successful_downloads': successful_downloads,
        'source_url': f'https://open.spotify.com/artist/{artist_id}/discography/all',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'songs': song_ids,
        'unique_song_count': len(song_ids),
        'created_at': datetime.now().isoformat(),
        'last_updated': datetime.now().isoformat()
    }
    
    song_manager.existing_playlists[playlist_key] = playlist_entry
    
    # Store main artist info
    if main_artist_uri:
        song_manager.store_artist_info(main_artist_uri, artist_name, playlist_key)
    
    # Save databases
    save_databases(song_manager)
    
    print(f"\n📊 Processing Summary for {artist_name}:")
    print(f"   ✅ Total tracks processed: {len(processed_tracks)}")
    print(f"   🔄 Existing songs updated: {existing_songs_updated}")
    print(f"   🆕 New songs to download: {len(new_songs_to_download)}")
    print(f"   📋 Created playlist: {playlist_name}")
    print(f"   🆔 Playlist Key: {playlist_key}")
    
    # Download new songs
    if new_songs_to_download:
        print(f"\n🎵 Starting downloads for {len(new_songs_to_download)} new songs...")
        successful_downloads = 0
        
        for song_id, track_name, artists_string in new_songs_to_download:
            try:
                print(f"\n📥 Downloading: {track_name} by {artists_string}")
                
                if download_song(track_name, artists_string, song_id, song_manager.songs_folder):
                    # Update download status
                    song_manager.existing_songs[song_id]['download_info'].update({
                        'status': 'completed',
                        'file_path': str(song_manager.songs_folder / f"{song_id}.mp3"),
                        'downloaded_at': datetime.now().isoformat()
                    })
                    
                    successful_downloads += 1
                    print(f"   ✅ Successfully downloaded: {track_name}")
                else:
                    # Mark as failed
                    song_manager.existing_songs[song_id]['download_info']['status'] = 'failed'
                    print(f"   ❌ Failed to download: {track_name}")
                
                # Small delay between downloads
                time.sleep(Config.DOWNLOAD_DELAY)
                
            except Exception as e:
                print(f"   ❌ Download error for {track_name}: {e}")
                song_manager.existing_songs[song_id]['download_info']['status'] = 'failed'
        
        # Update playlist with final successful downloads count
        song_manager.existing_playlists[playlist_key]['successful_downloads'] = successful_downloads
        song_manager.existing_playlists[playlist_key]['last_updated'] = datetime.now().isoformat()
        save_databases(song_manager)
        print(f"\n💾 Updated databases - {successful_downloads} successful downloads for {artist_name}")

def save_databases(song_manager: SmartSongManager):
    """Save songs, playlists, and artists databases"""
    try:
        # Save songs database
        songs_db = {
            'songs': song_manager.existing_songs,
            'total_songs': len(song_manager.existing_songs),
            'last_updated': datetime.now().isoformat()
        }
        
        songs_db_path = song_manager.metadata_folder / 'songs_database.json'
        with open(songs_db_path, 'w', encoding='utf-8') as f:
            json.dump(songs_db, f, indent=2, ensure_ascii=False)
        
        # Save playlists database
        playlists_db = {
            'playlists': song_manager.existing_playlists,
            'total_playlists': len(song_manager.existing_playlists),
            'last_updated': datetime.now().isoformat()
        }
        
        playlists_db_path = song_manager.metadata_folder / 'playlists_database.json'
        with open(playlists_db_path, 'w', encoding='utf-8') as f:
            json.dump(playlists_db, f, indent=2, ensure_ascii=False)
        
        # Save artists database
        artists_db = {
            'artists': song_manager.existing_artists,
            'total_artists': len(song_manager.existing_artists),
            'last_updated': datetime.now().isoformat()
        }
        
        artists_db_path = song_manager.metadata_folder / 'artists_database.json'
        with open(artists_db_path, 'w', encoding='utf-8') as f:
            json.dump(artists_db, f, indent=2, ensure_ascii=False)
        
        # Save song-playlist mapping
        song_playlist_mapping = {}
        for song_id, song_info in song_manager.existing_songs.items():
            playlists = song_info.get('playlists', [])
            song_playlist_mapping[song_id] = playlists
        
        mapping_db = {
            'mapping': song_playlist_mapping,
            'last_updated': datetime.now().isoformat()
        }
        
        mapping_db_path = song_manager.metadata_folder / 'song_playlist_mapping.json'
        with open(mapping_db_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_db, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Saved databases:")
        print(f"   📚 Songs: {len(song_manager.existing_songs)}")
        print(f"   📋 Playlists: {len(song_manager.existing_playlists)}")
        print(f"   🎤 Artists: {len(song_manager.existing_artists)}")
        
    except Exception as e:
        print(f"❌ Error saving databases: {e}")

def main():
    """Main function to run the batch artist discography scraper"""
    global stop_capture, all_artist_tracks, captured_data, current_artist_id
    
    print("🎵 Batch Spotify Artist Discography Scraper")
    print("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        print("❌ Prerequisites not met. Please install required tools.")
        return
    
    # Get multiple artist IDs from user
    artist_ids = get_multiple_artist_ids()
    Config.ARTIST_IDS = artist_ids
    
    print(f"\n🎯 Will process {len(artist_ids)} artists:")
    for i, artist_id in enumerate(artist_ids, 1):
        print(f"   {i}. {artist_id}")
    
    print("\n📋 Instructions:")
    print("1. Browser will open and navigate to each artist automatically")
    print("2. For each artist, YOU will manually scroll down to load all tracks")
    print("3. The script will capture track data as you scroll")
    print("4. Press Enter when you've seen all tracks for that artist")
    print("5. Repeat for ALL artists before any downloads start")
    print("6. After all artists are done, the script will process and download everything")
    print("7. Press Enter to start...")
    input()
    
    # Setup browser once
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    driver.request_interceptor = request_interceptor
    driver.response_interceptor = response_interceptor
    
    # Dictionary to store all collected artist data
    all_artists_data = {}
    total_data_collected = 0
    total_errors = 0
    
    try:
        print("🌐 Browser opened successfully")
        
        # PHASE 1: Collect data from all artist pages
        print(f"\n{'='*80}")
        print(f"🔍 PHASE 1: DATA COLLECTION FROM ALL ARTIST PAGES")
        print(f"{'='*80}")
        
        for i, artist_id in enumerate(artist_ids, 1):
            print(f"\n{'='*60}")
            print(f"🎤 Collecting Data from Artist {i}/{len(artist_ids)}: {artist_id}")
            print(f"{'='*60}")
            
            # Reset global variables for this artist
            current_artist_id = artist_id
            all_artist_tracks = []
            stop_capture = False
            
            try:
                # Construct artist discography URL
                artist_url = f"https://open.spotify.com/artist/{artist_id}/discography/all"
                print(f"🔗 Opening: {artist_url}")
                
                # Navigate to artist page
                driver.get(artist_url)
                print("⏳ Waiting for page to load...")
                time.sleep(5)
                
                # Wait for manual scrolling
                wait_for_manual_scroll(artist_id)
                
                print(f"📊 Data Collection Summary for Artist {artist_id}:")
                print(f"   🎵 Tracks Found: {len(all_artist_tracks)}")
                
                if all_artist_tracks:
                    # Get artist name
                    stored_artist_name = get_artist_name_from_database(artist_id)
                    
                    if stored_artist_name:
                        artist_name = stored_artist_name
                    else:
                        # Get artist name from first track
                        first_track = all_artist_tracks[0]
                        artists_data = safe_get(first_track, 'artists', 'items', default=[])
                        if artists_data:
                            artist_name = safe_get(artists_data[0], 'profile', 'name', default='Unknown Artist')
                        else:
                            artist_name = f"Artist_{artist_id}"
                    
                    print(f"🎤 Artist: {artist_name}")
                    print(f"🆔 Artist ID: {artist_id}")
                    
                    # Store collected data for later processing
                    all_artists_data[artist_id] = {
                        'artist_name': artist_name,
                        'artist_id': artist_id,
                        'tracks': all_artist_tracks.copy()
                    }
                    
                    total_data_collected += 1
                    print(f"✅ Data collected for artist: {artist_name}")
                else:
                    print(f"❌ No tracks found for artist {artist_id}")
                    total_errors += 1
                
                # Delay between artists
                if i < len(artist_ids):  # Don't delay after the last artist
                    print(f"⏳ Waiting {Config.DELAY_BETWEEN_ARTISTS} seconds before next artist...")
                    time.sleep(Config.DELAY_BETWEEN_ARTISTS)
                
            except Exception as e:
                print(f"❌ Error collecting data for artist {artist_id}: {e}")
                total_errors += 1
                continue
        
        # Close browser after data collection
        print(f"\n🔄 Closing browser after data collection...")
        driver.quit()
        print("✅ Browser closed")
        
        # PHASE 2: Process all collected data and download songs
        print(f"\n{'='*80}")
        print(f"📊 PHASE 2: PROCESSING ALL COLLECTED DATA AND DOWNLOADING")
        print(f"{'='*80}")
        print(f"📈 Data Collection Summary:")
        print(f"   ✅ Successfully collected data from: {total_data_collected} artists")
        print(f"   ❌ Failed to collect data from: {total_errors} artists")
        print(f"   📋 Total artists processed: {len(artist_ids)}")
        
        if all_artists_data:
            total_processed = 0
            processing_errors = 0
            
            for artist_id, artist_data in all_artists_data.items():
                try:
                    print(f"\n{'='*60}")
                    print(f"🔄 Processing collected data for: {artist_data['artist_name']}")
                    print(f"{'='*60}")
                    
                    # Temporarily set global variables for processing
                    all_artist_tracks = artist_data['tracks']
                    
                    # Process tracks (this includes downloads)
                    process_artist_tracks(artist_data['artist_name'], artist_data['artist_id'])
                    total_processed += 1
                    print(f"✅ Successfully processed and downloaded for: {artist_data['artist_name']}")
                    
                except Exception as e:
                    print(f"❌ Error processing artist {artist_data['artist_name']}: {e}")
                    processing_errors += 1
                    continue
            
            # Final summary
            print(f"\n{'='*80}")
            print(f"🎉 BATCH PROCESSING COMPLETE!")
            print(f"{'='*80}")
            print(f"📊 Final Summary:")
            print(f"   🔍 Data Collection Phase:")
            print(f"      ✅ Successfully collected: {total_data_collected} artists")
            print(f"      ❌ Collection errors: {total_errors} artists")
            print(f"   📥 Processing & Download Phase:")
            print(f"      ✅ Successfully processed: {total_processed} artists")
            print(f"      ❌ Processing errors: {processing_errors} artists")
            print(f"   📋 Total artists requested: {len(artist_ids)}")
        else:
            print(f"❌ No artist data was collected successfully!")
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
    
    finally:
        print("🎉 All done!")

# Run the main function
if __name__ == "__main__":
    main()
