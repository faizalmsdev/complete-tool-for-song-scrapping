#!/usr/bin/env python3
"""
Enhanced Batch Artist Scraper with Automatic Scrolling
This script processes multiple artist IDs continuously with automatic scrolling feature.
Includes cookie support to avoid being blocked by YouTube/Spotify.
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
import gzip
import brotli
import zlib
import logging
import random  # For rotating user agents and proxies
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import gzip
import brotli

# === CONFIGURATION ===
class Config:
    # Spotify settings
    ARTIST_IDS = []  # Will be set by user input
    TARGET_API_URL = "https://api-partner.spotify.com/pathfinder/v2/query"
    
    # Auto-scrolling settings
    SCROLL_STEP = 150  # Small scroll steps for smooth scrolling
    SCROLL_DELAY = 0.3  # Fast continuous scrolling (300ms between scrolls)
    MAX_SCROLLS = 1000  # Higher safety limit
    NO_CONTENT_THRESHOLD = 5  # Number of checks before considering scrolling complete
    
    # Download settings
    AUDIO_QUALITY = '320k'
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
    
    # Cookie settings
    COOKIES_FILE = "cookies.txt"  # Path to cookies file for avoiding blocks
    BROWSER_FOR_COOKIES = None   # Browser to extract cookies from (chrome, firefox, edge, etc.)
    
    # Anti-block settings
    USE_SESSION_PER_ARTIST = True  # Create new browser session for each artist
    ROTATE_USER_AGENTS = True     # Use different user agents for each session
    USE_PROXIES = False           # Use proxies to avoid IP blocks
    PROXY_LIST = []               # List of proxy URLs to rotate through

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
    
    # Test cookies if available (matching FinalcodeWithCookies approach)
    cookies_file = Config.COOKIES_FILE
    if os.path.exists(cookies_file):
        print(f"🍪 Testing cookies from: {cookies_file}")
        try:
            import yt_dlp
            test_opts = {
                'quiet': True,
                'cookiefile': cookies_file,
                'skip_download': True,
                'extract_flat': True,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            }
            with yt_dlp.YoutubeDL(test_opts) as ydl:
                ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
            print("   ✅ Cookies are valid and working")
        except Exception as e:
            print(f"   ⚠️ Cookie test failed: {e}")
            print("   💡 You may need to update your cookies.txt file")
            
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
        print(f"📥 Downloading: {track_name} by {artists_string}")
        
        # Prioritize cookies.txt file over browser cookies (matching FinalcodeWithCookies approach)
        cookies_file = Config.COOKIES_FILE
        if os.path.exists(cookies_file):
            print(f"   🍪 Using cookies.txt file")
        elif Config.BROWSER_FOR_COOKIES:
            print(f"   🍪 Using cookies from {Config.BROWSER_FOR_COOKIES} browser")
        else:
            print(f"   ⚠️ No cookies available - may encounter bot detection")
        
        # Configure yt-dlp options with extra parameters to avoid bot detection
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
            'quiet': False,  # Set to False to see detailed output for debugging
            'no_warnings': False,  # Show warnings for debugging
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'nocheckcertificate': True,
            'ignoreerrors': False,  # Stop on errors to see what's happening
            'geo_bypass': True,     # Try to bypass geo-restrictions
            'sleep_interval': 2,    # Delay between requests to avoid rate limiting
            'max_sleep_interval': 5,
            'rm_cachedir': True,    # Clear cache between downloads
            'extractor_retries': 3, # Retry extraction if it fails
            'skip_download_archive': True, # Don't use download archive to ensure fresh download
            'force_generic_extractor': False, # Use specific extractors when possible
            'concurrent_fragment_downloads': 1, # Limit concurrent fragment downloads
        }
        
        # Add either file cookies or browser cookies, not both (matching FinalcodeWithCookies approach)
        if os.path.exists(cookies_file):
            ydl_opts['cookiefile'] = cookies_file
        elif Config.BROWSER_FOR_COOKIES:
            try:
                ydl_opts['cookiesfrombrowser'] = (Config.BROWSER_FOR_COOKIES,)
                print(f"   🍪 Using cookies from {Config.BROWSER_FOR_COOKIES} browser")
            except Exception as e:
                print(f"   ⚠️ Could not load browser cookies: {e}")
        
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
        
        # Check if it's a bot verification error
        error_str = str(e)
        if "Sign in to confirm you're not a bot" in error_str:
            print(f"   ⚠️ YouTube bot verification detected.")
            print(f"   💡 Tips to fix:")
            print(f"      1. Make sure your cookies.txt file is up to date")
            print(f"      2. Try with a different YouTube account")
            print(f"      3. Consider using --cookies-from-browser option directly")
            
            # Try an alternative method with browser cookies if not already tried
            if not Config.BROWSER_FOR_COOKIES and os.path.exists(Config.COOKIES_FILE):
                print(f"   🔄 Attempting alternative download method...")
                try:
                    # Try with a different user agent
                    alt_opts = ydl_opts.copy()
                    alt_opts['user_agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0'
                    
                    with yt_dlp.YoutubeDL(alt_opts) as ydl:
                        info = ydl.extract_info(f"ytsearch1:{search_query}", download=True)
                        
                        if info and 'entries' in info and len(info['entries']) > 0:
                            entry = info['entries'][0]
                            print(f"   ✅ Downloaded with alternative method: {entry.get('title', 'Unknown')}")
                            return True
                except Exception as e2:
                    print(f"   ❌ Alternative method failed: {e2}")
        
        return False

# === AUTOMATIC SCROLLING FUNCTION ===
def automatic_scroll_to_bottom(driver, artist_id: str) -> bool:
    """
    Automatically scroll to the bottom of the Spotify artist discography page
    Returns True if successful, False otherwise
    """
    global stop_capture, auto_scroll_active
    
    print(f"🤖 Starting automatic scrolling for artist {artist_id}...")
    auto_scroll_active = True
    
    try:
        # Wait for the page to load
        time.sleep(5)
        
        # Find the scrollable viewport using multiple selectors
        selectors_to_try = [
            '[data-overlayscrollbars-viewport]',
            '.os-viewport',
            'div[class*="os-scrollbar"]',
            'main[role="main"]',
            'main',
            'section[data-testid*="discography"]',
            '[data-testid="artist-page"]',
            'div[style*="overflow"]',
            '[class*="scroll"]'
        ]
        
        viewport = None
        for selector in selectors_to_try:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"🔍 Found {len(elements)} elements with selector: {selector}")
                
                for i, element in enumerate(elements):
                    try:
                        # Check if element is scrollable and has reasonable size
                        scroll_height = driver.execute_script("return arguments[0].scrollHeight", element)
                        client_height = driver.execute_script("return arguments[0].clientHeight", element)
                        
                        if scroll_height > client_height and client_height > 300:
                            viewport = element
                            print(f"✅ Found scrollable viewport: {selector}")
                            print(f"   📏 Dimensions: {scroll_height}x{client_height}")
                            break
                    except Exception as e:
                        continue
                
                if viewport:
                    break
                    
            except Exception as e:
                print(f"❌ Error with selector {selector}: {e}")
                continue
        
        # Alternative method: find viewport through discography content
        if not viewport:
            print("🔍 Trying alternative method to find scrollable content...")
            try:
                discography_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'songs') or contains(text(), 'album') or contains(text(), 'EP')]")
                for elem in discography_elements:
                    parent = elem.find_element(By.XPATH, "..")
                    while parent:
                        try:
                            scroll_height = driver.execute_script("return arguments[0].scrollHeight", parent)
                            client_height = driver.execute_script("return arguments[0].clientHeight", parent)
                            if scroll_height > client_height and client_height > 300:
                                viewport = parent
                                print(f"✅ Found viewport via content search")
                                break
                            parent = parent.find_element(By.XPATH, "..")
                        except:
                            break
                    if viewport:
                        break
            except Exception as e:
                print(f"❌ Error in alternative search: {e}")
        
        if not viewport:
            print("❌ Could not find scrollable viewport!")
            return False
        
        # Get initial scroll measurements
        initial_scroll_top = driver.execute_script("return arguments[0].scrollTop", viewport)
        initial_scroll_height = driver.execute_script("return arguments[0].scrollHeight", viewport)
        client_height = driver.execute_script("return arguments[0].clientHeight", viewport)
        
        print(f"📊 Initial measurements:")
        print(f"   📍 Scroll position: {initial_scroll_top}px")
        print(f"   📏 Scroll height: {initial_scroll_height}px")
        print(f"   🖥️  Client height: {client_height}px")
        
        # Start automatic scrolling
        scroll_count = 0
        last_height = initial_scroll_height
        no_new_content_count = 0
        tracks_found_start = len(all_artist_tracks)
        
        print(f"🚀 Starting automatic continuous scrolling...")
        print(f"   ⚙️  Scroll step: {Config.SCROLL_STEP}px")
        print(f"   ⏱️  Scroll delay: {Config.SCROLL_DELAY}s")
        
        while scroll_count < Config.MAX_SCROLLS and not stop_capture:
            # Get current scroll measurements
            current_scroll_top = driver.execute_script("return arguments[0].scrollTop", viewport)
            current_height = driver.execute_script("return arguments[0].scrollHeight", viewport)
            max_scroll_top = current_height - client_height
            
            # Check if we've reached the bottom
            if current_scroll_top >= max_scroll_top - 20:  # 20px tolerance
                if current_height == last_height:
                    no_new_content_count += 1
                    if no_new_content_count >= Config.NO_CONTENT_THRESHOLD:
                        print(f"✅ Reached bottom! No new content for {Config.NO_CONTENT_THRESHOLD} checks")
                        break
                else:
                    no_new_content_count = 0
                    last_height = current_height
                    print(f"📈 New content detected! Height: {current_height}px")
            
            # Calculate and perform scroll
            next_scroll_top = min(current_scroll_top + Config.SCROLL_STEP, max_scroll_top)
            driver.execute_script("arguments[0].scrollTop = arguments[1]", viewport, next_scroll_top)
            
            scroll_count += 1
            
            # Show progress every 50 scrolls
            if scroll_count % 50 == 0:
                tracks_found_current = len(all_artist_tracks)
                new_tracks = tracks_found_current - tracks_found_start
                print(f"📊 Progress - Scrolls: {scroll_count}, Position: {next_scroll_top}px, Tracks: {new_tracks}")
            
            # Continuous scrolling delay
            time.sleep(Config.SCROLL_DELAY)
        
        # Scrolling completed - show final results
        final_scroll_top = driver.execute_script("return arguments[0].scrollTop", viewport)
        final_height = driver.execute_script("return arguments[0].scrollHeight", viewport)
        tracks_found_final = len(all_artist_tracks)
        total_new_tracks = tracks_found_final - tracks_found_start
        
        print(f"\n🎉 Automatic scrolling completed for artist {artist_id}!")
        print(f"📊 Scrolling Summary:")
        print(f"   🔢 Total scrolls performed: {scroll_count}")
        print(f"   📏 Initial height: {initial_scroll_height}px → Final: {final_height}px")
        print(f"   📍 Final position: {final_scroll_top}px")
        print(f"   📈 Content loaded: {final_height - initial_scroll_height}px")
        print(f"   🎵 New tracks found: {total_new_tracks}")
        
        if scroll_count >= Config.MAX_SCROLLS:
            print(f"⚠️  Reached maximum scroll limit ({Config.MAX_SCROLLS})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during automatic scrolling: {e}")
        return False
    
    finally:
        auto_scroll_active = False
        stop_capture = True

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
                
                if len(captured_data) % 10 == 0:  # Show progress every 10 requests
                    print(f"[+] Captured {len(captured_data)} requests for artist {current_artist_id}")
                
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
                    if tracks:
                        print(f"[+] Found {len(tracks)} new tracks for artist {current_artist_id} (Total: {len(all_artist_tracks) + len(tracks)})")
                        
                        for track_item in tracks:
                            track = track_item.get('track', {})
                            if track:
                                all_artist_tracks.append(track)
                
    except Exception as e:
        print(f"[!] Error in response interceptor: {e}")

def get_multiple_artist_ids():
    """Get multiple artist IDs from user input"""
    print("🎵 Enhanced Batch Spotify Artist Scraper with Auto-Scroll")
    print("=" * 70)
    print("This tool automatically scrolls and scrapes all songs from multiple artists")
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
            confirm = input(f"Process {len(artist_ids)} artists automatically? (y/N): ").strip().lower()
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

def handle_youtube_captcha():
    """Handle YouTube CAPTCHA by opening browser"""
    if Config.ALLOW_YOUTUBE_CAPTCHA:
        print("\n🤖 YouTube may require CAPTCHA verification.")
        print("   Opening YouTube in browser for manual verification...")
        
        try:
            import webbrowser
            webbrowser.open("https://www.youtube.com")
            print("   ✅ YouTube opened in browser")
            print("   👆 Please solve any CAPTCHA if prompted, then press Enter to continue")
            input("   Press Enter when ready...")
            return True
        except Exception as e:
            print(f"   ⚠️ Could not open browser: {e}")
            return False
    return False

def main():
    """Main function to run the enhanced batch artist scraper with auto-scroll"""
    global stop_capture, all_artist_tracks, captured_data, current_artist_id
    
    print("🎵 Enhanced Batch Spotify Artist Scraper with Auto-Scroll")
    print("=" * 70)
    
    # Set up logging for debugging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Test cookies before starting
    # Test cookies if available (matching FinalcodeWithCookies approach)
    cookies_file = Config.COOKIES_FILE
    if os.path.exists(cookies_file):
        print(f"🍪 Testing cookies from: {cookies_file}")
        try:
            import yt_dlp
            test_opts = {
                'quiet': True,
                'cookiefile': cookies_file,
                'skip_download': True,
                'extract_flat': True,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            }
            with yt_dlp.YoutubeDL(test_opts) as ydl:
                ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
            print("   ✅ Cookies are valid and working")
        except Exception as e:
            print(f"   ⚠️ Cookie test failed: {e}")
            print("   💡 You may need to update your cookies.txt file")
            print("   ⚠️ Continuing without valid cookies may result in bot detection")
            response = input("   Do you want to continue anyway? (y/n): ")
            if response.lower() != 'y':
                print("Exiting...")
                return
    
    # Check for cookies.txt file (matching FinalcodeWithCookies approach)
    cookies_file = Config.COOKIES_FILE
    if os.path.exists(cookies_file):
        print(f"🍪 Found cookies file: {cookies_file}")
        # Try to validate the cookies
        try:
            with open(cookies_file, 'r', encoding='utf-8') as f:
                cookie_lines = f.readlines()
                valid_cookie_lines = [line for line in cookie_lines if line.strip() and not line.startswith('#')]
                print(f"   📊 Cookie file contains {len(valid_cookie_lines)} valid entries")
        except Exception as e:
            print(f"   ⚠️ Error reading cookies file: {e}")
    else:
        print(f"⚠️ Warning: No cookies.txt file found")
        print(f"   This may cause YouTube to show 'confirm you're not a bot' errors")
        print(f"   Consider getting a valid cookies.txt file or setting Config.BROWSER_FOR_COOKIES")
        
        # Ask if user wants to continue without cookies
        continue_choice = input("Continue without valid cookies? (y/N): ").strip().lower()
        if continue_choice != 'y':
            print("Exiting. Please add a valid cookies.txt file and restart.")
            return
    
    # Check prerequisites
    if not check_prerequisites():
        print("❌ Prerequisites not met. Please install required tools.")
        return
    
    # Get multiple artist IDs from user
    artist_ids = get_multiple_artist_ids()
    Config.ARTIST_IDS = artist_ids
    
    print(f"\n🎯 Will process {len(artist_ids)} artists automatically:")
    for i, artist_id in enumerate(artist_ids, 1):
        print(f"   {i}. {artist_id}")
    
    print("\n🤖 Automatic Mode Enabled:")
    print("1. Browser will open and navigate to each artist automatically")
    print("2. Auto-scroll will load all tracks automatically")
    print("3. Script will detect when scrolling is complete")
    print("4. Automatically move to next artist")
    print("5. Process and download all songs after data collection")
    print("6. Press Enter to start the automated process...")
    input()
    
    # Dictionary to store all collected artist data
    all_artists_data = {}
    total_data_collected = 0
    total_errors = 0
    
    # Function to create a new browser session
    def create_new_browser_session():
        print(f"🌐 Creating new browser session...")
        # Setup browser with auto-scroll optimized options
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Rotate user agents if enabled
        if Config.ROTATE_USER_AGENTS:
            import random
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            ]
            selected_user_agent = random.choice(user_agents)
            options.add_argument(f'user-agent={selected_user_agent}')
            print(f"🔄 Using rotating user agent: {selected_user_agent}")
        
        # Use proxy if enabled and available
        selenium_wire_options = {}
        if Config.USE_PROXIES and Config.PROXY_LIST:
            import random
            selected_proxy = random.choice(Config.PROXY_LIST)
            selenium_wire_options = {
                'proxy': {
                    'http': selected_proxy,
                    'https': selected_proxy,
                    'no_proxy': 'localhost,127.0.0.1'
                }
            }
            print(f"🔒 Using proxy: {selected_proxy}")
        
        # Keep browser visible to monitor auto-scrolling
        driver = webdriver.Chrome(options=options, seleniumwire_options=selenium_wire_options)
        driver.request_interceptor = request_interceptor
        driver.response_interceptor = response_interceptor
        
        # Load cookies if available (matching FinalcodeWithCookies approach)
        cookies_file = Config.COOKIES_FILE
        if os.path.exists(cookies_file):
            print(f"🍪 Loading cookies from {cookies_file}")
            # We need to navigate to a page first before adding cookies
            driver.get("https://www.youtube.com")
            time.sleep(2)
            
            # Parse cookies from the file and add to the browser
            try:
                with open(cookies_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    
                for line in lines:
                    if line.strip() and not line.startswith('#'):
                        fields = line.strip().split('\t')
                        if len(fields) >= 7:  # Valid cookie line
                            domain, flag, path, secure, expiry, name, value = fields[:7]
                            cookie = {
                                'domain': domain,
                                'path': path,
                                'secure': secure.lower() == 'true',
                                'expiry': int(expiry) if expiry.isdigit() else None,
                                'name': name,
                                'value': value
                            }
                            try:
                                driver.add_cookie(cookie)
                            except Exception as e:
                                print(f"   ⚠️ Couldn't add cookie {name}: {e}")
                print("✅ Cookies loaded successfully")
            except Exception as e:
                print(f"❌ Error loading cookies: {e}")
        else:
            print(f"⚠️ No cookies.txt file found")
            
        return driver
    
    # Dictionary to store all collected artist data
    all_artists_data = {}
    total_data_collected = 0
    total_errors = 0
    
    try:
        print("🤖 Starting automated batch processing...")
        
        # PHASE 1: Automated data collection from all artist pages
        print(f"\n{'='*80}")
        print(f"🤖 PHASE 1: AUTOMATED DATA COLLECTION FROM ALL ARTISTS")
        print(f"{'='*80}")
        
        for i, artist_id in enumerate(artist_ids, 1):
            print(f"\n{'='*60}")
            print(f"🎤 Auto-Processing Artist {i}/{len(artist_ids)}: {artist_id}")
            print(f"{'='*60}")
            
            # Reset global variables for this artist
            current_artist_id = artist_id
            all_artist_tracks = []
            stop_capture = False
            
            # Create a new browser session for each artist
            driver = create_new_browser_session()
            
            try:
                # Construct artist discography URL
                artist_url = f"https://open.spotify.com/artist/{artist_id}/discography/all"
                print(f"🔗 Opening: {artist_url}")
                
                # Navigate to artist page
                driver.get(artist_url)
                print("⏳ Waiting for page to load...")
                
                # Start automatic scrolling
                scroll_success = automatic_scroll_to_bottom(driver, artist_id)
                
                if not scroll_success:
                    print(f"❌ Auto-scroll failed for artist {artist_id}")
                    total_errors += 1
                    continue
                
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
                    print(f"✅ Data collected automatically for: {artist_name}")
                else:
                    print(f"❌ No tracks found for artist {artist_id}")
                    total_errors += 1
                
                # Close the browser session for this artist
                print(f"🔄 Closing browser session for artist {artist_id}...")
                driver.quit()
                print(f"✅ Browser session closed for artist {artist_id}")
                
                # Automatic delay between artists
                if i < len(artist_ids):  # Don't delay after the last artist
                    print(f"⏳ Auto-waiting {Config.DELAY_BETWEEN_ARTISTS} seconds before next artist...")
                    time.sleep(Config.DELAY_BETWEEN_ARTISTS)
                
            except Exception as e:
                print(f"❌ Error in auto-processing artist {artist_id}: {e}")
                total_errors += 1
                # Make sure to close the browser session even if there's an error
                try:
                    driver.quit()
                    print(f"✅ Browser session closed after error")
                except:
                    pass
                continue
        
        # PHASE 2: Process all collected data and download songs
        print(f"\n{'='*80}")
        print(f"📊 PHASE 2: PROCESSING ALL COLLECTED DATA AND DOWNLOADING")
        print(f"{'='*80}")
        print(f"📈 Automated Collection Summary:")
        print(f"   ✅ Successfully collected data from: {total_data_collected} artists")
        print(f"   ❌ Failed to collect data from: {total_errors} artists")
        print(f"   📋 Total artists requested: {len(artist_ids)}")
        
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
            print(f"🎉 AUTOMATED BATCH PROCESSING COMPLETE!")
            print(f"{'='*80}")
            print(f"📊 Final Summary:")
            print(f"   🤖 Automated Collection Phase:")
            print(f"      ✅ Successfully auto-collected: {total_data_collected} artists")
            print(f"      ❌ Collection errors: {total_errors} artists")
            print(f"   📥 Processing & Download Phase:")
            print(f"      ✅ Successfully processed: {total_processed} artists")
            print(f"      ❌ Processing errors: {processing_errors} artists")
            print(f"   📋 Total artists requested: {len(artist_ids)}")
            print(f"\n🎵 All artist discographies have been automatically scraped and downloaded!")
        else:
            print(f"❌ No artist data was collected successfully!")
        
    except Exception as e:
        print(f"❌ Critical error in automated processing: {e}")
    
    finally:
        try:
            driver.quit()
        except:
            pass
        print("🎉 Automated batch processing complete!")
        print("Use this : python add_cover_art_urls.py")
        print("Use this : python code_push_songs.py")
        print("Use this : python code_push_metadata.py")
        print("Use this : python next_artist_finder.py")

# Run the main function
if __name__ == "__main__":
    main()