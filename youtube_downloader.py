#!/usr/bin/env python3
"""
YouTube Downloader
Download YouTube videos/audio using either direct links or song search.
"""

import os
import sys
import subprocess
import re
from pathlib import Path

def install_required_packages():
    """Install required packages if not available"""
    try:
        import yt_dlp
        print("✅ yt-dlp is available")
    except ImportError:
        print("📦 Installing yt-dlp...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yt-dlp"])
        print("✅ yt-dlp installed successfully")

def check_ffmpeg():
    """Check if ffmpeg is available"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ ffmpeg found")
            return True
        else:
            print("❌ ffmpeg not working properly")
            return False
    except FileNotFoundError:
        print("❌ ffmpeg not found - please install ffmpeg")
        print("   Download from: https://ffmpeg.org/download.html")
        return False

def is_youtube_url(url):
    """Check if the provided string is a valid YouTube URL"""
    youtube_patterns = [
        r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/',
        r'(https?://)?(www\.)?youtu\.be/',
        r'(https?://)?(www\.)?youtube\.com/watch\?v=',
        r'(https?://)?(www\.)?youtube\.com/embed/',
        r'(https?://)?(www\.)?youtube\.com/v/'
    ]
    
    for pattern in youtube_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False

def download_from_url(url, download_type, output_folder, quality='192'):
    """Download from YouTube URL"""
    try:
        import yt_dlp
        
        print(f"🔗 Processing URL: {url}")
        
        # Create output folder if it doesn't exist
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        if download_type == "video":
            # Download video (MP4)
            ydl_opts = {
                'format': 'best[ext=mp4]/best',
                'outtmpl': str(Path(output_folder) / '%(title)s.%(ext)s'),
                'writeinfojson': True,  # Save video info
                'writethumbnail': True,  # Save thumbnail
            }
            format_desc = "video (MP4)"
        else:
            # Download audio only (MP3)
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': str(Path(output_folder) / '%(title)s.%(ext)s'),
                'extractaudio': True,
                'audioformat': 'mp3',
                'audioquality': quality,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': quality,
                }],
                'writeinfojson': True,  # Save video info
            }
            format_desc = f"audio (MP3 {quality}K)"
        
        print(f"📥 Downloading {format_desc}...")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Get video info first
            info = ydl.extract_info(url, download=False)
            title = info.get('title', 'Unknown')
            duration = info.get('duration', 0)
            uploader = info.get('uploader', 'Unknown')
            
            print(f"📹 Title: {title}")
            print(f"👤 Uploader: {uploader}")
            if duration:
                minutes, seconds = divmod(duration, 60)
                print(f"⏱️  Duration: {minutes:02d}:{seconds:02d}")
            
            # Download the video/audio
            ydl.download([url])
            
            print(f"✅ Successfully downloaded: {title}")
            return True
            
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

def search_and_download(query, download_type, output_folder, quality='192'):
    """Search for a song and download it"""
    try:
        import yt_dlp
        
        print(f"🔍 Searching for: {query}")
        
        # Create output folder if it doesn't exist
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        if download_type == "video":
            # Download video (MP4)
            ydl_opts = {
                'format': 'best[ext=mp4]/best',
                'outtmpl': str(Path(output_folder) / '%(title)s.%(ext)s'),
                'writeinfojson': True,  # Save video info
                'writethumbnail': True,  # Save thumbnail
            }
            format_desc = "video (MP4)"
            search_query = f"ytsearch1:{query}"
        else:
            # Download audio only (MP3)
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': str(Path(output_folder) / '%(title)s.%(ext)s'),
                'extractaudio': True,
                'audioformat': 'mp3',
                'audioquality': quality,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': quality,
                }],
                'writeinfojson': True,  # Save video info
            }
            format_desc = f"audio (MP3 {quality}K)"
            search_query = f"ytsearch1:{query}"
        
        print(f"📥 Downloading {format_desc}...")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Search and download
            info = ydl.extract_info(search_query, download=True)
            
            if info and 'entries' in info and len(info['entries']) > 0:
                entry = info['entries'][0]
                title = entry.get('title', 'Unknown')
                uploader = entry.get('uploader', 'Unknown')
                duration = entry.get('duration', 0)
                
                print(f"📹 Found: {title}")
                print(f"👤 Uploader: {uploader}")
                if duration:
                    minutes, seconds = divmod(duration, 60)
                    print(f"⏱️  Duration: {minutes:02d}:{seconds:02d}")
                
                print(f"✅ Successfully downloaded: {title}")
                return True
            else:
                print(f"❌ No results found for: {query}")
                return False
                
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

def get_download_folder():
    """Get or create download folder"""
    downloads_folder = Path("downloads")
    downloads_folder.mkdir(exist_ok=True)
    return downloads_folder

def main():
    """Main function"""
    print("🎵 YouTube Video/Audio Downloader")
    print("=" * 50)
    
    # Check prerequisites
    print("🔧 Checking prerequisites...")
    install_required_packages()
    if not check_ffmpeg():
        print("⚠️  Warning: ffmpeg not found. Audio extraction may not work.")
        print("   You can still download videos, but audio-only downloads will fail.")
        ffmpeg_available = False
    else:
        ffmpeg_available = True
    
    print()
    
    while True:
        try:
            print("🎯 Choose download method:")
            print("1. Enter YouTube URL")
            print("2. Search by song/video name")
            print("3. Exit")
            
            choice = input("\nEnter choice (1-3): ").strip()
            
            if choice == "3":
                print("👋 Goodbye!")
                break
            elif choice not in ["1", "2"]:
                print("❌ Invalid choice. Please enter 1, 2, or 3.")
                continue
            
            # Get download type
            print("\n📁 Choose download type:")
            print("1. Audio only (MP3)")
            print("2. Video (MP4)")
            
            type_choice = input("Enter choice (1-2): ").strip()
            
            if type_choice == "1":
                if not ffmpeg_available:
                    print("❌ Audio extraction requires ffmpeg. Please install ffmpeg first.")
                    continue
                download_type = "audio"
                # Get audio quality
                print("\n🎵 Choose audio quality:")
                print("1. 128K (Good)")
                print("2. 192K (Better)")
                print("3. 320K (Best)")
                
                quality_choice = input("Enter choice (1-3): ").strip()
                quality_map = {"1": "128", "2": "192", "3": "320"}
                quality = quality_map.get(quality_choice, "192")
                
            elif type_choice == "2":
                download_type = "video"
                quality = None
            else:
                print("❌ Invalid choice. Please enter 1 or 2.")
                continue
            
            # Get output folder
            output_folder = get_download_folder()
            print(f"📂 Downloads will be saved to: {output_folder.absolute()}")
            
            if choice == "1":
                # URL mode
                url = input("\n🔗 Enter YouTube URL: ").strip()
                
                if not url:
                    print("❌ No URL provided.")
                    continue
                
                if not is_youtube_url(url):
                    print("❌ Invalid YouTube URL. Please enter a valid YouTube link.")
                    continue
                
                success = download_from_url(url, download_type, output_folder, quality)
                
            elif choice == "2":
                # Search mode
                query = input("\n🎵 Enter song/video name: ").strip()
                
                if not query:
                    print("❌ No search query provided.")
                    continue
                
                success = search_and_download(query, download_type, output_folder, quality)
            
            if success:
                print(f"\n🎉 Download completed successfully!")
                print(f"📂 Check the '{output_folder}' folder for your downloaded file.")
            else:
                print(f"\n❌ Download failed. Please try again.")
            
            print("\n" + "="*50)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            continue

if __name__ == "__main__":
    main()
