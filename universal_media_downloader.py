#!/usr/bin/env python3
"""
Universal Media Downloader
Download media from YouTube, Instagram, and other platforms using links or search.
"""

import os
import sys
import subprocess
import re
from pathlib import Path

def install_required_packages():
    """Install required packages if not available"""
    packages = ['yt-dlp', 'requests']
    
    for package in packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package} is available")
        except ImportError:
            print(f"📦 Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✅ {package} installed successfully")

def check_ffmpeg():
    """Check if ffmpeg is available"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ ffmpeg found")
            return True
        else:
            print("⚠️  ffmpeg not working properly")
            return False
    except FileNotFoundError:
        print("⚠️  ffmpeg not found - audio extraction may be limited")
        print("   Download from: https://ffmpeg.org/download.html")
        return False

def detect_platform(url):
    """Detect which platform the URL belongs to"""
    # YouTube patterns
    youtube_patterns = [
        r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/',
        r'(https?://)?(www\.)?youtu\.be/',
    ]
    
    # Instagram patterns
    instagram_patterns = [
        r'(https?://)?(www\.)?instagram\.com/',
        r'(https?://)?(www\.)?instagr\.am/',
    ]
    
    # TikTok patterns
    tiktok_patterns = [
        r'(https?://)?(www\.)?tiktok\.com/',
        r'(https?://)?(vm\.)?tiktok\.com/',
    ]
    
    # Twitter patterns
    twitter_patterns = [
        r'(https?://)?(www\.)?(twitter|x)\.com/',
    ]
    
    # Facebook patterns
    facebook_patterns = [
        r'(https?://)?(www\.)?facebook\.com/',
        r'(https?://)?(www\.)?fb\.watch/',
    ]
    
    for pattern in youtube_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return 'youtube'
    
    for pattern in instagram_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return 'instagram'
    
    for pattern in tiktok_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return 'tiktok'
    
    for pattern in twitter_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return 'twitter'
    
    for pattern in facebook_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return 'facebook'
    
    return 'unknown'

def get_instagram_media_type(url):
    """Determine Instagram media type"""
    if '/reel/' in url:
        return 'reel'
    elif '/tv/' in url:
        return 'igtv'
    elif '/stories/' in url:
        return 'story'
    elif '/p/' in url:
        return 'post'
    else:
        return 'unknown'

def download_media(url, platform, download_type, output_folder, quality='192'):
    """Universal media downloader"""
    try:
        import yt_dlp
        
        print(f"🔗 Processing {platform.upper()} URL: {url}")
        
        # Create output folder if it doesn't exist
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        # Base configuration
        ydl_opts = {
            'writeinfojson': True,  # Save media info
            'writethumbnail': True,  # Save thumbnail
        }
        
        # Platform-specific filename templates
        if platform == 'youtube':
            ydl_opts['outtmpl'] = str(Path(output_folder) / 'YT_%(title)s_%(id)s.%(ext)s')
        elif platform == 'instagram':
            ydl_opts['outtmpl'] = str(Path(output_folder) / 'IG_%(uploader)s_%(title)s_%(id)s.%(ext)s')
        elif platform == 'tiktok':
            ydl_opts['outtmpl'] = str(Path(output_folder) / 'TT_%(uploader)s_%(title)s_%(id)s.%(ext)s')
        elif platform == 'twitter':
            ydl_opts['outtmpl'] = str(Path(output_folder) / 'TW_%(uploader)s_%(title)s_%(id)s.%(ext)s')
        elif platform == 'facebook':
            ydl_opts['outtmpl'] = str(Path(output_folder) / 'FB_%(uploader)s_%(title)s_%(id)s.%(ext)s')
        else:
            ydl_opts['outtmpl'] = str(Path(output_folder) / '%(uploader)s_%(title)s_%(id)s.%(ext)s')
        
        # Configure download type
        if download_type == "video":
            ydl_opts['format'] = 'best[ext=mp4]/best'
            format_desc = "video (MP4)"
        elif download_type == "audio":
            ydl_opts.update({
                'format': 'bestaudio/best',
                'extractaudio': True,
                'audioformat': 'mp3',
                'audioquality': quality,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': quality,
                }],
            })
            format_desc = f"audio (MP3 {quality}K)"
        else:  # best
            ydl_opts['format'] = 'best'
            format_desc = "best quality"
        
        print(f"📥 Downloading {format_desc}...")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Get media info
            try:
                info = ydl.extract_info(url, download=False)
                
                title = info.get('title', 'Unknown')
                uploader = info.get('uploader', 'Unknown')
                duration = info.get('duration', 0)
                
                print(f"📱 Platform: {platform.upper()}")
                
                if platform == 'instagram':
                    media_type = get_instagram_media_type(url)
                    print(f"📱 Media Type: {media_type.upper()}")
                
                print(f"📝 Title: {title}")
                print(f"👤 Uploader: {uploader}")
                
                if duration:
                    minutes, seconds = divmod(duration, 60)
                    print(f"⏱️  Duration: {minutes:02d}:{seconds:02d}")
                
                # Check for multiple items (playlists, carousels, etc.)
                if 'entries' in info:
                    print(f"📸 Multiple items detected: {len(info['entries'])} items")
                
            except Exception as e:
                print(f"⚠️  Could not extract full info: {e}")
                print("🔄 Proceeding with download...")
            
            # Download the media
            ydl.download([url])
            
            print(f"✅ Successfully downloaded from {platform.upper()}!")
            return True
            
    except Exception as e:
        print(f"❌ Download failed: {e}")
        
        # Provide platform-specific error messages
        error_str = str(e).lower()
        if "private" in error_str or "login required" in error_str:
            print("💡 Tip: This might be private content or require login.")
        elif "not available" in error_str:
            print("💡 Tip: The media might have been deleted or is region-locked.")
        elif "age restriction" in error_str:
            print("💡 Tip: This media might have age restrictions.")
        elif platform == 'instagram' and "stories" in url:
            print("💡 Tip: Instagram stories are only available for 24 hours.")
        
        return False

def search_and_download(query, download_type, output_folder, quality='192'):
    """Search for content and download (YouTube only)"""
    try:
        import yt_dlp
        
        print(f"🔍 Searching for: {query}")
        
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        # Configure for search
        ydl_opts = {
            'outtmpl': str(Path(output_folder) / 'YT_SEARCH_%(title)s_%(id)s.%(ext)s'),
            'writeinfojson': True,
        }
        
        if download_type == "video":
            ydl_opts['format'] = 'best[ext=mp4]/best'
            search_query = f"ytsearch1:{query}"
        else:  # audio
            ydl_opts.update({
                'format': 'bestaudio/best',
                'extractaudio': True,
                'audioformat': 'mp3',
                'audioquality': quality,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': quality,
                }],
            })
            search_query = f"ytsearch1:{query}"
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_query, download=True)
            
            if info and 'entries' in info and len(info['entries']) > 0:
                entry = info['entries'][0]
                title = entry.get('title', 'Unknown')
                uploader = entry.get('uploader', 'Unknown')
                
                print(f"📹 Found: {title}")
                print(f"👤 Uploader: {uploader}")
                print(f"✅ Successfully downloaded!")
                return True
            else:
                print(f"❌ No results found for: {query}")
                return False
                
    except Exception as e:
        print(f"❌ Search and download failed: {e}")
        return False

def get_download_folder():
    """Get or create download folder"""
    downloads_folder = Path("media_downloads")
    downloads_folder.mkdir(exist_ok=True)
    return downloads_folder

def main():
    """Main function"""
    print("🌐 Universal Media Downloader")
    print("=" * 50)
    print("Supports: YouTube, Instagram, TikTok, Twitter, Facebook")
    print()
    
    # Check prerequisites
    print("🔧 Checking prerequisites...")
    install_required_packages()
    ffmpeg_available = check_ffmpeg()
    print()
    
    while True:
        try:
            print("🎯 Choose download method:")
            print("1. Enter media URL (any supported platform)")
            print("2. Search by name (YouTube only)")
            print("3. Exit")
            
            choice = input("\nEnter choice (1-3): ").strip()
            
            if choice == "3":
                print("👋 Goodbye!")
                break
            elif choice not in ["1", "2"]:
                print("❌ Invalid choice. Please enter 1, 2, or 3.")
                continue
            
            if choice == "1":
                # URL mode
                url = input("\n🔗 Enter media URL: ").strip()
                
                if not url:
                    print("❌ No URL provided.")
                    continue
                
                # Detect platform
                platform = detect_platform(url)
                
                if platform == 'unknown':
                    print("❌ Unsupported platform or invalid URL.")
                    print("Supported platforms: YouTube, Instagram, TikTok, Twitter, Facebook")
                    continue
                
                print(f"🎯 Detected platform: {platform.upper()}")
                
                # Get download type
                print("\n📁 Choose download type:")
                print("1. Best quality")
                print("2. Video only (MP4)")
                if ffmpeg_available:
                    print("3. Audio only (MP3)")
                
                type_choice = input("Enter choice (1-3): ").strip()
                
                if type_choice == "1":
                    download_type = "best"
                    quality = None
                elif type_choice == "2":
                    download_type = "video"
                    quality = None
                elif type_choice == "3" and ffmpeg_available:
                    download_type = "audio"
                    # Get audio quality
                    print("\n🎵 Choose audio quality:")
                    print("1. 128K (Good)")
                    print("2. 192K (Better)")
                    print("3. 320K (Best)")
                    
                    quality_choice = input("Enter choice (1-3): ").strip()
                    quality_map = {"1": "128", "2": "192", "3": "320"}
                    quality = quality_map.get(quality_choice, "192")
                else:
                    print("❌ Invalid choice or audio extraction not available.")
                    continue
                
                # Download
                output_folder = get_download_folder()
                print(f"📂 Downloads will be saved to: {output_folder.absolute()}")
                
                success = download_media(url, platform, download_type, output_folder, quality)
                
            elif choice == "2":
                # Search mode (YouTube only)
                query = input("\n🔍 Enter search query: ").strip()
                
                if not query:
                    print("❌ No search query provided.")
                    continue
                
                # Get download type
                print("\n📁 Choose download type:")
                print("1. Video (MP4)")
                if ffmpeg_available:
                    print("2. Audio only (MP3)")
                
                type_choice = input("Enter choice (1-2): ").strip()
                
                if type_choice == "1":
                    download_type = "video"
                    quality = None
                elif type_choice == "2" and ffmpeg_available:
                    download_type = "audio"
                    # Get audio quality
                    print("\n🎵 Choose audio quality:")
                    print("1. 128K (Good)")
                    print("2. 192K (Better)")
                    print("3. 320K (Best)")
                    
                    quality_choice = input("Enter choice (1-3): ").strip()
                    quality_map = {"1": "128", "2": "192", "3": "320"}
                    quality = quality_map.get(quality_choice, "192")
                else:
                    print("❌ Invalid choice.")
                    continue
                
                # Search and download
                output_folder = get_download_folder()
                print(f"📂 Downloads will be saved to: {output_folder.absolute()}")
                
                success = search_and_download(query, download_type, output_folder, quality)
            
            if success:
                print(f"\n🎉 Download completed successfully!")
                print(f"📂 Check the folder for your downloaded file.")
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
