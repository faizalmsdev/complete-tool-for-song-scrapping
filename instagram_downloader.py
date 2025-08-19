#!/usr/bin/env python3
"""
Instagram Media Downloader
Download Instagram reels, videos, images, and stories using links.
"""

import os
import sys
import subprocess
import re
import requests
from pathlib import Path
from urllib.parse import urlparse

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

def is_instagram_url(url):
    """Check if the provided string is a valid Instagram URL"""
    instagram_patterns = [
        r'(https?://)?(www\.)?instagram\.com/p/',           # Posts
        r'(https?://)?(www\.)?instagram\.com/reel/',        # Reels
        r'(https?://)?(www\.)?instagram\.com/tv/',          # IGTV
        r'(https?://)?(www\.)?instagram\.com/stories/',     # Stories
        r'(https?://)?(www\.)?instagr\.am/',               # Short links
    ]
    
    for pattern in instagram_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False

def get_media_type(url):
    """Determine the type of Instagram media from URL"""
    if '/reel/' in url:
        return 'reel'
    elif '/tv/' in url:
        return 'igtv'
    elif '/stories/' in url:
        return 'story'
    elif '/p/' in url:
        return 'post'  # Could be photo, video, or carousel
    else:
        return 'unknown'

def download_instagram_media(url, output_folder, download_format='best'):
    """Download Instagram media using yt-dlp"""
    try:
        import yt_dlp
        
        print(f"🔗 Processing Instagram URL: {url}")
        
        # Create output folder if it doesn't exist
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        # Configure yt-dlp options
        ydl_opts = {
            'outtmpl': str(Path(output_folder) / '%(uploader)s_%(title)s_%(id)s.%(ext)s'),
            'writeinfojson': True,  # Save media info
            'writethumbnail': True,  # Save thumbnail if available
        }
        
        # Set format based on user preference
        if download_format == 'video_only':
            ydl_opts['format'] = 'best[ext=mp4]/best'
        elif download_format == 'audio_only':
            ydl_opts.update({
                'format': 'bestaudio/best',
                'extractaudio': True,
                'audioformat': 'mp3',
                'audioquality': '192',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            })
        else:  # best quality (default)
            ydl_opts['format'] = 'best'
        
        print(f"📥 Downloading Instagram media...")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Get media info first
            try:
                info = ydl.extract_info(url, download=False)
                
                title = info.get('title', 'Unknown')
                uploader = info.get('uploader', 'Unknown')
                description = info.get('description', '')
                duration = info.get('duration', 0)
                media_type = get_media_type(url)
                
                print(f"📱 Media Type: {media_type.upper()}")
                print(f"📝 Title: {title}")
                print(f"👤 Uploader: {uploader}")
                
                if duration:
                    minutes, seconds = divmod(duration, 60)
                    print(f"⏱️  Duration: {minutes:02d}:{seconds:02d}")
                
                if description and len(description) > 0:
                    # Truncate long descriptions
                    desc_preview = description[:100] + "..." if len(description) > 100 else description
                    print(f"📄 Description: {desc_preview}")
                
                # Check if it's a carousel (multiple images/videos)
                if 'entries' in info:
                    print(f"📸 Carousel detected with {len(info['entries'])} items")
                
            except Exception as e:
                print(f"⚠️  Could not extract info: {e}")
                print("🔄 Proceeding with download anyway...")
            
            # Download the media
            ydl.download([url])
            
            print(f"✅ Successfully downloaded Instagram media!")
            return True
            
    except Exception as e:
        print(f"❌ Download failed: {e}")
        
        # Check for common errors and provide helpful messages
        if "Private account" in str(e) or "login required" in str(e).lower():
            print("💡 Tip: This might be a private account or require login.")
            print("   Try using a public post URL instead.")
        elif "not available" in str(e).lower():
            print("💡 Tip: The media might have been deleted or is no longer available.")
        elif "age restriction" in str(e).lower():
            print("💡 Tip: This media might have age restrictions.")
        
        return False

def download_instagram_simple(url, output_folder):
    """Fallback method using requests for simple image downloads"""
    try:
        print(f"🔄 Trying alternative download method...")
        
        # This is a very basic fallback - in practice, Instagram's API restrictions
        # make this difficult without proper authentication
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            print("⚠️  Note: Basic fallback method has limited functionality.")
            print("   For full features, ensure yt-dlp is working properly.")
            return False
        else:
            return False
            
    except Exception as e:
        print(f"❌ Fallback method also failed: {e}")
        return False

def get_download_folder():
    """Get or create download folder for Instagram media"""
    downloads_folder = Path("instagram_downloads")
    downloads_folder.mkdir(exist_ok=True)
    return downloads_folder

def main():
    """Main function"""
    print("📱 Instagram Media Downloader")
    print("=" * 50)
    
    # Check prerequisites
    print("🔧 Checking prerequisites...")
    install_required_packages()
    
    # Check ffmpeg for video processing
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ ffmpeg found")
            ffmpeg_available = True
        else:
            print("⚠️  ffmpeg not working properly")
            ffmpeg_available = False
    except FileNotFoundError:
        print("⚠️  ffmpeg not found - video processing may be limited")
        print("   Download from: https://ffmpeg.org/download.html")
        ffmpeg_available = False
    
    print()
    
    while True:
        try:
            print("📱 Instagram Media Downloader")
            print("-" * 30)
            print("Supports:")
            print("• Instagram Posts (photos/videos)")
            print("• Instagram Reels")
            print("• Instagram IGTV")
            print("• Instagram Stories (if public)")
            print("• Carousel posts (multiple images/videos)")
            print()
            
            # Get Instagram URL
            url = input("🔗 Enter Instagram URL (or 'exit' to quit): ").strip()
            
            if url.lower() in ['exit', 'quit', 'q']:
                print("👋 Goodbye!")
                break
            
            if not url:
                print("❌ No URL provided.")
                continue
            
            if not is_instagram_url(url):
                print("❌ Invalid Instagram URL. Please enter a valid Instagram link.")
                print("   Examples:")
                print("   • https://www.instagram.com/p/ABC123/")
                print("   • https://www.instagram.com/reel/XYZ789/")
                continue
            
            # Get download format preference
            print("\n📁 Choose download format:")
            print("1. Best quality (default)")
            print("2. Video only (MP4)")
            if ffmpeg_available:
                print("3. Audio only (MP3) - for videos with sound")
            
            format_choice = input("Enter choice (1-3): ").strip()
            
            format_map = {
                "1": "best",
                "2": "video_only",
                "3": "audio_only" if ffmpeg_available else "best"
            }
            
            download_format = format_map.get(format_choice, "best")
            
            if format_choice == "3" and not ffmpeg_available:
                print("⚠️  Audio extraction requires ffmpeg. Using best quality instead.")
                download_format = "best"
            
            # Get output folder
            output_folder = get_download_folder()
            print(f"📂 Downloads will be saved to: {output_folder.absolute()}")
            
            # Determine media type
            media_type = get_media_type(url)
            print(f"🎯 Detected media type: {media_type.upper()}")
            
            # Download the media
            success = download_instagram_media(url, output_folder, download_format)
            
            if not success:
                print("\n🔄 Trying alternative method...")
                success = download_instagram_simple(url, output_folder)
            
            if success:
                print(f"\n🎉 Download completed successfully!")
                print(f"📂 Check the '{output_folder}' folder for your downloaded file.")
            else:
                print(f"\n❌ Download failed.")
                print("💡 Troubleshooting tips:")
                print("   • Make sure the post is public")
                print("   • Check if the URL is correct and complete")
                print("   • Some Instagram content may require login")
                print("   • Stories are only available for 24 hours")
            
            print("\n" + "="*50)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            continue

if __name__ == "__main__":
    main()
