#!/usr/bin/env python3
"""
Song Cleanup Tool
=================
This tool scans the songs folder, identifies songs that need to be removed,
and cleans up all associated metadata from the database files.
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional

# === CONFIGURATION ===
class Config:
    # Paths
    CONSOLIDATED_FOLDER = "consolidated_music"
    SONGS_FOLDER = os.path.join(CONSOLIDATED_FOLDER, "songs")
    METADATA_FOLDER = os.path.join(CONSOLIDATED_FOLDER, "metadata")
    
    # Database files
    SONGS_DB_FILE = os.path.join(METADATA_FOLDER, "songs_database.json")
    PLAYLISTS_DB_FILE = os.path.join(METADATA_FOLDER, "playlists_database.json")
    MAPPING_DB_FILE = os.path.join(METADATA_FOLDER, "song_playlist_mapping.json")
    ARTISTS_DB_FILE = os.path.join(METADATA_FOLDER, "artists_database.json")

# === UTILITY FUNCTIONS ===
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

def save_json_file(file_path: str, data: dict) -> bool:
    """Save JSON file with error handling"""
    try:
        # Create backup first
        backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        if os.path.exists(file_path):
            import shutil
            shutil.copy2(file_path, backup_path)
            print(f"📋 Created backup: {os.path.basename(backup_path)}")
        
        # Save new data
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"❌ Error saving {file_path}: {e}")
        return False

def scan_songs_folder() -> List[str]:
    """Scan songs folder and return list of song files"""
    if not os.path.exists(Config.SONGS_FOLDER):
        print(f"❌ Songs folder not found: {Config.SONGS_FOLDER}")
        return []
    
    song_files = []
    for file in os.listdir(Config.SONGS_FOLDER):
        if file.endswith('.mp3') and file.startswith('song_'):
            song_files.append(file)
    
    return sorted(song_files)

def extract_song_id_from_filename(filename: str) -> str:
    """Extract song ID from filename (removes .mp3 extension)"""
    if filename.endswith('.mp3'):
        return filename[:-4]  # Remove .mp3
    return filename

def get_song_metadata_from_id(song_id: str, songs_db: dict) -> Optional[dict]:
    """Get song metadata from songs database"""
    songs = songs_db.get('songs', {})
    song_info = songs.get(song_id)
    
    if song_info:
        metadata = song_info.get('metadata', {})
        return {
            'track_name': metadata.get('track_name', 'Unknown Track'),
            'artists_string': metadata.get('artists_string', 'Unknown Artist'),
            'album_name': metadata.get('album_name', 'Unknown Album'),
            'duration_formatted': metadata.get('duration_formatted', '0:00'),
            'playlists': song_info.get('playlists', [])
        }
    return None

# === MAIN CLEANUP CLASS ===
class SongCleanupTool:
    def __init__(self):
        self.songs_db = {}
        self.playlists_db = {}
        self.mapping_db = {}
        self.artists_db = {}
        self.load_databases()
    
    def load_databases(self):
        """Load all required databases"""
        print("📚 Loading databases...")
        
        self.songs_db = load_json_file(Config.SONGS_DB_FILE)
        songs_count = len(self.songs_db.get('songs', {}))
        print(f"   📜 Songs database: {songs_count} songs")
        
        self.playlists_db = load_json_file(Config.PLAYLISTS_DB_FILE)
        playlists_count = len(self.playlists_db.get('playlists', {}))
        print(f"   📋 Playlists database: {playlists_count} playlists")
        
        self.mapping_db = load_json_file(Config.MAPPING_DB_FILE)
        mapping_count = len(self.mapping_db.get('mapping', {}))
        print(f"   🔗 Mapping database: {mapping_count} song mappings")
        
        self.artists_db = load_json_file(Config.ARTISTS_DB_FILE)
        artists_count = len(self.artists_db.get('artists', {}))
        print(f"   🎤 Artists database: {artists_count} artists")
    
    def scan_and_identify_songs(self) -> List[dict]:
        """Scan songs folder and identify all songs with their metadata"""
        print("\n🔍 Scanning songs folder...")
        
        song_files = scan_songs_folder()
        print(f"   📁 Found {len(song_files)} song files in folder")
        
        identified_songs = []
        
        for filename in song_files:
            song_id = extract_song_id_from_filename(filename)
            metadata = get_song_metadata_from_id(song_id, self.songs_db)
            
            song_info = {
                'filename': filename,
                'song_id': song_id,
                'file_path': os.path.join(Config.SONGS_FOLDER, filename),
                'file_exists': True,
                'in_database': metadata is not None,
                'metadata': metadata
            }
            
            identified_songs.append(song_info)
        
        # Also check for songs in database but not in folder
        songs_in_db = self.songs_db.get('songs', {})
        for song_id in songs_in_db:
            expected_filename = f"{song_id}.mp3"
            expected_path = os.path.join(Config.SONGS_FOLDER, expected_filename)
            
            if not os.path.exists(expected_path):
                metadata = get_song_metadata_from_id(song_id, self.songs_db)
                song_info = {
                    'filename': expected_filename,
                    'song_id': song_id,
                    'file_path': expected_path,
                    'file_exists': False,
                    'in_database': True,
                    'metadata': metadata
                }
                identified_songs.append(song_info)
        
        return identified_songs
    
    def display_songs_summary(self, songs: List[dict]):
        """Display summary of identified songs"""
        print(f"\n📊 Songs Summary:")
        
        files_exist = [s for s in songs if s['file_exists']]
        files_missing = [s for s in songs if not s['file_exists']]
        in_database = [s for s in songs if s['in_database']]
        not_in_database = [s for s in songs if not s['in_database']]
        
        print(f"   📁 Files in folder: {len(files_exist)}")
        print(f"   ❌ Files missing: {len(files_missing)}")
        print(f"   📚 In database: {len(in_database)}")
        print(f"   ⚠️  Not in database: {len(not_in_database)}")
        
        # Show examples
        if files_missing:
            print(f"\n⚠️  Examples of missing files:")
            for song in files_missing[:5]:
                metadata = song['metadata']
                if metadata:
                    print(f"   - {metadata['track_name']} by {metadata['artists_string']}")
        
        if not_in_database:
            print(f"\n⚠️  Examples of files not in database:")
            for song in not_in_database[:5]:
                print(f"   - {song['filename']} ({song['song_id']})")
    
    def select_songs_for_removal(self, songs: List[dict]) -> List[dict]:
        """Interactive song selection for removal"""
        print(f"\n🎯 Song Selection for Removal")
        print("=" * 50)
        
        removal_options = {
            '1': 'Remove files missing from folder (database cleanup)',
            '2': 'Remove files not in database (orphaned files)',
            '3': 'Remove specific songs by search',
            '4': 'Remove songs from specific playlists',
            '5': 'Show all songs and select manually',
            '6': 'Cancel - don\'t remove anything'
        }
        
        print("Select removal option:")
        for key, desc in removal_options.items():
            print(f"   {key}. {desc}")
        
        while True:
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == '1':
                return [s for s in songs if not s['file_exists'] and s['in_database']]
            
            elif choice == '2':
                return [s for s in songs if s['file_exists'] and not s['in_database']]
            
            elif choice == '3':
                return self.search_and_select_songs(songs)
            
            elif choice == '4':
                return self.select_songs_by_playlist(songs)
            
            elif choice == '5':
                return self.manual_song_selection(songs)
            
            elif choice == '6':
                print("❌ Cancelled - no songs will be removed")
                return []
            
            else:
                print("❌ Invalid choice. Please enter 1-6.")
    
    def search_and_select_songs(self, songs: List[dict]) -> List[dict]:
        """Search for songs by name/artist and select for removal"""
        search_term = input("Enter search term (song name or artist): ").strip().lower()
        
        if not search_term:
            print("❌ No search term provided")
            return []
        
        matching_songs = []
        for song in songs:
            if song['metadata']:
                track_name = song['metadata']['track_name'].lower()
                artists = song['metadata']['artists_string'].lower()
                
                if search_term in track_name or search_term in artists:
                    matching_songs.append(song)
        
        if not matching_songs:
            print(f"❌ No songs found matching '{search_term}'")
            return []
        
        print(f"\n🔍 Found {len(matching_songs)} songs matching '{search_term}':")
        for i, song in enumerate(matching_songs, 1):
            metadata = song['metadata']
            status = "📁" if song['file_exists'] else "❌"
            print(f"   {i}. {status} {metadata['track_name']} by {metadata['artists_string']}")
        
        confirm = input(f"\nRemove all {len(matching_songs)} matching songs? (y/n): ").strip().lower()
        if confirm in ['y', 'yes']:
            return matching_songs
        else:
            return []
    
    def select_songs_by_playlist(self, songs: List[dict]) -> List[dict]:
        """Select songs that belong to specific playlists"""
        playlists = self.playlists_db.get('playlists', {})
        
        if not playlists:
            print("❌ No playlists found in database")
            return []
        
        print(f"\n📋 Available playlists:")
        playlist_list = list(playlists.keys())
        for i, playlist_key in enumerate(playlist_list, 1):
            playlist_info = playlists[playlist_key]
            print(f"   {i}. {playlist_info['name']} ({playlist_info.get('total_tracks', 0)} tracks)")
        
        try:
            choice = int(input(f"\nSelect playlist (1-{len(playlist_list)}): ").strip())
            if 1 <= choice <= len(playlist_list):
                selected_playlist = playlist_list[choice - 1]
                
                # Find songs belonging to this playlist
                playlist_songs = []
                for song in songs:
                    if song['metadata'] and selected_playlist in song['metadata'].get('playlists', []):
                        playlist_songs.append(song)
                
                if playlist_songs:
                    playlist_name = playlists[selected_playlist]['name']
                    print(f"\n📋 Found {len(playlist_songs)} songs in playlist '{playlist_name}'")
                    
                    confirm = input(f"Remove all {len(playlist_songs)} songs from this playlist? (y/n): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        return playlist_songs
                else:
                    print(f"❌ No songs found for selected playlist")
            else:
                print("❌ Invalid playlist selection")
        except ValueError:
            print("❌ Invalid input")
        
        return []
    
    def manual_song_selection(self, songs: List[dict]) -> List[dict]:
        """Manual selection of songs for removal"""
        print(f"\n📜 All Songs (showing first 50):")
        
        display_songs = songs[:50]
        for i, song in enumerate(display_songs, 1):
            status = "📁" if song['file_exists'] else "❌"
            db_status = "📚" if song['in_database'] else "⚠️"
            
            if song['metadata']:
                print(f"   {i:2d}. {status}{db_status} {song['metadata']['track_name']} by {song['metadata']['artists_string']}")
            else:
                print(f"   {i:2d}. {status}{db_status} {song['filename']} (no metadata)")
        
        if len(songs) > 50:
            print(f"   ... and {len(songs) - 50} more songs")
        
        print(f"\nStatus: 📁=File exists, ❌=Missing file, 📚=In database, ⚠️=Not in database")
        print(f"Enter song numbers to remove (comma-separated, e.g., '1,3,5-10'):")
        
        selection = input("Song numbers: ").strip()
        
        if not selection:
            return []
        
        selected_songs = []
        try:
            # Parse selection (supports ranges like "5-10")
            parts = selection.split(',')
            indices = set()
            
            for part in parts:
                part = part.strip()
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    indices.update(range(start, end + 1))
                else:
                    indices.add(int(part))
            
            for i in indices:
                if 1 <= i <= len(display_songs):
                    selected_songs.append(display_songs[i - 1])
            
            print(f"✅ Selected {len(selected_songs)} songs for removal")
            
        except ValueError:
            print("❌ Invalid selection format")
        
        return selected_songs
    
    def remove_songs_from_databases(self, songs_to_remove: List[dict]):
        """Remove selected songs from all databases"""
        if not songs_to_remove:
            print("✅ No songs to remove")
            return
        
        print(f"\n🗑️  Removing {len(songs_to_remove)} songs from databases...")
        
        removed_files = 0
        removed_from_db = 0
        updated_playlists = set()
        
        for song in songs_to_remove:
            song_id = song['song_id']
            
            # Remove physical file if it exists
            if song['file_exists']:
                try:
                    os.remove(song['file_path'])
                    print(f"   🗑️  Deleted file: {song['filename']}")
                    removed_files += 1
                except Exception as e:
                    print(f"   ❌ Failed to delete {song['filename']}: {e}")
            
            # Remove from songs database
            if song_id in self.songs_db.get('songs', {}):
                del self.songs_db['songs'][song_id]
                removed_from_db += 1
                print(f"   📚 Removed from songs database: {song_id}")
            
            # Remove from mapping database
            if song_id in self.mapping_db.get('mapping', {}):
                del self.mapping_db['mapping'][song_id]
                print(f"   🔗 Removed from mapping database: {song_id}")
            
            # Update playlists to remove this song
            if song['metadata']:
                playlists = song['metadata'].get('playlists', [])
                for playlist_key in playlists:
                    if playlist_key in self.playlists_db.get('playlists', {}):
                        playlist_songs = self.playlists_db['playlists'][playlist_key].get('songs', [])
                        if song_id in playlist_songs:
                            playlist_songs.remove(song_id)
                            self.playlists_db['playlists'][playlist_key]['songs'] = playlist_songs
                            self.playlists_db['playlists'][playlist_key]['total_tracks'] = len(playlist_songs)
                            self.playlists_db['playlists'][playlist_key]['unique_song_count'] = len(playlist_songs)
                            self.playlists_db['playlists'][playlist_key]['last_updated'] = datetime.now().isoformat()
                            updated_playlists.add(playlist_key)
                            print(f"   📋 Removed from playlist: {playlist_key}")
        
        # Update database totals
        self.songs_db['total_songs'] = len(self.songs_db.get('songs', {}))
        self.songs_db['last_updated'] = datetime.now().isoformat()
        
        self.mapping_db['last_updated'] = datetime.now().isoformat()
        
        self.playlists_db['last_updated'] = datetime.now().isoformat()
        
        print(f"\n📊 Removal Summary:")
        print(f"   🗑️  Files deleted: {removed_files}")
        print(f"   📚 Removed from database: {removed_from_db}")
        print(f"   📋 Updated playlists: {len(updated_playlists)}")
    
    def save_all_databases(self):
        """Save all updated databases"""
        print(f"\n💾 Saving updated databases...")
        
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
        
        if save_json_file(Config.ARTISTS_DB_FILE, self.artists_db):
            success_count += 1
            print("   ✅ Artists database saved")
        
        if success_count == 4:
            print("💾 All databases saved successfully!")
        else:
            print(f"⚠️  Only {success_count}/4 databases saved successfully")
    
    def run_cleanup(self):
        """Main cleanup process"""
        print("🧹 Song Cleanup Tool")
        print("=" * 50)
        print("This tool will help you remove songs and clean up metadata")
        print()
        
        # Scan and identify songs
        songs = self.scan_and_identify_songs()
        
        if not songs:
            print("❌ No songs found")
            return
        
        # Display summary
        self.display_songs_summary(songs)
        
        # Select songs for removal
        songs_to_remove = self.select_songs_for_removal(songs)
        
        if not songs_to_remove:
            print("✅ No songs selected for removal")
            return
        
        # Confirm removal
        print(f"\n⚠️  FINAL CONFIRMATION")
        print(f"You are about to remove {len(songs_to_remove)} songs:")
        
        for song in songs_to_remove[:10]:  # Show first 10
            if song['metadata']:
                print(f"   - {song['metadata']['track_name']} by {song['metadata']['artists_string']}")
            else:
                print(f"   - {song['filename']}")
        
        if len(songs_to_remove) > 10:
            print(f"   ... and {len(songs_to_remove) - 10} more songs")
        
        print(f"\n⚠️  This will:")
        print(f"   🗑️  Delete {len([s for s in songs_to_remove if s['file_exists']])} physical files")
        print(f"   📚 Remove {len([s for s in songs_to_remove if s['in_database']])} database entries")
        print(f"   📋 Update affected playlists")
        print(f"   💾 Create backup files before making changes")
        
        final_confirm = input(f"\nAre you absolutely sure? Type 'DELETE' to confirm: ").strip()
        
        if final_confirm == 'DELETE':
            # Perform removal
            self.remove_songs_from_databases(songs_to_remove)
            
            # Save databases
            self.save_all_databases()
            
            print(f"\n🎉 Cleanup completed successfully!")
            print(f"   📁 Check backup files if you need to restore anything")
        else:
            print("❌ Cleanup cancelled - no changes made")

def main():
    """Main function"""
    cleanup_tool = SongCleanupTool()
    cleanup_tool.run_cleanup()

if __name__ == "__main__":
    main()
