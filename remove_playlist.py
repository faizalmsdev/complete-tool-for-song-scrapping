import json
import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional

class PlaylistRemover:
    def __init__(self, consolidated_folder: str = "consolidated_music"):
        self.consolidated_folder = Path(consolidated_folder)
        self.songs_folder = self.consolidated_folder / "songs"
        self.metadata_folder = self.consolidated_folder / "metadata"
        
        # Database paths
        self.songs_db_path = self.metadata_folder / 'songs_database.json'
        self.playlists_db_path = self.metadata_folder / 'playlists_database.json'
        self.artists_db_path = self.metadata_folder / 'artists_database.json'
        self.mapping_db_path = self.metadata_folder / 'song_playlist_mapping.json'
        
        # Data containers
        self.songs_db = {}
        self.playlists_db = {}
        self.artists_db = {}
        self.mapping_db = {}
        
        # Load existing databases
        self.load_databases()
    
    def load_databases(self):
        """Load all existing databases"""
        try:
            # Load songs database
            if self.songs_db_path.exists():
                with open(self.songs_db_path, 'r', encoding='utf-8') as f:
                    self.songs_db = json.load(f)
                print(f"📚 Loaded songs database: {len(self.songs_db.get('songs', {}))} songs")
            else:
                print("❌ Songs database not found")
                return False
            
            # Load playlists database
            if self.playlists_db_path.exists():
                with open(self.playlists_db_path, 'r', encoding='utf-8') as f:
                    self.playlists_db = json.load(f)
                print(f"📚 Loaded playlists database: {len(self.playlists_db.get('playlists', {}))} playlists")
            else:
                print("❌ Playlists database not found")
                return False
            
            # Load artists database
            if self.artists_db_path.exists():
                with open(self.artists_db_path, 'r', encoding='utf-8') as f:
                    self.artists_db = json.load(f)
                print(f"📚 Loaded artists database: {len(self.artists_db.get('artists', {}))} artists")
            
            # Load mapping database
            if self.mapping_db_path.exists():
                with open(self.mapping_db_path, 'r', encoding='utf-8') as f:
                    self.mapping_db = json.load(f)
                print(f"📚 Loaded mapping database")
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading databases: {e}")
            return False
    
    def save_databases(self):
        """Save all databases back to files"""
        try:
            # Update timestamps
            current_time = datetime.now().isoformat()
            
            # Save songs database
            self.songs_db['last_updated'] = current_time
            self.songs_db['total_songs'] = len(self.songs_db.get('songs', {}))
            with open(self.songs_db_path, 'w', encoding='utf-8') as f:
                json.dump(self.songs_db, f, indent=2, ensure_ascii=False)
            
            # Save playlists database
            self.playlists_db['last_updated'] = current_time
            self.playlists_db['total_playlists'] = len(self.playlists_db.get('playlists', {}))
            with open(self.playlists_db_path, 'w', encoding='utf-8') as f:
                json.dump(self.playlists_db, f, indent=2, ensure_ascii=False)
            
            # Save artists database
            if self.artists_db:
                self.artists_db['last_updated'] = current_time
                self.artists_db['total_artists'] = len(self.artists_db.get('artists', {}))
                with open(self.artists_db_path, 'w', encoding='utf-8') as f:
                    json.dump(self.artists_db, f, indent=2, ensure_ascii=False)
            
            # Save mapping database
            if self.mapping_db:
                self.mapping_db['last_updated'] = current_time
                with open(self.mapping_db_path, 'w', encoding='utf-8') as f:
                    json.dump(self.mapping_db, f, indent=2, ensure_ascii=False)
            
            print("💾 All databases saved successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error saving databases: {e}")
            return False
    
    def find_playlist_by_name(self, playlist_name: str) -> Optional[str]:
        """Find playlist ID by name (case-insensitive partial match)"""
        playlists = self.playlists_db.get('playlists', {})
        matches = []
        
        for playlist_id, playlist_info in playlists.items():
            stored_name = playlist_info.get('name', '').lower()
            search_name = playlist_name.lower()
            
            if search_name in stored_name or stored_name in search_name:
                matches.append((playlist_id, playlist_info.get('name', 'Unknown')))
        
        if not matches:
            return None
        elif len(matches) == 1:
            return matches[0][0]
        else:
            # Multiple matches found
            print(f"\n🔍 Multiple playlists found matching '{playlist_name}':")
            for i, (playlist_id, name) in enumerate(matches, 1):
                print(f"   {i}. {name} (ID: {playlist_id})")
            
            while True:
                try:
                    choice = input(f"\nSelect playlist (1-{len(matches)}) or 'q' to quit: ").strip()
                    if choice.lower() == 'q':
                        return None
                    
                    choice_num = int(choice)
                    if 1 <= choice_num <= len(matches):
                        return matches[choice_num - 1][0]
                    else:
                        print(f"❌ Please enter a number between 1 and {len(matches)}")
                except ValueError:
                    print("❌ Please enter a valid number or 'q'")
    
    def get_songs_in_playlist(self, playlist_id: str) -> List[str]:
        """Get list of song IDs in the playlist"""
        playlist_info = self.playlists_db.get('playlists', {}).get(playlist_id, {})
        return playlist_info.get('song_ids', [])
    
    def remove_playlist_from_songs(self, playlist_id: str, song_ids: List[str]) -> Dict[str, str]:
        """
        Remove playlist ID from songs and determine which songs should be deleted
        Returns dict: song_id -> action ('remove' or 'keep')
        """
        songs = self.songs_db.get('songs', {})
        song_actions = {}
        
        for song_id in song_ids:
            if song_id in songs:
                song_playlists = songs[song_id].get('playlists', [])
                
                # Remove this playlist from the song's playlist list
                if playlist_id in song_playlists:
                    song_playlists.remove(playlist_id)
                    songs[song_id]['playlists'] = song_playlists
                
                # Determine action based on remaining playlists
                if not song_playlists:  # No more playlists
                    song_actions[song_id] = 'remove'
                else:
                    song_actions[song_id] = 'keep'
            else:
                print(f"⚠️  Warning: Song {song_id} not found in database")
        
        return song_actions
    
    def remove_songs_from_filesystem(self, songs_to_remove: List[str]) -> int:
        """Remove song files from the filesystem"""
        removed_count = 0
        
        for song_id in songs_to_remove:
            # Find and remove the actual audio file
            for ext in ['.mp3', '.m4a', '.flac', '.wav', '.ogg']:
                song_file = self.songs_folder / f"{song_id}{ext}"
                if song_file.exists():
                    try:
                        song_file.unlink()
                        removed_count += 1
                        print(f"   🗑️  Deleted: {song_file.name}")
                        break
                    except Exception as e:
                        print(f"   ❌ Failed to delete {song_file.name}: {e}")
        
        return removed_count
    
    def clean_artists_database(self, playlist_id: str):
        """Remove playlist ID from artists database"""
        if not self.artists_db:
            return
        
        artists = self.artists_db.get('artists', {})
        artists_to_remove = []
        
        for artist_uri, artist_info in artists.items():
            playlist_ids = artist_info.get('playlist_ids', [])
            
            if playlist_id in playlist_ids:
                playlist_ids.remove(playlist_id)
                artist_info['playlist_ids'] = playlist_ids
                artist_info['last_updated'] = datetime.now().isoformat()
                
                # If artist has no more playlists, mark for removal
                if not playlist_ids:
                    artists_to_remove.append(artist_uri)
        
        # Remove artists with no playlists
        for artist_uri in artists_to_remove:
            del artists[artist_uri]
            print(f"   🎤 Removed artist: {artist_uri}")
    
    def update_mapping_database(self, songs_to_remove: List[str]):
        """Update song-playlist mapping database"""
        if not self.mapping_db:
            return
        
        mapping = self.mapping_db.get('mapping', {})
        
        # Remove songs that are being deleted
        for song_id in songs_to_remove:
            if song_id in mapping:
                del mapping[song_id]
        
        self.mapping_db['mapping'] = mapping
    
    def remove_playlist(self, playlist_name: str, confirm: bool = True) -> bool:
        """
        Remove a playlist and handle associated songs
        
        Args:
            playlist_name: Name of the playlist to remove
            confirm: Whether to ask for confirmation before removal
        
        Returns:
            True if successful, False otherwise
        """
        # Find playlist
        playlist_id = self.find_playlist_by_name(playlist_name)
        if not playlist_id:
            print(f"❌ Playlist '{playlist_name}' not found")
            return False
        
        playlist_info = self.playlists_db.get('playlists', {}).get(playlist_id, {})
        actual_name = playlist_info.get('name', 'Unknown')
        song_ids = self.get_songs_in_playlist(playlist_id)
        
        print(f"\n🎵 Found playlist: {actual_name}")
        print(f"📊 Playlist contains {len(song_ids)} songs")
        
        if not song_ids:
            print("📭 Playlist is empty")
        
        # Analyze song removal impact
        song_actions = self.remove_playlist_from_songs(playlist_id, song_ids)
        songs_to_remove = [sid for sid, action in song_actions.items() if action == 'remove']
        songs_to_keep = [sid for sid, action in song_actions.items() if action == 'keep']
        
        print(f"\n📋 Impact Analysis:")
        print(f"   🗑️  Songs to be deleted (not in other playlists): {len(songs_to_remove)}")
        print(f"   💾 Songs to be kept (in other playlists): {len(songs_to_keep)}")
        
        if songs_to_remove:
            print(f"\n🎵 Songs that will be completely removed:")
            songs_db = self.songs_db.get('songs', {})
            for song_id in songs_to_remove[:10]:  # Show first 10
                if song_id in songs_db:
                    metadata = songs_db[song_id].get('metadata', {})
                    track_name = metadata.get('track_name', 'Unknown')
                    artists = metadata.get('artists_string', 'Unknown Artist')
                    print(f"   • {track_name} by {artists}")
            
            if len(songs_to_remove) > 10:
                print(f"   ... and {len(songs_to_remove) - 10} more songs")
        
        # Confirmation
        if confirm:
            print(f"\n⚠️  This will:")
            print(f"   • Remove playlist '{actual_name}'")
            print(f"   • Delete {len(songs_to_remove)} song files from disk")
            print(f"   • Keep {len(songs_to_keep)} songs (used in other playlists)")
            
            response = input(f"\nAre you sure you want to proceed? (yes/no): ").strip().lower()
            if response not in ['yes', 'y']:
                print("❌ Operation cancelled")
                return False
        
        # Perform removal
        print(f"\n🗑️  Removing playlist '{actual_name}'...")
        
        try:
            # Remove playlist from playlists database
            playlists = self.playlists_db.get('playlists', {})
            if playlist_id in playlists:
                del playlists[playlist_id]
                print(f"   ✅ Removed playlist from database")
            
            # Remove songs from songs database and filesystem
            songs_db = self.songs_db.get('songs', {})
            for song_id in songs_to_remove:
                if song_id in songs_db:
                    del songs_db[song_id]
            
            if songs_to_remove:
                removed_files = self.remove_songs_from_filesystem(songs_to_remove)
                print(f"   ✅ Removed {removed_files} song files from disk")
                print(f"   ✅ Removed {len(songs_to_remove)} songs from database")
            
            # Clean artists database
            self.clean_artists_database(playlist_id)
            
            # Update mapping database
            self.update_mapping_database(songs_to_remove)
            
            # Save all databases
            if self.save_databases():
                print(f"\n✅ Successfully removed playlist '{actual_name}'")
                print(f"📊 Final Summary:")
                print(f"   🗑️  Playlist removed: 1")
                print(f"   🗑️  Songs deleted: {len(songs_to_remove)}")
                print(f"   💾 Songs preserved: {len(songs_to_keep)}")
                return True
            else:
                print(f"❌ Error saving databases")
                return False
                
        except Exception as e:
            print(f"❌ Error during removal: {e}")
            return False
    
    def list_all_playlists(self):
        """List all available playlists"""
        playlists = self.playlists_db.get('playlists', {})
        
        if not playlists:
            print("📭 No playlists found")
            return
        
        print(f"\n📋 Available Playlists ({len(playlists)}):")
        print("=" * 60)
        
        for i, (playlist_id, playlist_info) in enumerate(playlists.items(), 1):
            name = playlist_info.get('name', 'Unknown')
            song_count = len(playlist_info.get('song_ids', []))
            created_at = playlist_info.get('created_at', 'Unknown')
            
            # Extract just the date part
            try:
                date_only = created_at.split('T')[0] if 'T' in created_at else created_at
            except:
                date_only = created_at
            
            print(f"{i:2d}. {name}")
            print(f"    📊 Songs: {song_count} | 📅 Created: {date_only}")
            print(f"    🆔 ID: {playlist_id}")
            print()

def main():
    """Main function to run the playlist remover"""
    print("🗑️  Playlist Remover Tool")
    print("=" * 50)
    print("This tool will remove a playlist and its associated songs from your music database.")
    print("Songs that exist in other playlists will be preserved.")
    print()
    
    # Initialize remover
    remover = PlaylistRemover()
    
    # Check if databases were loaded successfully
    if not remover.songs_db or not remover.playlists_db:
        print("❌ Required databases not found. Make sure you have run the scraper first.")
        return
    
    while True:
        print("\n🎵 What would you like to do?")
        print("1. List all playlists")
        print("2. Remove a playlist")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == '1':
            remover.list_all_playlists()
        
        elif choice == '2':
            playlist_name = input("\nEnter playlist name (or partial name): ").strip()
            
            if not playlist_name:
                print("❌ Please enter a playlist name")
                continue
            
            remover.remove_playlist(playlist_name)
        
        elif choice == '3':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    main()
