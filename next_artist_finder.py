#!/usr/bin/env python3
"""
Next Artist ID Finder
Reads the last name from artists scrapper.txt, finds it in artists_database.json,
and returns the next artist ID(s) in the specified format.
"""

import json
import os
from pathlib import Path

def read_artists_scrapper():
    """Read the artists scrapper.txt file and return the last artist name"""
    try:
        with open('artists scrapper.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # Get the last non-empty line and strip whitespace
            for line in reversed(lines):
                line = line.strip()
                if line:
                    return line
        return None
    except FileNotFoundError:
        print("❌ Error: 'artists scrapper.txt' file not found!")
        return None
    except Exception as e:
        print(f"❌ Error reading artists scrapper.txt: {e}")
        return None

def load_artists_database():
    """Load the artists database JSON file"""
    try:
        db_path = Path('consolidated_music/metadata/artists_database.json')
        with open(db_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('artists', {})
    except FileNotFoundError:
        print("❌ Error: artists_database.json file not found!")
        return None
    except Exception as e:
        print(f"❌ Error loading artists database: {e}")
        return None

def find_artist_by_name(artists_db, artist_name):
    """Find artist entry by name in the database"""
    for uri, artist_data in artists_db.items():
        if artist_data.get('name', '').strip() == artist_name.strip():
            return uri, artist_data
    return None, None

def get_next_artists(artists_db, start_uri, count):
    """Get the next 'count' artists after the specified URI"""
    artists_list = list(artists_db.items())
    
    # Find the index of the start artist
    start_index = -1
    for i, (uri, _) in enumerate(artists_list):
        if uri == start_uri:
            start_index = i
            break
    
    if start_index == -1:
        return []
    
    # Get the next 'count' artists
    next_artists = []
    for i in range(start_index + 1, min(start_index + 1 + count, len(artists_list))):
        uri, artist_data = artists_list[i]
        # Extract just the ID part from the URI (after the last colon)
        artist_id = uri.split(':')[-1]
        next_artists.append((artist_id, artist_data.get('name', 'Unknown')))
    
    return next_artists

def find_artist_by_id(artists_db, artist_id):
    """Find artist entry by ID in the database"""
    test_uri = f"spotify:artist:{artist_id}"
    if test_uri in artists_db:
        return test_uri, artists_db[test_uri]
    return None, None

def add_names_to_file(artist_names):
    """Add artist names to the artists scrapper.txt file"""
    try:
        # Read existing content
        existing_names = []
        try:
            with open('artists scrapper.txt', 'r', encoding='utf-8') as f:
                existing_names = [line.strip() for line in f.readlines() if line.strip()]
        except FileNotFoundError:
            print("� Creating new 'artists scrapper.txt' file...")
        
        # Add new names (avoid duplicates)
        added_count = 0
        for name in artist_names:
            if name not in existing_names:
                existing_names.append(name)
                added_count += 1
        
        # Write back to file
        with open('artists scrapper.txt', 'w', encoding='utf-8') as f:
            for name in existing_names:
                f.write(f"{name}\n")
        
        print(f"✅ Added {added_count} new artist(s) to 'artists scrapper.txt'")
        return True
    except Exception as e:
        print(f"❌ Error updating file: {e}")
        return False

def mode_ids_to_names(artists_db):
    """Mode 2: Convert artist IDs to names and add to file"""
    print("\n🆔 Paste Artist IDs Mode")
    print("=" * 50)
    print("📝 Paste your comma-separated artist IDs:")
    print("   Example: 2y2DSOmE3xKWW4Wia2ucCi, 1plObTufEAfeL1hk8Qz24v")
    
    user_input = input("\nPaste IDs here: ").strip()
    
    if not user_input:
        print("❌ No input provided.")
        return
    
    # Parse the IDs
    raw_ids = [id_str.strip() for id_str in user_input.split(',')]
    
    found_artists = []
    not_found_ids = []
    
    print(f"\n� Looking up {len(raw_ids)} artist ID(s)...")
    
    for artist_id in raw_ids:
        # Clean up the ID (remove any extra spaces or characters)
        clean_id = artist_id.strip()
        
        if len(clean_id) == 22 and clean_id.replace('_', '').replace('-', '').isalnum():
            found_uri, found_data = find_artist_by_id(artists_db, clean_id)
            
            if found_uri:
                artist_name = found_data.get('name', 'Unknown')
                found_artists.append((clean_id, artist_name))
                print(f"   ✅ {clean_id} -> {artist_name}")
            else:
                not_found_ids.append(clean_id)
                print(f"   ❌ {clean_id} -> Not found in database")
        else:
            not_found_ids.append(clean_id)
            print(f"   ❌ {clean_id} -> Invalid ID format")
    
    if found_artists:
        print(f"\n📊 Summary:")
        print(f"   ✅ Found: {len(found_artists)} artists")
        print(f"   ❌ Not found: {len(not_found_ids)} IDs")
        
        # Ask if user wants to add to file
        response = input(f"\n📝 Add these {len(found_artists)} artist names to 'artists scrapper.txt'? (y/N): ").strip().lower()
        
        if response in ['y', 'yes']:
            artist_names = [name for _, name in found_artists]
            add_names_to_file(artist_names)
        else:
            print("ℹ️ Artist names not added to file.")
            
        # Display the names for reference
        print(f"\n📋 Artist Names Found:")
        print("-" * 30)
        for _, name in found_artists:
            print(f"• {name}")
    else:
        print("\n❌ No valid artists found.")

def mode_next_artists(artists_db):
    """Mode 1: Find next artists from last in file"""
    print("\n🔍 Next Artist ID Finder Mode")
    print("=" * 50)
    
    # Option 1: Try to read from artists scrapper.txt
    print("📄 Reading last artist from 'artists scrapper.txt'...")
    last_artist_name = read_artists_scrapper()
    
    start_uri = None
    start_name = None
    
    if last_artist_name:
        print(f"📝 Last artist in file: '{last_artist_name}'")
        
        # Find this artist in the database
        found_uri, found_data = find_artist_by_name(artists_db, last_artist_name)
        
        if found_uri:
            start_uri = found_uri
            start_name = found_data.get('name')
            print(f"✅ Found artist in database: {start_name}")
        else:
            print(f"❌ Artist '{last_artist_name}' not found in database")
    else:
        print("❌ Could not read last artist from file")
    
    # Option 2: If automatic lookup failed, ask user
    if not start_uri:
        print("\n🤔 Please provide the last rendered artist name or ID:")
        user_input = input("Enter artist name or Spotify ID: ").strip()
        
        if not user_input:
            print("❌ No input provided. Exiting.")
            return
        
        # Check if it's a Spotify ID (contains alphanumeric characters)
        if len(user_input) == 22 and user_input.isalnum():
            # It's likely a Spotify ID
            test_uri = f"spotify:artist:{user_input}"
            if test_uri in artists_db:
                start_uri = test_uri
                start_name = artists_db[test_uri].get('name')
                print(f"✅ Found artist by ID: {start_name}")
            else:
                print(f"❌ Artist ID '{user_input}' not found in database")
                return
        else:
            # It's a name, search for it
            found_uri, found_data = find_artist_by_name(artists_db, user_input)
            if found_uri:
                start_uri = found_uri
                start_name = found_data.get('name')
                print(f"✅ Found artist by name: {start_name}")
            else:
                print(f"❌ Artist '{user_input}' not found in database")
                return
    
    # Ask for count
    print(f"\n📊 Starting from: {start_name}")
    try:
        count = int(input("How many next artist IDs do you need? "))
        if count <= 0:
            print("❌ Count must be a positive number")
            return
    except ValueError:
        print("❌ Invalid number entered")
        return
    
    # Get next artists
    print(f"\n🔍 Finding next {count} artist(s)...")
    next_artists = get_next_artists(artists_db, start_uri, count)
    
    if not next_artists:
        print("❌ No more artists found after the specified artist")
        return
    
    # Display results
    print(f"\n✅ Found {len(next_artists)} next artist(s):")
    print("-" * 50)
    
    # Create the comma-separated list of IDs
    artist_ids = []
    artist_names = []
    for i, (artist_id, artist_name) in enumerate(next_artists, 1):
        print(f"{i}. {artist_name} -> {artist_id}")
        artist_ids.append(artist_id)
        artist_names.append(artist_name)
    
    # Output the final result
    result = ", ".join(artist_ids)
    print(f"\n🎯 Result (copy this):")
    print("=" * 50)
    print(result)
    print("=" * 50)
    
    # Ask if user wants to add names to file
    response = input(f"\n📝 Add these {len(artist_names)} artist names to 'artists scrapper.txt'? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        add_names_to_file(artist_names)

def main():
    print("🎵 Artist ID & Name Manager")
    print("=" * 50)
    
    # Load artists database
    print("📚 Loading artists database...")
    artists_db = load_artists_database()
    if not artists_db:
        return
    
    print(f"✅ Loaded {len(artists_db)} artists from database")
    
    # Choose mode
    print("\n🔧 Choose Mode:")
    print("1. Find next artist IDs (from last in txt file)")
    print("2. Convert artist IDs to names (and add to txt file)")
    
    try:
        choice = input("\nEnter choice (1 or 2): ").strip()
        
        if choice == "1":
            mode_next_artists(artists_db)
        elif choice == "2":
            mode_ids_to_names(artists_db)
        else:
            print("❌ Invalid choice. Please enter 1 or 2.")
            return
            
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        return

if __name__ == "__main__":
    main()
