import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

# ✅ GitHub Configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = "faizalmsdev"
REPO = "audio-player"
BRANCH = "master"

# ✅ Local and Remote Paths
LOCAL_SONGS_FOLDER = os.path.join("consolidated_music", "songs")
REMOTE_SONGS_FOLDER = "public/consolidated_music/songs"

# ✅ GitHub API headers
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ✅ Get SHA for existing file (for update support)
def get_existing_file_sha(remote_filename):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{REMOTE_SONGS_FOLDER}/{remote_filename}?ref={BRANCH}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("sha")
    return None

# ✅ Upload or update file to GitHub
def upload_file(local_path, remote_filename):
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    sha = get_existing_file_sha(remote_filename)

    payload = {
        "message": f"{'Update' if sha else 'Add'} {remote_filename}",
        "content": content,
        "branch": BRANCH
    }

    if sha:
        payload["sha"] = sha

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{REMOTE_SONGS_FOLDER}/{remote_filename}"
    response = requests.put(url, headers=headers, json=payload)

    if response.status_code in [200, 201]:
        print(f"✅ {'Updated' if sha else 'Uploaded'}: {remote_filename}")
        try:
            os.remove(local_path)
            print(f"🗑️  Deleted local file: {local_path}")
        except Exception as e:
            print(f"❌ Could not delete {local_path}: {e}")
    else:
        try:
            error_detail = response.json()
        except ValueError:
            error_detail = response.text
        print(f"❌ Failed: {remote_filename} | {response.status_code} | {error_detail}")

# ✅ Main function
def main():
    if not os.path.exists(LOCAL_SONGS_FOLDER):
        print("❌ Folder not found:", LOCAL_SONGS_FOLDER)
        return

    for file in os.listdir(LOCAL_SONGS_FOLDER):
        local_file_path = os.path.join(LOCAL_SONGS_FOLDER, file)
        if os.path.isfile(local_file_path):
            upload_file(local_file_path, file)

if __name__ == "__main__":
    main()
