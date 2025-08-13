import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

# ✅ Repo & Branch Details
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = "faizalmsdev"
REPO = "ogplayer"
BRANCH = "main"

# ✅ Local & Remote Metadata Paths
LOCAL_METADATA_FOLDER = os.path.join("consolidated_music", "metadata")
REMOTE_METADATA_FOLDER = "public/consolidated_music/metadata"

# ✅ GitHub API headers
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ✅ Get SHA if file already exists in remote repo
def get_existing_file_sha(remote_filename):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{REMOTE_METADATA_FOLDER}/{remote_filename}?ref={BRANCH}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()["sha"]
    return None

# ✅ Upload or update a single JSON file
def upload_metadata_file(local_path, remote_filename):
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    sha = get_existing_file_sha(remote_filename)

    payload = {
        "message": f"{'Update' if sha else 'Add'} metadata file {remote_filename}",
        "content": content,
        "branch": BRANCH
    }

    if sha:
        payload["sha"] = sha

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{REMOTE_METADATA_FOLDER}/{remote_filename}"
    response = requests.put(url, headers=headers, json=payload)

    if response.status_code in [200, 201]:
        print(f"✅ {'Updated' if sha else 'Uploaded'}: {remote_filename}")
    else:
        print(f"❌ Failed: {remote_filename} | {response.status_code} | {response.json()}")

# ✅ Loop through metadata folder and push each .json file
def main():
    if not os.path.exists(LOCAL_METADATA_FOLDER):
        print("❌ Folder not found:", LOCAL_METADATA_FOLDER)
        return

    for file in os.listdir(LOCAL_METADATA_FOLDER):
        if file.endswith(".json"):
            local_file_path = os.path.join(LOCAL_METADATA_FOLDER, file)
            if os.path.isfile(local_file_path):
                upload_metadata_file(local_file_path, file)

if __name__ == "__main__":
    main()
