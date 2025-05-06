import requests

# === CONFIGURATION ===
REGISTRY_URL = "http://localhost:8099"  # Change this to your Apicurio host
GROUP_ID = "asel-schemas.ingestion-schemas"                
# === API ENDPOINTS ===
ARTIFACTS_URL = f"{REGISTRY_URL}/apis/registry/v2/groups/{GROUP_ID}/artifacts"

# === Fetch all artifact IDs ===
response = requests.get(ARTIFACTS_URL)
if response.status_code != 200:
    print(f"Failed to fetch artifacts: {response.status_code} - {response.text}")
    exit(1)

artifact_list = response.json()
if not artifact_list:
    print("No artifacts found.")
    exit(0)

# === Delete each artifact ===
for artifact in artifact_list["artifacts"]:
    print(artifact)
    artifact_id = artifact['id']
    delete_url = f"{ARTIFACTS_URL}/{artifact_id}"
    del_response = requests.delete(delete_url)
    if del_response.status_code == 204:
        print(f"✅ Deleted: {artifact_id}")
    else:
        print(f"❌ Failed to delete {artifact_id}: {del_response.status_code} - {del_response.text}")
