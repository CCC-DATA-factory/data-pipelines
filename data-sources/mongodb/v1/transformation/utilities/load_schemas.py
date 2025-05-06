import time
import requests
import os
import sys
import json

# ——— CONFIGURATION ———
REGISTRY_URL = "http://localhost:8099/apis/registry/v2"
PROCESSED_GROUP_ID = "asel-schemas.processed-schemas"
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")

# Metadata fields to inject
COMMON_METADATA_FIELDS = [
    {
        "name": "first_seen_date",
        "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
        "default": None
    },
    {
        "name": "ingestion_date",
        "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
        "default": None
    },
    {
        "name": "transformation_date",
        "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
        "default": None
    },
    {
        "name": "source_system",
        "type": ["null", "string"],
        "default": None
    }
]


def read_schema_from_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def inject_metadata_fields(schema_str):
    """Inject metadata fields into schema JSON if not already present."""
    schema_json = json.loads(schema_str)

    if "fields" not in schema_json or not isinstance(schema_json["fields"], list):
        raise ValueError("Schema does not contain a valid 'fields' array")

    existing_fields = {field["name"] for field in schema_json["fields"]}
    for meta_field in COMMON_METADATA_FIELDS:
        if meta_field["name"] not in existing_fields:
            schema_json["fields"].append(meta_field)

    return json.dumps(schema_json, indent=2)


def create_or_update_schema(group_id, artifact_id, schema_str):
    """Upload schema to NiFi Registry under a specific group."""
    headers = {
        "Content-Type": "application/vnd.apache.avro+json",
        "X-Registry-ArtifactId": artifact_id
    }

    # 1) Try to create artifact
    create_url = f"{REGISTRY_URL}/groups/{group_id}/artifacts"
    resp = requests.post(create_url, data=schema_str, headers=headers)
    if resp.status_code in (200, 201):
        print(f"✅ Created artifact {artifact_id} in group {group_id}")
        return

    # 2) If already exists, append new version
    if resp.status_code == 409:
        version_url = f"{REGISTRY_URL}/groups/{group_id}/artifacts/{artifact_id}/versions"
        resp2 = requests.post(version_url, data=schema_str, headers=headers)
        if resp2.status_code in (200, 201):
            print(f"✅ Appended new version for {artifact_id} in group {group_id}")
        else:
            print(f"❌ Error adding version for {artifact_id} in group {group_id}: {resp2.status_code} {resp2.text}")
            resp2.raise_for_status()
        return

    # 3) Other errors
    print(f"❌ Error creating artifact {artifact_id} in group {group_id}: {resp.status_code} {resp.text}")
    resp.raise_for_status()


def main():
    if not os.path.isdir(SCHEMA_DIR):
        sys.exit(f"❌ Schema directory not found: {SCHEMA_DIR}")


    for fname in os.listdir(SCHEMA_DIR):
        if not fname.lower().endswith(".avsc"):
            continue

        artifact_id = f"{os.path.splitext(fname)[0]}"
        file_path = os.path.join(SCHEMA_DIR, fname)

        try:
            # Step 1: Upload RAW schema (unmodified)
            processed_schema = read_schema_from_file(file_path)
            # Step 2: Upload STAGING schema (with metadata)
            metadataProcessedSchema = inject_metadata_fields(processed_schema)
            create_or_update_schema(PROCESSED_GROUP_ID, artifact_id, metadataProcessedSchema)
            time.sleep(1)

        except Exception as e:
            print(f"❌ Failed for {artifact_id}: {e}")


if __name__ == "__main__":
    main()
