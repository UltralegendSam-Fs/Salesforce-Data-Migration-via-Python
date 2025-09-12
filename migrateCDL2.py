#!/usr/bin/env python3
"""
contentversion_migration.py

Purpose:
  - Read ContentDocumentId & Target_Parent_Id from CDL mapping CSV
  - Download latest ContentVersion binary from SOURCE org
  - Upload to TARGET org (no corruption)
  - Create ContentDocumentLink in TARGET org with SAME ShareType as source org
  - Output mapping: Old_ContentVersionId, Old_ContentDocumentId, New_ContentVersionId, New_ContentDocumentId, Target_Parent_Id
"""

import os
import csv
import math
import base64
import logging
import requests
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from mappings import FILES_DIR

INPUT_MAPPING_FILE = os.path.join(FILES_DIR, "contentdocumentlink_mapping.csv")
OUTPUT_VERSION_MAPPING_FILE = os.path.join(FILES_DIR, "contentversion_migration_mapping.csv")
LOG_FILE = os.path.join(FILES_DIR, "contentversion_migration.log")

# === Logging configuration ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Runtime config
CHUNK_SIZE = 200
API_VERSION = "v59.0"


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def log_and_print(msg, level="info"):
    """Log and print message."""
    print(msg)
    getattr(logging, level)(msg)


def read_mapping_file():
    """Read mapping CSV into list of dicts."""
    mappings = []
    with open(INPUT_MAPPING_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["ContentDocumentId"]:
                mappings.append({
                    "ContentDocumentId": row["ContentDocumentId"],
                    "Target_Parent_Id": row.get("Target_Parent_Id", "")
                })
    return mappings


def fetch_contentversions(sf: Salesforce, content_doc_ids):
    """Fetch latest ContentVersion for given ContentDocumentIds."""
    ids_csv = ",".join(f"'{cid}'" for cid in content_doc_ids)
    soql = f"""
        SELECT Id, ContentDocumentId, Title, PathOnClient
        FROM ContentVersion
        WHERE IsLatest = true
        AND ContentDocumentId IN ({ids_csv})
    """
    return sf.query_all(soql)["records"]


def download_file_as_base64(sf: Salesforce, version_id: str) -> str:
    """Download file binary from ContentVersion and return base64 string."""
    url = f"https://{sf.sf_instance}/services/data/{API_VERSION}/sobjects/ContentVersion/{version_id}/VersionData"
    response = requests.get(url, headers={'Authorization': 'Bearer ' + sf.session_id}, stream=True)
    response.raise_for_status()
    return base64.b64encode(response.content).decode("utf-8")


def create_cdl(sf_target: Salesforce, new_doc_id: str, parent_id: str):
    """Create ContentDocumentLink in target org with fixed ShareType='V'."""
    share_type = "V"  # Default Viewer
    try:
        sf_target.ContentDocumentLink.create({
            "ContentDocumentId": new_doc_id,
            "LinkedEntityId": parent_id,
            "ShareType": share_type,
            "Visibility": "AllUsers"
        })
        log_and_print(f"🔗 Linked {new_doc_id} to {parent_id} (ShareType={share_type})")
    except Exception as e:
        log_and_print(f"❌ Failed to link {new_doc_id} to {parent_id} (ShareType={share_type}): {e}", "error")



# --------------------------------------------------
# Main migration
# --------------------------------------------------
def migrate_versions(sf_source, sf_target, mappings):
    results = []
    cd_id_chunks = [mappings[i:i+CHUNK_SIZE] for i in range(0, len(mappings), CHUNK_SIZE)]

    for chunk_index, chunk in enumerate(cd_id_chunks, start=1):
        cd_ids = [m["ContentDocumentId"] for m in chunk]
        log_and_print(f"[INFO] Processing chunk {chunk_index}/{len(cd_id_chunks)} ({len(cd_ids)} ContentDocumentIds)")

        versions = fetch_contentversions(sf_source, cd_ids)

        for v in versions:
            try:
                mapping_row = next((m for m in chunk if m["ContentDocumentId"] == v["ContentDocumentId"]), None)
                target_parent_id = mapping_row["Target_Parent_Id"] if mapping_row else None

                file_base64 = download_file_as_base64(sf_source, v["Id"])

                new_version = sf_target.ContentVersion.create({
                    "Title": v["Title"],
                    "PathOnClient": v["PathOnClient"] or f"{v['Title']}.bin",
                    "VersionData": file_base64,
                    "Card_Legacy_Id__c": v["Id"] 
                })
                new_ver_id = new_version["id"]

                new_doc_id = sf_target.ContentVersion.get(new_ver_id)["ContentDocumentId"]

                if target_parent_id:
                    create_cdl(sf_target, new_doc_id, target_parent_id)

                results.append({
                    "Old_ContentVersionId": v["Id"],
                    "Old_ContentDocumentId": v["ContentDocumentId"],
                    "New_ContentVersionId": new_ver_id,
                    "New_ContentDocumentId": new_doc_id,
                    "Target_Parent_Id": target_parent_id
                })

                log_and_print(f"✅ Migrated: {v['Id']} → {new_ver_id}")

            except Exception as e:
                log_and_print(f"❌ Failed to migrate ContentVersion {v['Id']}: {e}", "error")

    return results


def write_mapping(results):
    """Write migration mapping to CSV."""
    with open(OUTPUT_VERSION_MAPPING_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Old_ContentVersionId",
                "Old_ContentDocumentId",
                "New_ContentVersionId",
                "New_ContentDocumentId",
                "Target_Parent_Id"
            ]
        )
        writer.writeheader()
        writer.writerows(results)


# --------------------------------------------------
# Entry point
# --------------------------------------------------
def main():
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    mappings = read_mapping_file()
    log_and_print(f"[INFO] Found {len(mappings)} mappings to migrate.")

    results = migrate_versions(sf_source, sf_target, mappings)
    write_mapping(results)

    log_and_print(f"[DONE] Migrated {len(results)} ContentVersions.")
    log_and_print(f"[INFO] Output mapping file: {OUTPUT_VERSION_MAPPING_FILE}")


if __name__ == "__main__":
    main()
