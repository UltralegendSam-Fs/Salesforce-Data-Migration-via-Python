#!/usr/bin/env python3
"""
contentversion_migration.py (fast version)

Improvements:
  - Parallel downloads/uploads with ThreadPoolExecutor
  - Dict lookup for mappings (faster than next())
  - Incremental CSV writing (resume capability)
  - Larger SOQL batch size
"""

import os
import csv
import base64
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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
CHUNK_SIZE = 400   # max safe SOQL IN() batch
API_VERSION = "v59.0"
MAX_WORKERS = 5     # parallel threads (tune based on API limits)


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def log_and_print(msg, level="info"):
    """Log and print message."""
    print(msg)
    getattr(logging, level)(msg)


def read_mapping_file():
    """Read mapping CSV into dict {ContentDocumentId: Target_Parent_Id}."""
    mapping_dict = {}
    with open(INPUT_MAPPING_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["ContentDocumentId"]:
                mapping_dict[row["ContentDocumentId"]] = row.get("Target_Parent_Id", "")
    return mapping_dict


def fetch_contentversions(sf: Salesforce, content_doc_ids, batch_size: int = 400):
    """Fetch latest ContentVersion for given ContentDocumentIds (batched)."""
    if not content_doc_ids:
        return []

    content_doc_ids = list(content_doc_ids)
    all_results = []

    for i in range(0, len(content_doc_ids), batch_size):
        batch = content_doc_ids[i: i + batch_size]
        ids_csv = ",".join(f"'{cid}'" for cid in batch)

        soql = f"""
            SELECT Id, ContentDocumentId, Title, PathOnClient
            FROM ContentVersion
            WHERE IsLatest = true
            AND ContentDocumentId IN ({ids_csv})
        """

        try:
            results = sf.query_all(soql)["records"]
            all_results.extend(results)
            log_and_print(f"[DEBUG] Batch {i//batch_size+1}: fetched {len(results)} ContentVersions")
        except Exception as e:
            log_and_print(f"[ERROR] Failed fetching ContentVersions batch {i//batch_size+1}: {e}", "error")
            continue

    log_and_print(f"[INFO] Fetched total {len(all_results)} ContentVersions")
    return all_results


def download_file_as_base64(sf: Salesforce, version_id: str) -> str:
    """Download file binary from ContentVersion and return base64 string."""
    url = f"https://{sf.sf_instance}/services/data/{API_VERSION}/sobjects/ContentVersion/{version_id}/VersionData"
    response = sf.session.get(url, headers={'Authorization': 'Bearer ' + sf.session_id}, stream=True)
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
        log_and_print(f"❌ Failed to link {new_doc_id} to {parent_id}: {e}", "error")


# --------------------------------------------------
# Worker function
# --------------------------------------------------
def migrate_one(v, target_parent_id, sf_source, sf_target):
    """Migrate a single ContentVersion record."""
    try:
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

        result = {
            "Old_ContentVersionId": v["Id"],
            "Old_ContentDocumentId": v["ContentDocumentId"],
            "New_ContentVersionId": new_ver_id,
            "New_ContentDocumentId": new_doc_id,
            "Target_Parent_Id": target_parent_id
        }
        log_and_print(f"✅ Migrated: {v['Id']} → {new_ver_id}")
        return result

    except Exception as e:
        log_and_print(f"❌ Failed to migrate ContentVersion {v['Id']}: {e}", "error")
        return None


# --------------------------------------------------
# Main migration
# --------------------------------------------------
def migrate_versions(sf_source, sf_target, mapping_dict):
    """Migrate all versions with parallel processing."""
    results = []

    # prepare CSV writer (append mode, resume capable)
    file_exists = os.path.exists(OUTPUT_VERSION_MAPPING_FILE)
    out_file = open(OUTPUT_VERSION_MAPPING_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=[
        "Old_ContentVersionId",
        "Old_ContentDocumentId",
        "New_ContentVersionId",
        "New_ContentDocumentId",
        "Target_Parent_Id"
    ])
    if not file_exists:
        writer.writeheader()

    cd_ids = list(mapping_dict.keys())
    chunks = [cd_ids[i:i+CHUNK_SIZE] for i in range(0, len(cd_ids), CHUNK_SIZE)]

    for chunk_index, cd_chunk in enumerate(chunks, start=1):
        log_and_print(f"[INFO] Processing chunk {chunk_index}/{len(chunks)} ({len(cd_chunk)} ContentDocumentIds)")
        versions = fetch_contentversions(sf_source, cd_chunk)

        # parallel uploads
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(migrate_one, v, mapping_dict.get(v["ContentDocumentId"]), sf_source, sf_target)
                for v in versions
            ]
            for f in as_completed(futures):
                result = f.result()
                if result:
                    writer.writerow(result)
                    out_file.flush()
                    results.append(result)

    out_file.close()
    return results


# --------------------------------------------------
# Entry point
# --------------------------------------------------
def main():
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    mapping_dict = read_mapping_file()
    log_and_print(f"[INFO] Found {len(mapping_dict)} mappings to migrate.")

    results = migrate_versions(sf_source, sf_target, mapping_dict)

    log_and_print(f"[DONE] Migrated {len(results)} ContentVersions.")
    log_and_print(f"[INFO] Output mapping file: {OUTPUT_VERSION_MAPPING_FILE}")


if __name__ == "__main__":
    main()
