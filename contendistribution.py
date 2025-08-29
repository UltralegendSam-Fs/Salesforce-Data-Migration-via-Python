#!/usr/bin/env python3
"""
contentdistribution_migration.py

Purpose:
  - Read Old & New ContentDocumentIds and ContentVersionIds from mapping CSV
  - Fetch ContentDistribution records from SOURCE org
  - Recreate them in TARGET org with New IDs
  - Output mapping: Old_ContentDistributionId, Old_ContentDocumentId, New_ContentDistributionId, New_ContentDocumentId
"""

import os
import csv
import math
import logging
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET

# === Files & Directories ===
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)

INPUT_MAPPING_FILE = os.path.join(FILES_DIR, "contentversion_migration_mapping.csv")  # from ContentVersion migration
OUTPUT_DISTRIBUTION_MAPPING_FILE = os.path.join(FILES_DIR, "contentdistribution_migration_mapping.csv")
LOG_FILE = os.path.join(FILES_DIR, "contentdistribution_migration.log")

# === Logging configuration ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Runtime config
CHUNK_SIZE = 200  # batch size

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def read_doc_ver_mapping():
    """
    Read mapping CSV and return dict:
    { Old_ContentDocumentId: {"new_doc": New_ContentDocumentId, "new_ver": New_ContentVersionId} }
    """
    mapping = {}
    with open(INPUT_MAPPING_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            old_doc = row["Old_ContentDocumentId"]
            if old_doc:
                mapping[old_doc] = {
                    "new_doc": row["New_ContentDocumentId"],
                    "new_ver": row["New_ContentVersionId"]
                }
    logging.info(f"Loaded {len(mapping)} document mappings from {INPUT_MAPPING_FILE}")
    return mapping

def fetch_contentdistributions(sf: Salesforce, content_doc_ids):
    """
    Fetch ContentDistribution records for given ContentDocumentIds.
    """
    ids_csv = ",".join(f"'{cid}'" for cid in content_doc_ids)
    soql = f"""
        SELECT Id, Name, ContentVersionId, ContentDocumentId, RelatedRecordId,
               PreferencesAllowPDFDownload, PreferencesAllowOriginalDownload,
               PreferencesPasswordRequired, PreferencesNotifyOnVisit,
               PreferencesLinkLatestVersion, PreferencesAllowViewInBrowser,
               PreferencesExpires, PreferencesNotifyRndtnComplete, ExpiryDate
        FROM ContentDistribution
        WHERE ContentDocumentId IN ({ids_csv})
    """
    records = sf.query_all(soql)["records"]
    logging.info(f"Fetched {len(records)} ContentDistribution records for {len(content_doc_ids)} ContentDocumentIds")
    return records

# --------------------------------------------------
# Main migration
# --------------------------------------------------
def migrate_distributions(sf_source, sf_target, doc_mapping):
    results = []
    old_ids = list(doc_mapping.keys())
    total_chunks = math.ceil(len(old_ids) / CHUNK_SIZE)

    for i in range(0, len(old_ids), CHUNK_SIZE):
        chunk = old_ids[i:i+CHUNK_SIZE]
        logging.info(f"Processing chunk {i//CHUNK_SIZE+1}/{total_chunks} with {len(chunk)} ContentDocumentIds")
        print(f"[INFO] Processing chunk {i//CHUNK_SIZE+1}/{total_chunks} ({len(chunk)} ContentDocumentIds)")

        distributions = fetch_contentdistributions(sf_source, chunk)

        for dist in distributions:
            try:
                map_entry = doc_mapping[dist["ContentDocumentId"]]
                new_doc_id = map_entry["new_doc"]
                new_ver_id = map_entry["new_ver"]

                payload = {
                    "Name": dist["Name"],
                    "ContentVersionId": new_ver_id,
                    "RelatedRecordId": dist.get("RelatedRecordId"),
                    "PreferencesAllowPDFDownload": dist.get("PreferencesAllowPDFDownload"),
                    "PreferencesAllowOriginalDownload": dist.get("PreferencesAllowOriginalDownload"),
                    "PreferencesPasswordRequired": dist.get("PreferencesPasswordRequired"),
                    "PreferencesNotifyOnVisit": dist.get("PreferencesNotifyOnVisit"),
                    "PreferencesLinkLatestVersion": dist.get("PreferencesLinkLatestVersion"),
                    "PreferencesAllowViewInBrowser": dist.get("PreferencesAllowViewInBrowser"),
                    "PreferencesExpires": dist.get("PreferencesExpires"),
                    "PreferencesNotifyRndtnComplete": dist.get("PreferencesNotifyRndtnComplete"),
                    "ExpiryDate": dist.get("ExpiryDate")
                }

                insert_res = sf_target.ContentDistribution.create(payload)
                new_dist_id = insert_res["id"]

                results.append({
                    "Old_ContentDistributionId": dist["Id"],
                    "Old_ContentDocumentId": dist["ContentDocumentId"],
                    "New_ContentDistributionId": new_dist_id,
                    "New_ContentDocumentId": new_doc_id
                })

                logging.info(f"Migrated ContentDistribution {dist['Id']} -> {new_dist_id}")
                print(f"✅ Migrated: {dist['Id']} -> {new_dist_id}")

            except Exception as e:
                logging.error(f"Failed to migrate ContentDistribution {dist['Id']}: {e}")
                print(f"❌ Failed to migrate ContentDistribution {dist['Id']}: {e}")

    return results

def write_mapping(results):
    """Write migration mapping to CSV."""
    with open(OUTPUT_DISTRIBUTION_MAPPING_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Old_ContentDistributionId",
                "Old_ContentDocumentId",
                "New_ContentDistributionId",
                "New_ContentDocumentId"
            ]
        )
        writer.writeheader()
        writer.writerows(results)
    logging.info(f"Mapping saved to {OUTPUT_DISTRIBUTION_MAPPING_FILE}")

# --------------------------------------------------
# Entry point
# --------------------------------------------------
def main():
    logging.info("=== Starting ContentDistribution Migration ===")
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    doc_mapping = read_doc_ver_mapping()
    print(f"[INFO] Found {len(doc_mapping)} mappings for ContentDistribution migration.")

    results = migrate_distributions(sf_source, sf_target, doc_mapping)
    write_mapping(results)

    logging.info(f"Completed migration of {len(results)} ContentDistributions")
    logging.info("=== Migration Finished ===")

    print(f"[DONE] Migrated {len(results)} ContentDistributions.")
    print(f"[INFO] Output mapping file: {OUTPUT_DISTRIBUTION_MAPPING_FILE}")


if __name__ == "__main__":
    main()
