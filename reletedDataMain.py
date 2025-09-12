#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import logging

from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET

from reletedDataHelper import (
    migrate_attachments,
    migrate_files,
    migrate_feed,
    CHUNK_SIZE_ACTIVITIES,
)
from mappings import FILES_DIR

#INPUT_FILE = os.path.join(FILES_DIR, "eventMessage_import.csv")     # expects Source_Activity_Id,Target_Activity_Id,...
#INPUT_FILE = os.path.join(FILES_DIR, "task_import_log.csv")     # expects Source_Activity_Id,Target_Activity_Id,...
#INPUT_FILE = os.path.join(FILES_DIR, "event_import_log.csv")     # expects Source_Activity_Id,Target_Activity_Id,...
# === Input files for all activity types ===
INPUT_FILES = [
    os.path.join(FILES_DIR, "task_import_log.csv"),          # Task
    # os.path.join(FILES_DIR, "event_import_log.csv"),         # Event
    os.path.join(FILES_DIR, "eventMessage_import.csv"),      # EventMessage
]
OUTPUT_FILE = os.path.join(FILES_DIR, "activity_related_migration.csv")
LOG_FILE = os.path.join(FILES_DIR, "activity_related_migration.log")

# === logging ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

def process_file(sf_source, sf_target, input_file):
    """Process one input mapping file for activity-related migration."""
    if not os.path.exists(input_file):
        logging.warning(f"Skipping missing file: {input_file}")
        return

    df_map = pd.read_csv(input_file)
    if "Source_Activity_Id" not in df_map.columns or "Target_Activity_Id" not in df_map.columns:
        raise ValueError(f"{input_file} must contain 'Source_Activity_Id' and 'Target_Activity_Id' columns.")

    # Normalize
    df_map = df_map[["Source_Activity_Id", "Target_Activity_Id"]].dropna().drop_duplicates()
    activity_map = dict(zip(df_map["Source_Activity_Id"], df_map["Target_Activity_Id"]))
    src_ids_all = list(activity_map.keys())

    logging.info(f"[{os.path.basename(input_file)}] Total activities to process: {len(src_ids_all)}")

    all_results = []
    for start in range(0, len(src_ids_all), CHUNK_SIZE_ACTIVITIES):
        src_chunk = src_ids_all[start:start + CHUNK_SIZE_ACTIVITIES]
        file_map = {}

        logging.info(f"[{os.path.basename(input_file)}] Processing {start+1} to {start+len(src_chunk)}")

        # 1) Attachments
        logging.info("Migrating Attachments...")
        migrate_attachments(sf_source, sf_target, src_chunk, activity_map, all_results)

        # 2) Files
        logging.info("Migrating Files...")
        migrate_files(sf_source, sf_target, src_chunk, activity_map, all_results, file_map)

        # 3) Feed
        logging.info("Migrating Feed (posts & comments)...")
        migrate_feed(sf_source, sf_target, src_chunk, activity_map, all_results, file_map)

        # Save checkpoint
        output_file = os.path.join(
            FILES_DIR, f"{os.path.splitext(os.path.basename(input_file))[0]}_related_migration.csv"
        )
        pd.DataFrame(all_results).to_csv(output_file, index=False, encoding="utf-8-sig")
        logging.info(f"Checkpoint saved after {start+len(src_chunk)} activities → {output_file}")

    logging.info(f"[{os.path.basename(input_file)}] Migration finished.")


def main():
    logging.info("Connecting to Salesforce orgs...")
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)
    logging.info("Connected.")

    for input_file in INPUT_FILES:
        process_file(sf_source, sf_target, input_file)

    logging.info("✅ All migrations completed.")


# def main():
#     logging.info("Connecting to Salesforce orgs...")
#     sf_source = connect_salesforce(SF_SOURCE)
#     sf_target = connect_salesforce(SF_TARGET)
#     logging.info("Connected.")

#     if not os.path.exists(INPUT_FILE):
#         raise FileNotFoundError(f"Input mapping file not found: {INPUT_FILE}")

#     df_map = pd.read_csv(INPUT_FILE)
#     if "Source_Activity_Id" not in df_map.columns or "Target_Activity_Id" not in df_map.columns:
#         raise ValueError("Input CSV must contain 'Source_Activity_Id' and 'Target_Activity_Id' columns.")

#     # Normalize and drop empties
#     df_map = df_map[["Source_Activity_Id", "Target_Activity_Id"]].dropna().drop_duplicates()

#     all_results = []

#     # Build dictionary for quick lookup
#     activity_map = dict(zip(df_map["Source_Activity_Id"], df_map["Target_Activity_Id"]))
#     src_ids_all = list(activity_map.keys())

#     logging.info(f"Total activities to process: {len(src_ids_all)}")

#     # main()
    

#     # Process in chunks to avoid huge queries & to checkpoint results
#     for start in range(0, len(src_ids_all), CHUNK_SIZE_ACTIVITIES):
#         src_chunk = src_ids_all[start:start + CHUNK_SIZE_ACTIVITIES]
#         file_map = {}
#         logging.info(f"Processing activities {start+1} to {start+len(src_chunk)}")

#         # 1) Attachments
#         logging.info("Migrating Attachments...")
#         migrate_attachments(sf_source, sf_target, src_chunk, activity_map, all_results)

#         # 2) Files (ContentDocument/Version/Link)
#         logging.info("Migrating Files...")
#         migrate_files(sf_source, sf_target, src_chunk, activity_map, all_results, file_map)
#         pd.DataFrame(all_results).to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

#         # 3) Feed (FeedItem / FeedComment)
#         logging.info("Migrating Feed (posts & comments)...")
#         migrate_feed(sf_source, sf_target, src_chunk, activity_map, all_results, file_map)

#         # Save intermediate output after each chunk
#         pd.DataFrame(all_results).to_csv(OUTPUT_FILE, index=False)
#         logging.info(f"Checkpoint saved after {start+len(src_chunk)} activities → {OUTPUT_FILE}")

#     logging.info("Migration finished.")
#     pd.DataFrame(all_results).to_csv(OUTPUT_FILE, index=False)
#     logging.info(f"Final results saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
