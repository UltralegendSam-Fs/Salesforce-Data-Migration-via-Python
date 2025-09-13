#!/usr/bin/env python3
"""
stream_contentdocumentlink_mapping.py

Purpose:
  - Stream ContentDocumentLink records from SOURCE org constrained by OBJECT_CONDITIONS.
  - Map LinkedEntityId -> target record Id using Card_Legacy_Id__c in TARGET org.
  - Write mapping rows to CSV: Source_ContentDocumentLink_Id, ContentDocumentId, Source_Parent_Id, Target_Parent_Id

Notes:
  - For large volumes we use chunked processing and CSV streaming.
  - Tune CHUNK_SIZE depending on your org (safe default 800).
"""

import os
import csv
import time
import math
import logging
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from typing import List, Dict, Iterable
from mappings import FILES_DIR, fetch_service_appointment_ids

OUTPUT_CSV = os.path.join(FILES_DIR, "contentdocumentlink_mapping.csv")
LOG_FILE = os.path.join(FILES_DIR, "contentdocumentlink_mapping.log")

# === Logging configuration ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Runtime config ===
CHUNK_SIZE = 800
SLEEP_BETWEEN_RETRIES = 2
MAX_RETRIES = 5

# === Object Conditions ===
# OBJECT_CONDITIONS = {
#     "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false",
#     "Contact": "Account.RecordType.Name IN ('Parent Company','Brand','Dealer') AND Account.IsPersonAccount = false",
#     "CollaborationGroup": "",
#     "Impact_Tracker__c": "Clients_Brands__c!=null and Clients_Brands__r.RecordType.name in ('Parent Company','Brand','Dealer')",
#     "Order": "RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive')",
#     "Request__c": "Brand__r.RecordType.name in ('Parent Company','Brand','Dealer') and Division__r.RecordType.name ='Brand Program')",
#     "ServiceAppointment": "",
#     "User": "isActive = true",
#     "WorkOrder": "Field_Win_Win__c IN (select id from order where  RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive'))"
# }
OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false"
}

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def chunked(iterable: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]


def safe_query_all(sf: Salesforce, soql: str):
    attempt = 0
    while True:
        try:
            logging.debug(f"Executing SOQL: {soql}")
            return sf.query_all(soql).get("records", [])
        except Exception as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                logging.error(f"SOQL failed after {MAX_RETRIES} retries. Error: {e}")
                raise
            wait = SLEEP_BETWEEN_RETRIES * (2 ** (attempt - 1))
            logging.warning(f"Query failed (attempt {attempt}/{MAX_RETRIES}). Retrying in {wait}s. Error: {e}")
            time.sleep(wait)


def fetch_source_parent_ids(sf_source: Salesforce, obj: str, condition: str) -> List[str]:
     
    if obj == "ServiceAppointment":
        filtered_ids = set()
        filtered_ids=fetch_service_appointment_ids(sf_source)
        return filtered_ids
     
    if condition.strip():
        soql = f"SELECT Id FROM {obj} WHERE {condition}"
    else:
        soql = f"SELECT Id FROM {obj}"
        
    logging.info(f"Fetching parent Ids from source for {obj} with condition: {condition or 'NO CONDITION'}")
    records = safe_query_all(sf_source, soql)
    ids = [r["Id"] for r in records]
    logging.info(f"Found {len(ids)} parent records for {obj}")
    return ids


def fetch_cdls_for_parent_chunk(sf_source: Salesforce, parent_ids_chunk: List[str]) -> List[Dict]:
    ids_csv = ",".join(f"'{pid}'" for pid in parent_ids_chunk)
    soql = f"""
        SELECT Id, ContentDocumentId, LinkedEntityId
        FROM ContentDocumentLink
        WHERE ContentDocument.CreatedDate = TODAY AND LinkedEntityId IN ({ids_csv})
    """
    return safe_query_all(sf_source, soql)


def build_target_map_for_chunk(sf_target: Salesforce, obj: str, source_parent_chunk: List[str]) -> Dict[str, str]:
    ids_csv = ",".join(f"'{sid}'" for sid in source_parent_chunk)
    soql = f"SELECT Id, Card_Legacy_Id__c FROM {obj} WHERE Card_Legacy_Id__c IN ({ids_csv})"
    records = safe_query_all(sf_target, soql)
    mapping = {}
    for r in records:
        legacy = r.get("Card_Legacy_Id__c")
        if legacy:
            mapping[legacy] = r["Id"]
    return mapping


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    logging.info("=== Starting ContentDocumentLink Mapping ===")
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Source_ContentDocumentLink_Id",
            "ContentDocumentId",
            "Source_Parent_Id",
            "Target_Parent_Id",
            "Parent_Object"
        ])

        total_rows = 0

        for obj, condition in OBJECT_CONDITIONS.items():
            parent_ids = fetch_source_parent_ids(sf_source, obj, condition)
            if not parent_ids:
                logging.info(f"No parent records found for {obj}, skipping...")
                continue

            chunk_count = math.ceil(len(parent_ids) / CHUNK_SIZE)
            logging.info(f"Processing {len(parent_ids)} parent ids for {obj} in {chunk_count} chunks.")

            for chunk_idx, parent_chunk in enumerate(chunked(parent_ids, CHUNK_SIZE), start=1):
                logging.info(f"[{obj}] chunk {chunk_idx}/{chunk_count} -> {len(parent_chunk)} parent ids.")
                print(f"[INFO] [{obj}] chunk {chunk_idx}/{chunk_count} -> {len(parent_chunk)} parent ids.")

                try:
                    cdls = fetch_cdls_for_parent_chunk(sf_source, parent_chunk)
                except Exception as e:
                    logging.error(f"Failed to fetch ContentDocumentLink chunk for {obj} chunk {chunk_idx}: {e}")
                    print(f"[ERROR] Failed to fetch ContentDocumentLink chunk for {obj} chunk {chunk_idx}: {e}")
                    continue

                if not cdls:
                    logging.info(f"No ContentDocumentLinks found for {obj} chunk {chunk_idx}")
                    continue

                try:
                    target_map = build_target_map_for_chunk(sf_target, obj, parent_chunk)
                except Exception as e:
                    logging.error(f"Failed to query target mapping for {obj} chunk {chunk_idx}: {e}")
                    print(f"[ERROR] Failed to query target mapping for {obj} chunk {chunk_idx}: {e}")
                    target_map = {}

                for link in cdls:
                    src_cdl_id = link.get("Id")
                    content_doc_id = link.get("ContentDocumentId")
                    src_parent_id = link.get("LinkedEntityId")
                    tgt_parent_id = target_map.get(src_parent_id)
                    writer.writerow([src_cdl_id, content_doc_id, src_parent_id, tgt_parent_id, obj])
                    total_rows += 1

                csvfile.flush()

                if chunk_idx % 10 == 0:
                    logging.info(f"Processed {chunk_idx} chunks for {obj}... total rows so far: {total_rows}")
                    print(f"[INFO] Processed {chunk_idx} chunks for {obj}... total rows so far: {total_rows}")

        logging.info(f"Completed. Total mapping rows written: {total_rows}")
        logging.info(f"Output CSV: {OUTPUT_CSV}")
        print(f"[DONE] Completed. Total mapping rows written: {total_rows}")
        print(f"[INFO] Output CSV: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()