#!/usr/bin/env python3
"""
stream_contentdocumentlink_mapping.py

Purpose:
  - Stream ContentDocumentLink records from SOURCE org constrained by OBJECT_CONDITIONS.
  - Fetch ALL ContentDocumentLinks for documents (including user shares) to preserve document sharing.
  - Map LinkedEntityId -> target record Id using Card_Legacy_Id__c in TARGET org.
  - Write mapping rows to CSV: Source_ContentDocumentLink_Id, ContentDocumentId, Source_Parent_Id, Target_Parent_Id, Parent_Object, ShareType, Visibility

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
from typing import Dict,Iterable, List, Generator, Any
from utils.mappings import FILES_DIR, fetch_service_appointment_ids,fetch_user_ids
from utils.retry_utils import safe_query

OUTPUT_CSV = os.path.join(FILES_DIR, "contentdocumentlink_mapping.csv")
file_exists = os.path.exists(OUTPUT_CSV)
UNMATCHED_CSV = os.path.join(FILES_DIR, "contentdocumentlink_unmatched.csv")
unmatched_exists = os.path.exists(UNMATCHED_CSV)
LOG_FILE = os.path.join(FILES_DIR, "contentdocumentlink_mapping.log")

# === Logging configuration ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Runtime config ===
CHUNK_SIZE = 200
SLEEP_BETWEEN_RETRIES = 2
MAX_RETRIES = 5

# === Object Conditions ===
OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false AND DE_Is_Shell_Account__c = false ",
    "Contact": "Account.RecordType.Name IN ('Parent Company','Brand','Dealer') AND Account.IsPersonAccount = false",
    "Impact_Tracker__c": "Clients_Brands__c!=null and Clients_Brands__r.RecordType.name in ('Parent Company','Brand','Dealer')",
    "Order": "RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive') AND Account.recordtype.name IN  ('Parent Company','Brand','Dealer')",
    "Request__c": "Id in (select Request__c from Request_Brand_Division__c where   Brand__r.RecordType.name in ('Parent Company','Brand','Dealer') and Division__r.RecordType.name ='Brand Program')",
    "ServiceAppointment": "",
    "WorkOrder": "Field_Win_Win__c IN (select id from order where  RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive') AND Account.recordtype.name IN ('Parent Company','Brand','Dealer'))"
}

# OBJECT_CONDITIONS = {
#     "Request__c": "Id in (select Request__c from Request_Brand_Division__c where   Brand__r.RecordType.name in ('Parent Company','Brand','Dealer') and Division__r.RecordType.name ='Brand Program')"
# }
# OBJECT_CONDITIONS = {
#     "ServiceAppointment": "",
#     "User": "isActive = true",
#     "WorkOrder": "Field_Win_Win__c IN (select id from order where  RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive'))"
# }

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def chunked(iterable: Iterable[Any], n: int) -> Generator[List[Any], None, None]:
    iterable = list(iterable)  # convert to list so slicing works
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]


def safe_query_all(sf: Salesforce, soql: str):
    attempt = 0
    while True:
        try:
            logging.debug(f"Executing SOQL: {soql}")
            return safe_query(sf, soql).get("records", [])
        except Exception as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                logging.error(f"SOQL failed after {MAX_RETRIES} retries. Error: {e}")
                raise
            wait = SLEEP_BETWEEN_RETRIES * (2 ** (attempt - 1))
            logging.warning(f"Query failed (attempt {attempt}/{MAX_RETRIES}). Retrying in {wait}s. Error: {e}")
            time.sleep(wait)


def fetch_source_parent_ids(sf_source: Salesforce,sf_target: Salesforce, obj: str, condition: str) -> List[str]:
     
    if obj == "ServiceAppointment":
        filtered_ids = set()
        filtered_ids=fetch_service_appointment_ids(sf_source)
        return filtered_ids
    
    if obj == "User":
        user_ids = set()
        user_ids = fetch_user_ids(sf_target)
        ids_str= ",".join(f"'{uid}'" for uid in user_ids)
        condition = f"Id IN ({ids_str}) AND {condition}" if condition else f"Id IN ({ids_str})"
        
     
    if condition.strip():
        soql = f"SELECT Id FROM {obj} WHERE {condition}"
    else:
        soql = f"SELECT Id FROM {obj}"
    logging.info(f"Fetching parent Ids from source for {obj} with condition: {condition or 'NO CONDITION'}")
    records = safe_query_all(sf_source, soql)
    ids = [r["Id"] for r in records]
    logging.info(f"Found {len(ids)} parent records for {obj}")
    print(f"Found {len(ids)} parent records for {obj}")
    return ids


def fetch_cdls_for_parent_chunk(sf_source: Salesforce, parent_ids_chunk: List[str]) -> List[Dict]:
    """Fetch ContentDocumentLinks for parent entities, then fetch ALL links for those documents"""
    ids_csv = ",".join(f"'{pid}'" for pid in parent_ids_chunk)
    
    # First, get ContentDocumentLinks for the parent entities
    parent_soql = f"""
        SELECT Id, ContentDocumentId, LinkedEntityId, ShareType, Visibility
        FROM ContentDocumentLink
        WHERE ContentDocument.CreatedDate >= LAST_N_MONTHS:24 AND LinkedEntityId IN ({ids_csv})
    """
    parent_links = safe_query_all(sf_source, parent_soql)
    
    if not parent_links:
        return []
    
    # Extract unique ContentDocumentIds from parent links
    content_doc_ids = list(set([link["ContentDocumentId"] for link in parent_links]))
    
    # Now fetch ALL ContentDocumentLinks for these documents (including user shares)
    doc_ids_csv = ",".join(f"'{doc_id}'" for doc_id in content_doc_ids)
    all_links_soql = f"""
        SELECT Id, ContentDocumentId, LinkedEntityId, ShareType, Visibility, LinkedEntity.Type
        FROM ContentDocumentLink
        WHERE ContentDocumentId IN ({doc_ids_csv})
    """
    
    all_links = safe_query_all(sf_source, all_links_soql)
    logging.info(f"Found {len(parent_links)} parent links and {len(all_links)} total links (including user shares)")
    
    return all_links


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


def build_user_target_map(sf_target: Salesforce, source_user_ids: List[str]) -> Dict[str, str]:
    """Build mapping for Users from source to target org"""
    if not source_user_ids:
        return {}
    
    ids_csv = ",".join(f"'{uid}'" for uid in source_user_ids)
    soql = f"SELECT Id, Card_Legacy_Id__c FROM User WHERE Card_Legacy_Id__c IN ({ids_csv})"
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

    with open(OUTPUT_CSV, mode="a", newline="", encoding="utf-8") as csvfile, \
         open(UNMATCHED_CSV, mode="a", newline="", encoding="utf-8") as unmatched_csvfile:
        writer = csv.writer(csvfile)
        unmatched_writer = csv.writer(unmatched_csvfile)
        # Only write header if file is new
        if not file_exists:
            writer.writerow([
                "Source_ContentDocumentLink_Id",
                "ContentDocumentId",
                "Source_Parent_Id",
                "Target_Parent_Id",
                "Parent_Object",
                "ShareType",
                "Visibility"
            ])
        if not unmatched_exists:
            unmatched_writer.writerow([
                "Source_ContentDocumentLink_Id",
                "ContentDocumentId",
                "Source_Parent_Id",
                "Target_Parent_Id",
                "Parent_Object",
                "ShareType",
                "Visibility"
            ])

        total_rows = 0

        for obj, condition in OBJECT_CONDITIONS.items():
            parent_ids = fetch_source_parent_ids(sf_source,sf_target, obj, condition)
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

                # Separate parent entity links from user links
                parent_links = []
                user_links = []
                user_ids = set()
                
                for link in cdls:
                    linked_entity_type = link.get("LinkedEntity", {}).get("Type", "Unknown")
                    if linked_entity_type == "User":
                        user_links.append(link)
                        user_ids.add(link.get("LinkedEntityId"))
                    elif link.get("LinkedEntityId") in parent_chunk:
                        parent_links.append(link)

                logging.info(f"[{obj}] chunk {chunk_idx}: Found {len(parent_links)} parent links and {len(user_links)} user shares")
                print(f"[INFO] [{obj}] chunk {chunk_idx}: Found {len(parent_links)} parent links and {len(user_links)} user shares")

                # Build target mappings for both parent entities and users
                try:
                    target_map = build_target_map_for_chunk(sf_target, obj, parent_chunk)
                except Exception as e:
                    logging.error(f"Failed to query target mapping for {obj} chunk {chunk_idx}: {e}")
                    print(f"[ERROR] Failed to query target mapping for {obj} chunk {chunk_idx}: {e}")
                    target_map = {}

                try:
                    user_target_map = build_user_target_map(sf_target, list(user_ids))
                except Exception as e:
                    logging.error(f"Failed to query user target mapping for chunk {chunk_idx}: {e}")
                    print(f"[ERROR] Failed to query user target mapping for chunk {chunk_idx}: {e}")
                    user_target_map = {}

                # Process parent entity links
                for link in parent_links:
                    src_cdl_id = link.get("Id")
                    content_doc_id = link.get("ContentDocumentId")
                    src_parent_id = link.get("LinkedEntityId")
                    share_type = link.get("ShareType") 
                    # If Sharetype is I then set to V
                    if share_type == "I":
                        share_type = "V"
                    visibility = link.get("Visibility")
                    
                    tgt_parent_id = target_map.get(src_parent_id)

                    if tgt_parent_id:
                        writer.writerow([src_cdl_id, content_doc_id, src_parent_id, tgt_parent_id, obj, share_type, visibility])
                    else:
                        unmatched_writer.writerow([src_cdl_id, content_doc_id, src_parent_id, "", obj, share_type, visibility])
                    total_rows += 1

                # Process user links
                for link in user_links:
                    src_cdl_id = link.get("Id")
                    content_doc_id = link.get("ContentDocumentId")
                    src_user_id = link.get("LinkedEntityId")
                    share_type = link.get("ShareType")
                     # If Sharetype is I then set to V
                    if share_type == "I":
                        share_type = "V"
                    visibility = link.get("Visibility")
                    
                    tgt_user_id = user_target_map.get(src_user_id)

                    if tgt_user_id:
                        writer.writerow([src_cdl_id, content_doc_id, src_user_id, tgt_user_id, "User", share_type, visibility])
                    else:
                        unmatched_writer.writerow([src_cdl_id, content_doc_id, src_user_id, "", "User", share_type, visibility])
                    total_rows += 1

                csvfile.flush()
                unmatched_csvfile.flush()

                if chunk_idx % 10 == 0:
                    logging.info(f"Processed {chunk_idx} chunks for {obj}... total rows so far: {total_rows}")
                    print(f"[INFO] Processed {chunk_idx} chunks for {obj}... total rows so far: {total_rows}")

        logging.info(f"Completed. Total mapping rows written: {total_rows}")
        logging.info(f"Output CSV: {OUTPUT_CSV}")
        print(f"[DONE] Completed. Total mapping rows written: {total_rows}")
        print(f"[INFO] Output CSV: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()