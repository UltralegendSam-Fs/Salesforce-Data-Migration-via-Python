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


OBJECT_CONDITIONS = {
   " User": "isActive = true"
}

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


# def fetch_cdls_for_parent_chunk(sf_source: Salesforce,sf_target:Salesforce, parent_ids_chunk: List[str]) -> List[Dict]:
#     """Fetch ContentDocumentLinks for parent entities, then fetch ALL links for those documents"""
#     ids_csv = ",".join(f"'{pid}'" for pid in parent_ids_chunk)
    
#     # First, get ContentDocumentLinks for the parent entities
#     parent_soql = f"""
#         SELECT Id, ContentDocumentId, LinkedEntityId, ShareType, Visibility
#         FROM ContentDocumentLink
#         WHERE ContentDocument.CreatedDate = TODAY AND LinkedEntityId IN ({ids_csv})
#     """
#     parent_links = safe_query_all(sf_source, parent_soql)
#     if not parent_links:
#         return []
    
#     # Extract unique ContentDocumentIds from parent links
#     content_doc_ids = list(set([link["ContentDocumentId"] for link in parent_links]))

#     if not content_doc_ids:
#         return []
    
#     doc_ids= ",".join(f"'{doc_id}'" for doc_id in content_doc_ids)

#     # Fetch only those ContentDocumentIds that have exactly one parent link (to avoid duplicates)
#     all_links_soql = f"""
#         SELECT Count(Id), ContentDocumentId
#         FROM ContentDocumentLink
#         WHERE ContentDocumentId IN ({doc_ids}) group by ContentDocumentId HAVING COUNT(Id) = 1  
#     """
#     valid_Link= safe_query_all(sf_source, all_links_soql)
#     valid_doc_ids = set(link["ContentDocumentId"] for link in valid_Link)
#     if not valid_doc_ids:
#         return []
    
#     valid_link_ids= ",".join(f"'{doc_id}'" for doc_id in valid_doc_ids)
    
#     # Now fetch ALL ContentDocumentLinks for these documents (including user shares)
#     all_links_soql = f"""
#         SELECT Id, ContentDocumentId, LinkedEntityId, ShareType, Visibility
#         FROM ContentDocumentLink
#         WHERE ContentDocumentId IN ({valid_link_ids})
#     """
#     all_links = safe_query_all(sf_source, all_links_soql)
#     logging.info(f"Found {len(parent_links)} parent links and {len(all_links)} total links (including user shares)")
#     print(f"Found {len(parent_links)} parent links and {len(all_links)} total links (including user shares)")
    
#     return all_links

def fetch_cdls_for_parent_chunk(sf_source: Salesforce, sf_target: Salesforce, parent_ids_chunk: List[str]) -> List[Dict]:
    """
    Fetch only ContentDocumentLinks where documents are uploaded directly on User records
    (i.e., the document has exactly one link, and that link is to a User record).
    """
    ids_csv = ",".join(f"'{pid}'" for pid in parent_ids_chunk)

    # Step 1: Get ContentDocumentLinks directly linked to User records in our chunk
    parent_soql = f"""
        SELECT Id, ContentDocumentId, LinkedEntityId, ShareType, Visibility
        FROM ContentDocumentLink
        WHERE ContentDocument.CreatedDate = TODAY AND LinkedEntityId IN ({ids_csv})
    """
    parent_links = safe_query_all(sf_source, parent_soql)
    if not parent_links:
        return []

    # Step 2: Extract ContentDocumentIds
    content_doc_ids = list(set([link["ContentDocumentId"] for link in parent_links]))
    if not content_doc_ids:
        return []

    doc_ids_csv = ",".join(f"'{doc_id}'" for doc_id in content_doc_ids)

    # Step 3: Keep only documents that have a single parent link (uploaded directly on User)
    single_parent_docs_soql = f"""
        SELECT ContentDocumentId
        FROM ContentDocumentLink
        WHERE ContentDocumentId IN ({doc_ids_csv})
        GROUP BY ContentDocumentId
        HAVING COUNT(Id) = 1
    """
    valid_links = safe_query_all(sf_source, single_parent_docs_soql)
    valid_doc_ids = set(link["ContentDocumentId"] for link in valid_links)
    if not valid_doc_ids:
        return []

    valid_doc_ids_csv = ",".join(f"'{doc_id}'" for doc_id in valid_doc_ids)

    # Step 4: Get only those valid links (directly on User)
    direct_user_links_soql = f"""
        SELECT Id, ContentDocumentId, LinkedEntityId, ShareType, Visibility
        FROM ContentDocumentLink
        WHERE ContentDocumentId IN ({valid_doc_ids_csv})
        AND LinkedEntityId IN ({ids_csv})
    """
    direct_user_links = safe_query_all(sf_source, direct_user_links_soql)

    logging.info(f"Found {len(direct_user_links)} direct user document links.")
    print(f"Found {len(direct_user_links)} direct user document links.")

    return direct_user_links

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
                # print(f"[INFO] [{obj}] chunk {chunk_idx}/{chunk_count} -> {len(parent_chunk)} parent ids.")

                try:
                    cdls = fetch_cdls_for_parent_chunk(sf_source,sf_target, parent_chunk)
                except Exception as e:
                    logging.error(f"Failed to fetch ContentDocumentLink chunk for {obj} chunk {chunk_idx}: {e}")
                    print(f"[ERROR] Failed to fetch ContentDocumentLink chunk for {obj} chunk {chunk_idx}: {e}")
                    continue

                if not cdls:
                    logging.info(f"No ContentDocumentLinks found for {obj} chunk {chunk_idx}")
                    continue

                # Separate parent entity links from user links
                parent_links = []
                
                for link in cdls:
                    parent_links.append(link)

                logging.info(f"[{obj}] chunk {chunk_idx}: Found {len(parent_links)} parent links ")
                print(f"[INFO] [{obj}] chunk {chunk_idx}: Found {len(parent_links)} parent links ")

                # Build target mappings for both parent entities and users
                try:
                    target_map = build_target_map_for_chunk(sf_target, obj, parent_chunk)
                except Exception as e:
                    logging.error(f"Failed to query target mapping for {obj} chunk {chunk_idx}: {e}")
                    print(f"[ERROR] Failed to query target mapping for {obj} chunk {chunk_idx}: {e}")
                    target_map = {}

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