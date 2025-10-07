#!/usr/bin/env python3
"""
feeditem_migration.py

Purpose:
  - Fetch FeedItem records from SOURCE Salesforce org
  - Apply OBJECT_CONDITIONS to filter by parent objects
  - Build Source→Target mappings using fetch_target_mappings()
  - Insert FeedItems into TARGET org
  - Store logs & results inside /files folder
"""

import os
import csv
import re
import logging
import html
from datetime import datetime
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from utils.mappings import fetch_target_mappings, fetch_createdByIds,fetch_service_appointment_ids,related_recordid_mapping,FILES_DIR
 
from utils.retry_utils import safe_query

BATCH_SIZE = 200

RESULT_FILE = os.path.join(FILES_DIR, "feeditem_results.csv")
FEEDITEM_EXPORT = os.path.join(FILES_DIR, "feedItem_export.csv")
FEEDITEM_INVALID = os.path.join(FILES_DIR, "feeditem_invalid.csv")
LOG_FILE = os.path.join(FILES_DIR, "feeditem_migration.log")

# === Logging Setup ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# === Object Conditions ===
# OBJECT_CONDITIONS = {
#     "Impact_Tracker__c": "Clients_Brands__c!=null and Clients_Brands__r.RecordType.name in ('Parent Company','Brand','Dealer')",
#     "Order": " RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive')",
#     "ServiceAppointment": "",
#     "User": "IsActive =true",
#     "WorkOrder": "Field_Win_Win__c IN (select id from order where  RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive'))"
# }

OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false AND DE_Is_Shell_Account__c = false"
}

    
def fetch_filtered_feeditems(sf_source,sf_target, obj_name, condition):
    """Fetch FeedItems for a specific object with optional condition on parent"""
    soql = f"SELECT Id FROM {obj_name}"
    if condition:
        soql += f" WHERE {condition}"

    try:
        if obj_name == "ServiceAppointment":
            # Special handling for ServiceAppointment to include specific conditions
            sa_ids = fetch_service_appointment_ids(sf_source)
            parent_ids = sa_ids
        else:
            parent_records = safe_query(sf_source, soql)["records"]
            parent_ids = {p["Id"] for p in parent_records}
        if not parent_ids:
            return []

        # 🔹 SOQL IN clause has 2000 limit, so chunk
        feeditems = []
        parent_list = list(parent_ids)
        for i in range(0, len(parent_list), 500):
            chunk = parent_list[i:i+500]
            ids_str = ",".join([f"'{pid}'" for pid in chunk])
            query = f"""
                SELECT Id, ParentId, Body, LinkUrl, Type, RelatedRecordId,CreatedById, CreatedDate,IsRichText,Visibility,Title
                FROM FeedItem
                WHERE ParentId IN ({ids_str})
            """
            records = safe_query(sf_source, query)["records"]
            records = related_recordid_mapping(sf_source,sf_target,records,"Item")
            feeditems.extend(records)

        logging.info(f"Fetched {len(feeditems)} FeedItems for {obj_name}")
        return feeditems

    except Exception as e:
        logging.error(f"Error fetching FeedItems for {obj_name}: {e}")
        return []

def insert_feeditems_with_retry(sf_target, target_feeditems, results, max_retries=3, retry_delay=5):
    """
    Insert FeedItems into target org in bulk with retry logic.
    Appends success/failure results to the results list.
    """
    import time
    
    try:
        
        # Retry logic for bulk insert
        insert_results = None
        for attempt in range(max_retries):
            try:
                insert_results = sf_target.bulk.FeedItem.insert([fi for *_, fi in target_feeditems])
                break  # Success, exit retry loop
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"FeedItem bulk insert failed after {max_retries} attempts: {e}")
                    # Mark all records as failed
                    insert_results = [{"success": False, "id": None, "errors": [str(e)]} for _ in target_feeditems]
                else:
                    logging.warning(f"FeedItem bulk insert attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
        

        for (src_id, src_parent, tgt_parent, _), ins_res in zip(target_feeditems, insert_results):
            if ins_res["success"]:
                results.append({
                    "Source_FeedItem_Id": src_id,
                    "Target_FeedItem_Id": ins_res.get("id"),
                    "Source_Parent_Id": src_parent,
                    "Target_Parent_Id": tgt_parent,
                    "Status": "Success"
                })
            else:
                print(f"❌ Errors: {ins_res['errors']} for Source Id: {src_id}")
                results.append({
                    "Source_FeedItem_Id": src_id,
                    "Target_FeedItem_Id": "",
                    "Source_Parent_Id": src_parent,
                    "Target_Parent_Id": tgt_parent,
                    "Status": f"Failed: {ins_res.get('errors')}"
                })
    except Exception as e:
        logging.error(f"FeedItem insertion failed: {e}")
        for src_id, src_parent, tgt_parent, _ in target_feeditems:
            results.append({
                "Source_FeedItem_Id": src_id,
                "Target_FeedItem_Id": "",
                "Source_Parent_Id": src_parent,
                "Target_Parent_Id": tgt_parent,
                "Status": f"Failed: {str(e)}"
            })
    return results

def insert_feeditems(sf_target, target_feeditems, results):
    """Legacy function - now calls retry version"""
    return insert_feeditems_with_retry(sf_target, target_feeditems, results)

def migrate_feeditems(sf_source, sf_target):
    """Main migration loop"""
    all_feeditems = []
    all_mappings = {}
    createdBy_mappings = {}
    relatedId_mappings = {}

    for obj_name, condition in OBJECT_CONDITIONS.items():
        logging.info(f"Processing object: {obj_name}")

        # Step 1: Fetch FeedItems
        relevant_feeditems = fetch_filtered_feeditems(sf_source, sf_target, obj_name, condition)
        if not relevant_feeditems:
            continue

        all_feeditems.extend(relevant_feeditems)

        # Step 2: Collect parent IDs
        parent_ids = {fi["ParentId"] for fi in relevant_feeditems}
        
        # Step 3: Build mappings
        all_mappings.update(fetch_target_mappings(sf_target, obj_name, parent_ids, BATCH_SIZE))
        
        # createdBy_ids = {fi["CreatedById"] for fi in relevant_feeditems}
        # createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)
        
        related_ids = {fi["RelatedRecordId"] for fi in relevant_feeditems}
        relatedId_mappings = fetch_target_mappings(sf_target, "ContentVersion", related_ids, BATCH_SIZE)

    logging.info(f"Total FeedItems collected: {len(all_feeditems)}")
    print(f"Total FeedItems collected: {len(all_feeditems)}")

    results = []
    invalid_rows = []

    # Step 4: Prepare records for insertion
    for i in range(0, len(all_feeditems), BATCH_SIZE):
        chunk = all_feeditems[i:i + BATCH_SIZE]
        target_feeditems = []

        for record in chunk:
            src_parent = record.get("ParentId")
            tgt_parent = all_mappings.get(src_parent)
            # tgt_createdBy = createdBy_mappings.get(record.get("CreatedById"))

            feed_body = record.get("Body") or ""

            # Skip invalid cases
            if not tgt_parent or "status changed to" in feed_body.lower():
                status = "Skipped - No Parent Mapping" if not tgt_parent else "Skipped - Invalid Body"
                invalid_rows.append([
                    record["Id"],
                    src_parent,
                    tgt_parent or "",
                    status
                ])
                continue

            # Build insertable record
            new_feeditem = {
                "ParentId": tgt_parent,
                "Body": record.get("Body"),
                "LinkUrl": record.get("LinkUrl"),
                "CreatedById": record.get("CreatedById"),  
                "CreatedDate": record.get("CreatedDate"),
                "IsRichText": record.get("IsRichText"),
                "Visibility": record.get("Visibility"),
                "Title": record.get("Title"),
            }

            if record.get("RelatedRecordId") and record["RelatedRecordId"] in relatedId_mappings:
                new_feeditem["RelatedRecordId"] = relatedId_mappings[record["RelatedRecordId"]]

            target_feeditems.append((record["Id"], src_parent, tgt_parent, new_feeditem))
            
        if target_feeditems:
            with open(FEEDITEM_EXPORT, "a", newline="", encoding="utf-8") as fexp:
                writer = csv.writer(fexp)
                if fexp.tell() == 0:  # write header only once
                    writer.writerow([
                        "Source_FeedItem_Id",
                        "Source_Parent_Id",
                        "Target_Parent_Id",
                        "Source_CreatedById",
                        "Target_CreatedById"
                    ])
                for src_id, src_parent, tgt_parent, new_feeditem in target_feeditems:
                    writer.writerow([
                        src_id,
                        src_parent,
                        tgt_parent,
                        record.get("CreatedById", ""),          # source creator
                        new_feeditem.get("CreatedById", "")     # target creator
                    ])


        if target_feeditems:
            results = insert_feeditems(sf_target, target_feeditems, results)

    # Write invalid rows CSV
    if invalid_rows:
        with open(FEEDITEM_INVALID, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Source_FeedItem_Id", "Source_Parent_Id", "Target_Parent_Id", "Reason"])
            writer.writerows(invalid_rows)

    return results

def write_results(results):
    """Save results to CSV"""
    keys = ["Source_FeedItem_Id", "Target_FeedItem_Id", "Source_Parent_Id", "Target_Parent_Id", "Status"]
    with open(RESULT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)



def main():
    logging.info("=== Starting FeedItem Migration ===")

    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    results = migrate_feeditems(sf_source, sf_target)

    write_results(results)

    logging.info("=== FeedItem Migration Completed ===")


if __name__ == "__main__":
    main()
