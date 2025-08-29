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
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from mappings import fetch_target_mappings  # 🔹 your existing mapping function

# === Paths ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.path.join(BASE_DIR, "files")
os.makedirs(FILES_DIR, exist_ok=True)

RESULT_FILE = os.path.join(FILES_DIR, "feeditem_results.csv")
LOG_FILE = os.path.join(FILES_DIR, "feeditem_migration.log")

# === Logging Setup ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

BATCH_SIZE = 200

# === Object Conditions ===
# OBJECT_CONDITIONS = {
#     "Account": "RecordType.Name IN ('Parent Company','Brand','Retired','Dealer') AND IsPersonAccount = false",
#     "Asset": "Account.RecordType.Name IN ('Parent Company','Brand','Dealer') AND RecordType.Name = 'Brand Program'",
#     "BYO_Enhanced_Field_History__c": "",
#     "Capability_Enablement__c": "",
#     "Credit_Card_Program_Benefits__c": "Account__r.RecordType.Name IN ('Parent Company','Brand','Dealer')",
#     "DE_Application__c": "",
#     "FSL__GanttPalette__c": "",
#     "Impact_Tracker__c": "",
#     "Order": "",
#     "Product2": "RecordType.Name = 'Brand Product'",
#     "Products_Services__c": "",
#     "Request_Brand_Division__c": "Brand__r.RecordType.Name IN ('Parent Company','Brand','Dealer') AND Division__r.RecordType.Name = 'Brand Program' AND Request__c != null",
#     "Request__c": "",
#     "Risk_Exception__c": "",
#     "ServiceAppointment": "",
#     "ServiceResource": "",
#     "Service_Offering_Request__c": "",
#     "WorkOrder": ""
# }

OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Retired','Dealer') AND IsPersonAccount = false"
}

def strip_html_tags(text):
    """Remove HTML tags from a string"""
    if not text:
        return text
    return re.sub(r"<[^>]+>", "", text)

def related_recordid_mapping(sf_source,sf_target,records):
    doc_ids = set()

    for rec in records:
        body = rec.get("Body") or ""
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        for doc_id in matches:
            doc_ids.add(doc_id)

    if not doc_ids:
        print("⚠️ No <img> tags found in FeedItem bodies, skipping RelatedRecordId mapping")
        return records  # return unchanged
    
    # Step 2: Fetch latest ContentVersion for all unique ContentDocumentIds
    content_map = {}  # {ContentDocumentId: ContentVersionId}

    if doc_ids:
        ids_str = ",".join([f"'{d}'" for d in doc_ids])
        ver_soql = f"""
            SELECT ContentDocumentId, Id
            FROM ContentVersion
            WHERE ContentDocumentId IN ({ids_str}) AND IsLatest = true
        """
        ver_q = sf_source.query_all(ver_soql)["records"]
        
        for v in ver_q:
            content_map[v["ContentDocumentId"]] = v["Id"]

    # Step 3: Update each record with RelatedRecordId if image found
    for rec in records:
        body = rec.get("Body") or ""
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        if matches:
            doc_id = matches[0]  # pick first if multiple
            if doc_id in content_map:
                rec["RelatedRecordId"] = content_map[doc_id]
                print(f"🔗 Mapped FeedItem {rec}")
    return records

def fetch_filtered_feeditems(sf_source,sf_target, obj_name, condition):
    """Fetch FeedItems for a specific object with optional condition on parent"""
    soql = f"SELECT Id FROM {obj_name}"
    if condition:
        soql += f" WHERE {condition}"

    try:
        parent_records = sf_source.query_all(soql)["records"]
        parent_ids = {p["Id"] for p in parent_records}
        if not parent_ids:
            return []

        # 🔹 SOQL IN clause has 2000 limit, so chunk
        feeditems = []
        parent_list = list(parent_ids)
        print(f"Fetching FeedItems for {obj_name} with {len(parent_list)} parent IDs")
        for i in range(0, len(parent_list), 2000):
            chunk = parent_list[i:i+2000]
            ids_str = ",".join([f"'{pid}'" for pid in chunk])
            query = f"""
                SELECT Id, ParentId, Body, LinkUrl, Type, RelatedRecordId
                FROM FeedItem
                WHERE CreatedDate = TODAY AND ParentId IN ({ids_str})
            """
            records = sf_source.query_all(query)["records"]
            records = related_recordid_mapping(sf_source,sf_target,records)
            feeditems.extend(records)

        print(f"Fetched {len(feeditems)} FeedItems for {obj_name}")
        logging.info(f"Fetched {len(feeditems)} FeedItems for {obj_name}")
        return feeditems

    except Exception as e:
        logging.error(f"Error fetching FeedItems for {obj_name}: {e}")
        return []


def migrate_feeditems(sf_source, sf_target):
    """Main migration loop"""
    all_feeditems = []
    all_mappings = {}
    relatedId_mappings = {}

    for obj_name, condition in OBJECT_CONDITIONS.items():
        logging.info(f"Processing object: {obj_name}")

        # Step 1: Fetch FeedItems for this object
        relevant_feeditems = fetch_filtered_feeditems(sf_source,sf_target, obj_name, condition)
        if not relevant_feeditems:
            continue

        all_feeditems.extend(relevant_feeditems)

        # Step 2: Collect parent IDs
        parent_ids = {fi["ParentId"] for fi in relevant_feeditems}

        # Step 3: Build mapping for this object
        target_mapping = fetch_target_mappings(sf_target, obj_name, parent_ids, BATCH_SIZE)
        all_mappings.update(target_mapping)
        
        related_ids = {fi["RelatedRecordId"] for fi in relevant_feeditems}
        relatedId_mappings = fetch_target_mappings(sf_target, "ContentVersion", related_ids, BATCH_SIZE)
        

    logging.info(f"Total FeedItems collected: {len(all_feeditems)}")
    logging.info(f"Total ParentId mappings built: {len(all_mappings)}")

    print(f"Total FeedItems collected: {len(all_feeditems)}")
    print(f"Total ParentId mappings built: {len(all_mappings)}")

    # Step 4: Insert into Target Org
    results = []
    for i in range(0, len(all_feeditems), BATCH_SIZE):
        chunk = all_feeditems[i:i + BATCH_SIZE]
        target_feeditems = []

        for record in chunk:
            src_parent = record.get("ParentId")
            tgt_parent = all_mappings.get(src_parent)

            if not tgt_parent:
                results.append({
                    "Source_FeedItem_Id": record["Id"],
                    "Target_FeedItem_Id": "",
                    "Source_Parent_Id": src_parent,
                    "Target_Parent_Id": "",
                    "Status": "Skipped - No Parent Mapping"
                })
                continue

            # 🔹 Strip HTML tags from body
            clean_body = strip_html_tags(record.get("Body"))

            new_feeditem = {
                "ParentId": tgt_parent,
                "Body": clean_body,
                "LinkUrl": record.get("LinkUrl"),
                "Type": record.get("Type")
            }

            
            # Map RelatedRecordId if available
            if record.get("RelatedRecordId") and record["RelatedRecordId"] in relatedId_mappings:
                new_feeditem["RelatedRecordId"] = relatedId_mappings[record["RelatedRecordId"]]

            target_feeditems.append((record["Id"], src_parent, tgt_parent, new_feeditem))
            print(len(target_feeditems), "FeedItems prepared for insertion")

        if not target_feeditems:
            continue

        try:
            insert_results = sf_target.bulk.FeedItem.insert([fi for *_, fi in target_feeditems])
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
                    results.append({
                        "Source_FeedItem_Id": src_id,
                        "Target_FeedItem_Id": "",
                        "Source_Parent_Id": src_parent,
                        "Target_Parent_Id": tgt_parent,
                        "Status": f"Failed: {ins_res.get('errors')}"
                    })
        except Exception as e:
            for src_id, src_parent, tgt_parent, _ in target_feeditems:
                results.append({
                    "Source_FeedItem_Id": src_id,
                    "Target_FeedItem_Id": "",
                    "Source_Parent_Id": src_parent,
                    "Target_Parent_Id": tgt_parent,
                    "Status": f"Failed: {str(e)}"
                })

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
