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
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from mappings import fetch_target_mappings, fetch_createdByIds,fetch_service_appointment_ids,FILES_DIR

BATCH_SIZE = 200

RESULT_FILE = os.path.join(FILES_DIR, "feeditem_results.csv")
LOG_FILE = os.path.join(FILES_DIR, "feeditem_migration.log")

# === Logging Setup ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# === Object Conditions ===
# OBJECT_CONDITIONS = {
#     "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false",
#     "Asset": "Account.RecordType.Name IN ('Parent Company','Brand','Dealer') AND RecordType.Name = 'Brand Program'",
#     "BYO_Enhanced_Field_History__c": "",
#     "Capability_Enablement__c": "",
#     "Credit_Card_Program_Benefits__c": "Account__r.RecordType.Name IN ('Parent Company','Brand','Dealer')",
#     "FSL__GanttPalette__c": "",
#     "Impact_Tracker__c": "Clients_Brands__c!=null and Clients_Brands__r.RecordType.name in ('Parent Company','Brand','Dealer')",
#     "Order": " RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive')",
#     "Product2": "RecordType.Name = 'Brand Product'",
#     "Request_Brand_Division__c": "Brand__r.RecordType.Name IN ('Parent Company','Brand','Dealer') AND Division__r.RecordType.Name = 'Brand Program' AND Request__c != null",
#     "Request__c": "Brand__r.RecordType.name in ('Parent Company','Brand','Dealer') and Division__r.RecordType.name ='Brand Program')",
#     "Risk_Exception__c": "",
#     "ServiceAppointment": "",
#     "ServiceResource": "IsActive =true",
#     "WorkOrder": "Field_Win_Win__c IN (select id from order where  RecordType.name in ('Field Win Win','Gift Card Procurement','Incentive'))"
# }

OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false"
}

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
                body = re.sub(r'<img[^>]*>(?:</img>)?', '', body, flags=re.IGNORECASE)
                rec["Body"] = body.strip()
    return records

def fetch_filtered_feeditems(sf_source, sf_target, obj_name, condition):
    """Fetch FeedItems first, then filter based on parent records."""
    try:
        # Step 1: Fetch all FeedItems for given object type (Parent.Type)
        soql = f"""
            SELECT Id, ParentId, Body, LinkUrl, Type, RelatedRecordId,
                   CreatedById, CreatedDate, IsRichText, Visibility, Title
            FROM FeedItem
            WHERE  Parent.Type = '{obj_name}'
        """
        feeditems = sf_source.query_all(soql)["records"]
        print(f"Fetched FeedItems for {obj_name}: {len(feeditems)}")

        if not feeditems:
            print(f"⚠️ No FeedItems found for {obj_name}")
            return []

        parent_ids = {fi["ParentId"] for fi in feeditems if fi.get("ParentId")}
        if not parent_ids:
            return []

        # Step 2: Fetch parent records with condition
        parent_list = list(parent_ids)
        valid_parents = set()

        for i in range(0, len(parent_list), 2000):
            ids_str = ",".join([f"'{pid}'" for pid in parent_list[i:i+2000]])
            parent_soql = f"SELECT Id FROM {obj_name} WHERE Id IN ({ids_str})"
            if condition:
                parent_soql += f" AND {condition}"

            parent_records = sf_source.query_all(parent_soql)["records"]
            valid_parents.update({p["Id"] for p in parent_records})

        # Step 3: Filter feeditems whose ParentId is valid
        filtered_feeditems = [fi for fi in feeditems if fi["ParentId"] in valid_parents]

        # Step 4: Map RelatedRecordId if image is in Body
        filtered_feeditems = related_recordid_mapping(sf_source, sf_target, filtered_feeditems)

        print(f"FeedItems retained for {obj_name} after parent filtering: {len(filtered_feeditems)}")
        logging.info(f"{len(filtered_feeditems)} FeedItems retained for {obj_name}")

        return filtered_feeditems

    except Exception as e:
        logging.error(f"Error fetching FeedItems for {obj_name}: {e}")
        return []



def migrate_feeditems(sf_source, sf_target):
    """Main migration loop"""
    all_feeditems = []
    all_mappings = {}
    createdBy_mappings = {}
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

        # Step 4: Build CreatedById mappings
        createdBy_ids = {fi["CreatedById"] for fi in relevant_feeditems}
        createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)

        
        related_ids = {fi["RelatedRecordId"] for fi in relevant_feeditems}
        relatedId_mappings = fetch_target_mappings(sf_target, "ContentVersion", related_ids, BATCH_SIZE)
        

    logging.info(f"Total FeedItems collected: {len(all_feeditems)}")
    logging.info(f"Total ParentId mappings built: {len(all_mappings)}")

    print(f"Total FeedItems collected: {len(all_feeditems)}")

    # Step 4: Insert into Target Org
    results = []
    for i in range(0, len(all_feeditems), BATCH_SIZE):
        chunk = all_feeditems[i:i + BATCH_SIZE]
        target_feeditems = []

        for record in chunk:
            src_parent = record.get("ParentId")
            tgt_parent = all_mappings.get(src_parent)
            tgt_createdBy = createdBy_mappings.get(record.get("CreatedById"))

            feed_body = record.get("Body") or ""

            # 🔹 Skip if no parent mapping OR no valid body
            if not tgt_parent or "status changed to" in feed_body.lower():
                status = "Skipped - No Parent Mapping" if not tgt_parent else "Skipped - Invalid Body"
                results.append({
                    "Source_FeedItem_Id": record["Id"],
                    "Target_FeedItem_Id": "",
                    "Source_Parent_Id": src_parent,
                    "Target_Parent_Id": tgt_parent or "",
                    "Status": status
                })
                continue

            # ✅ Build feed item only if valid
            new_feeditem = {
                "ParentId": tgt_parent,
                "Body": record.get("Body"),
                "LinkUrl": record.get("LinkUrl"),
                "CreatedById": tgt_createdBy,
                "CreatedDate": record.get("CreatedDate"),
                "IsRichText": record.get("IsRichText"),
                "Visibility": record.get("Visibility"),
                "Title": record.get("Title"),
            }

            
            # Map RelatedRecordId if available
            if record.get("RelatedRecordId") and record["RelatedRecordId"] in relatedId_mappings:
                new_feeditem["RelatedRecordId"] = relatedId_mappings[record["RelatedRecordId"]]

            target_feeditems.append((record["Id"], src_parent, tgt_parent, new_feeditem))
        
            
        if not target_feeditems:
            continue

        try:
            print("Total FeedItems for insertion: ",len(target_feeditems))
            insert_results = sf_target.bulk.FeedItem.insert([fi for *_, fi in target_feeditems])
            print(f"Total FeedItems Inserted into Target: {len(insert_results)}")
            for (src_id, src_parent, tgt_parent, _), ins_res in zip(target_feeditems, insert_results):
                if ins_res["success"]:
                    print(f"Success : {ins_res["success"]} >> {ins_res.get("id")} for Source Id: {src_id}")
                    results.append({
                        "Source_FeedItem_Id": src_id,
                        "Target_FeedItem_Id": ins_res.get("id"),
                        "Source_Parent_Id": src_parent,
                        "Target_Parent_Id": tgt_parent,
                        "Status": "Success"
                    })
                else:
                    print(f"errors : {ins_res["errors"]} for Source Id: {src_id}")
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
