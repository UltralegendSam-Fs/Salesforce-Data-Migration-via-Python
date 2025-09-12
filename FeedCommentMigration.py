#!/usr/bin/env python3
"""
feedcomment_migration.py

Purpose:
  - Read FeedItem mapping CSV (with Source & Target FeedItem IDs + Parent IDs)
  - Fetch FeedComments from SOURCE Salesforce org
  - Insert them into TARGET Salesforce org
  - Create log CSV with Source & Target FeedComment IDs (with Parent IDs)
"""

import csv
import os
import re
from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from mappings import fetch_target_mappings, fetch_createdByIds, FILES_DIR

BATCH_SIZE = 200

INPUT_CSV = os.path.join(FILES_DIR, "feeditem_results.csv")
OUTPUT_CSV = os.path.join(FILES_DIR, "feedcomment_migration_log.csv")

# === Fetch FeedComments from Source ===
def fetch_feedcomments(sf_source: Salesforce, feeditem_ids: set[str]):
    if not feeditem_ids:
        return []
    ids_str = ",".join([f"'{fid}'" for fid in feeditem_ids])
    query = f"""
        SELECT Id, CommentBody, CreatedById, CreatedDate, ParentId, FeedItemId, RelatedRecordId,IsRichText,CommentType
        FROM FeedComment
        WHERE FeedItemId IN ({ids_str})
        ORDER BY CreatedDate ASC
    """
    results = sf_source.query_all(query)["records"]
    print(f"[INFO] Fetched {len(results)} FeedComments from source org")
    results = related_recordid_mapping(sf_source, results)  # pass only sf + records
    return results

def related_recordid_mapping(sf_source,records):
    doc_ids = set()

    for rec in records:
        body = rec.get("CommentBody") or ""
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        for doc_id in matches:
            doc_ids.add(doc_id)

    if not doc_ids:
        print("⚠️ No <img> tags found in FeedComment bodies, skipping RelatedRecordId mapping")
        return records  # return unchanged
    else:
        print(f"[INFO] Found {len(doc_ids)} unique ContentDocumentIds in FeedComment bodies")
    
    
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
        body = rec.get("CommentBody") or ""
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        if matches:
            doc_id = matches[0]  # pick first if multiple
            if doc_id in content_map:
                rec["RelatedRecordId"] = content_map[doc_id]
                body = re.sub(r'<img[^>]*>(?:</img>)?', '', body, flags=re.IGNORECASE)
                rec["CommentBody"] = body.strip()
    return records

def insert_feedcomments(sf: Salesforce, comments, feeditem_mapping):
    inserted_ids = []
    records_to_insert = []
    record_pairs = []  # [(source_comment, new_comment)]

    related_ids = {c["RelatedRecordId"] for c in comments if c.get("RelatedRecordId")}
    relatedId_mappings = fetch_target_mappings(sf, "ContentVersion", related_ids, BATCH_SIZE)

    createdBy_Ids = {c["CreatedById"] for c in comments}
    createdBy_mappings = fetch_createdByIds(sf, createdBy_Ids)

    for c in comments:
        try:
            tgt_createdBy = createdBy_mappings.get(c["CreatedById"])
            tgt_feeditem = feeditem_mapping.get(c["FeedItemId"])

            if not tgt_feeditem or not tgt_createdBy:
                print(f"[WARN] Skipping FeedComment {c['Id']} - Missing FeedItem or CreatedBy mapping")
                inserted_ids.append((c["Id"], None, c["FeedItemId"], tgt_feeditem, "Skipped - Missing mapping"))
                continue

            new_comment = {
                "FeedItemId": tgt_feeditem,
                "CommentBody": c.get("CommentBody", ""),
                "CreatedById": tgt_createdBy,
                "IsRichText": c.get("IsRichText"),
                "CreatedDate": c["CreatedDate"]
            }
            if c.get("RelatedRecordId") in relatedId_mappings:
                new_comment["RelatedRecordId"] = relatedId_mappings[c["RelatedRecordId"]]

            record_pairs.append((c, new_comment))
            records_to_insert.append(new_comment)
            
        except Exception as e:
            inserted_ids.append((c["Id"], None, c["FeedItemId"], None, f"Prep Error: {str(e)}"))

    # Insert in chunks
    print(f"[INFO] Prepared FeedComment {len(records_to_insert)} for insertion")
    for i in range(0, len(records_to_insert), BATCH_SIZE):
        batch = records_to_insert[i:i+BATCH_SIZE]
        src_batch = [p[0] for p in record_pairs[i:i+BATCH_SIZE]]
        results = sf.bulk.FeedComment.insert(batch)
        

        for c, res in zip(src_batch, results):
            if res.get("success"):
                print(f"[INFO] Inserted FeedComment {c['Id']} as {res.get('id')}")
                inserted_ids.append((c["Id"], res.get("id"), c["FeedItemId"], feeditem_mapping.get(c["FeedItemId"]), "Success"))
            else:
                msg = f"Failed: {res.get('errors')}"
                print(f"[ERROR] FeedComment {c['Id']} failed → {msg}")
                inserted_ids.append((c["Id"], None, c["FeedItemId"], feeditem_mapping.get(c["FeedItemId"]), msg))

    print(f"[INFO] Inserted {len(inserted_ids)} FeedComments into target org")
    return inserted_ids

# === Main Migration Process ===
def migrate_feedcomments(sf_source, sf_target):
    results = []
    feeditem_mapping = {}
    src_ids = set()
    tgt_ids = set()

    with open(INPUT_CSV, "r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            feeditem_mapping[row["Source_FeedItem_Id"]] = row["Target_FeedItem_Id"]

    print(f"[INFO] Found {len(feeditem_mapping)} FeedItems in mapping CSV")

    comments = fetch_feedcomments(sf_source, set(feeditem_mapping.keys()))
    if not comments:
        print(f"[INFO] No comments found")
        return

    inserted = insert_feedcomments(sf_target, comments, feeditem_mapping)

    for src_id, tgt_id, src_feeditem, tgt_feeditem, status in inserted:
        results.append({
            "Source_FeedComment_Id": src_id,
            "Target_FeedComment_Id": tgt_id,
            "Source_FeedItem_Id": src_feeditem,
            "Target_FeedItem_Id": tgt_feeditem,
            "Status": status
        })

    # === Write results to CSV ===
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=[
            "Source_FeedComment_Id",
            "Target_FeedComment_Id",
            "Source_FeedItem_Id",
            "Target_FeedItem_Id",
            "Status"
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"[INFO] Migration completed. Log saved in {OUTPUT_CSV}")


# === Run Script ===
if __name__ == "__main__":
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)
    migrate_feedcomments(sf_source, sf_target)
