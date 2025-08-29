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
from mappings import fetch_target_mappings

# === Files & Directories ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.path.join(BASE_DIR, "files")
os.makedirs(FILES_DIR, exist_ok=True)

INPUT_CSV = os.path.join(FILES_DIR, "feeditem_results.csv")
OUTPUT_CSV = os.path.join(FILES_DIR, "feedcomment_migration_log.csv")
BATCH_SIZE = 200

# === Fetch FeedComments from Source ===
def fetch_feedcomments(sf: Salesforce, feeditem_id: str):
    query = f"""
        SELECT Id, CommentBody, CreatedById, CreatedDate, ParentId, FeedItemId, RelatedRecordId
        FROM FeedComment
        WHERE FeedItemId = '{feeditem_id}'
        ORDER BY CreatedDate ASC
    """
    results = sf.query_all(query)["records"]
    results = related_recordid_mapping(sf, results)
    return results
# remove HTML tags
def clean_html(raw_html: str) -> str:
    """Remove HTML tags and return plain text"""
    if not raw_html:
        return ""
    clean_text = re.sub(r"<.*?>", "", raw_html)   
    return clean_text.strip()

def related_recordid_mapping(sf_source,records):
    doc_ids = set()

    for rec in records:
        body = rec.get("CommentBody") or ""
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
        body = rec.get("CommentBody") or ""
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        if matches:
            doc_id = matches[0]  # pick first if multiple
            if doc_id in content_map:
                rec["RelatedRecordId"] = content_map[doc_id]
                print(f"🔗 Mapped FeedItem {rec}")
    return records


# === Insert FeedComments into Target ===
def insert_feedcomments(sf: Salesforce, comments, target_feeditem_id):
    inserted_ids = []
    print(f"Inserting {(comments)} comments into FeedItem {target_feeditem_id}")
    related_ids = {fi["RelatedRecordId"] for fi in comments}
    relatedId_mappings = fetch_target_mappings(sf_target, "ContentVersion", related_ids, BATCH_SIZE)
    print(f"RelatedId mappings in comments: {(relatedId_mappings)}")
    print(f"Related Ids in comments: {(related_ids)}")
    for c in comments:
        try:
            print(f"Inserting comment {c['RelatedRecordId']}")
            body_plain = clean_html(c.get("CommentBody", ""))
            if c["RelatedRecordId"] in relatedId_mappings:
                 new_comment = {
                    "FeedItemId": target_feeditem_id,
                    "CommentBody": body_plain,
                    "RelatedRecordId": relatedId_mappings[c["RelatedRecordId"]]
                }    
            else:
                new_comment = {
                    "FeedItemId": target_feeditem_id,
                    "CommentBody": body_plain
                    # ParentId auto-derived, not set here
                }
            res = sf.FeedComment.create(new_comment)
            if res.get("success"):
                inserted_ids.append((c["Id"], res.get("id"), "Success"))
            else:
                inserted_ids.append((c["Id"], None, f"Failed: {res}"))
        except Exception as e:
            inserted_ids.append((c["Id"], None, f"Error: {str(e)}"))
    return inserted_ids

# === Main Migration Process ===
def migrate_feedcomments(sf_source, sf_target):
    results = []

    with open(INPUT_CSV, "r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            source_feeditem = row["Source_FeedItem_Id"]
            target_feeditem = row["Target_FeedItem_Id"]
            source_parent = row.get("Source_Parent_Id", "")
            target_parent = row.get("Target_Parent_Id", "")

            print(f"[INFO] Migrating comments for Source FeedItem {source_feeditem} -> Target FeedItem {target_feeditem}")

            comments = fetch_feedcomments(sf_source, source_feeditem)
            if not comments:
                print(f"[INFO] No comments found for {source_feeditem}")
                continue

            inserted = insert_feedcomments(sf_target, comments, target_feeditem)

            for src_id, tgt_id, status in inserted:
                results.append({
                    "Source_FeedComment_Id": src_id,
                    "Target_FeedComment_Id": tgt_id,
                    "Source_FeedItem_Id": source_feeditem,
                    "Target_FeedItem_Id": target_feeditem,
                    "Source_Parent_Id": source_parent,
                    "Target_Parent_Id": target_parent,
                    "Status": status
                })

    # === Write results to CSV ===
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=[
            "Source_FeedComment_Id",
            "Target_FeedComment_Id",
            "Source_FeedItem_Id",
            "Target_FeedItem_Id",
            "Source_Parent_Id",
            "Target_Parent_Id",
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
