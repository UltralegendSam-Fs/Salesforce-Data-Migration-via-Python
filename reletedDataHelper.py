#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import logging
import re
import os
import pandas as pd
from typing import Dict, List
from bs4 import BeautifulSoup
from mappings import fetch_createdByIds, build_owner_mapping, FILES_DIR


# === Tunables / Limits ===
CHUNK_SIZE_API = 200          # Bulk insert batch size (Salesforce limit for sObject collections)
CHUNK_SIZE_ACTIVITIES = 50    # How many activity IDs to process per outer loop (caller can override)
SOQL_IN_LIMIT = 1000          # Salesforce 'IN (...)' list size hard limit


activity_related_migration = os.path.join(FILES_DIR, "activity_related_migration.csv")

# ---------------------------
# Utility helpers
# ---------------------------

def _chunk_list(items: List, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]

def _soql_in_chunks(ids: List[str], limit: int = SOQL_IN_LIMIT):
    """Yield chunks of IDs, each <= SOQL 'IN' list limit."""
    for chunk in _chunk_list(ids, limit):
        yield "('" + "','".join(chunk) + "')"

# def load_migration_maps(excel_path: str):
#     """
#     Reads activity_related_migration.xlsx and returns two maps:
#     1) file_map: { SourceDocumentId: TargetDocumentId }
#     2) version_map: { SourceVersionId: TargetVersionId }
#     Only includes rows where Type='File' and Status='Success'
#     """
#     df = pd.read_csv(excel_path)
    
#     df = df[(df['Type'] == 'File') & (df['Status'] == 'Success')]

#     # Map SourceDocumentId -> TargetDocumentId
#     file_map = dict(zip(df['SourceDocumentId'], df['TargetDocumentId']))

#     # Map SourceVersionId -> TargetVersionId
#     version_map = dict(zip(df['SourceVersionId'], df['TargetVersionId']))

#     return file_map, version_map


def process_body(body: str) -> str:
    """
    Process FeedItem or FeedComment body:
    - Replace sfdc://<ContentVersionId> with mapped target ID
    - Keep <img> tags if present
    - Strip other HTML if no <img> tags
    """
    if not body:
        return body
    print("Original body:", body)
    #return re.body(r"<[^>]+>", "", body)
    return re.sub(r"<[^>]+>", "", body)

def related_recordid_mapping(sf_source,sf_target,records,object_type):
    doc_ids = set()
    createdBy_ids = set()
    createdBy_mappings = {}

    createdBy_ids = {rec["CreatedById"] for rec in records if rec.get("CreatedById")}
    createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)


    for rec in records:
        rec["CreatedById"] = createdBy_mappings.get(rec.get("CreatedById"), None)
        if object_type=="Comment":
            body = rec.get("CommentBody") or ""
        else:
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
        if object_type=="Comment":
            body = rec.get("CommentBody") or ""
            new_body = re.sub(r'<img[^>]*>(?:</img>)?', '', body, flags=re.IGNORECASE)
            rec["CommentBody"] = new_body.strip()

        else:
            body = rec.get("Body") or ""
            new_body = re.sub(r'<img[^>]*>(?:</img>)?', '', body, flags=re.IGNORECASE)
            rec["Body"] = new_body.strip()
            
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        if matches:
            doc_id = matches[0]  # pick first if multiple
            if doc_id in content_map:
                rec["RelatedRecordId"] = content_map[doc_id]
                
                print(f"🔗 Mapped FeedItem {rec}")
    return records


def _bulk_insert_with_fallback(sf_target, sobject_name: str, records: List[Dict]):
    """
    Try Bulk API insert; if unsupported or fails, gracefully fall back to REST one-by-one.
    Returns a list of dicts with keys: success(bool), id(str or None), errors(list of str).
    """
    results = []
    if not records:
        return results
    print(f"Attempting bulk insert of {(records)}")

    try:
        # Try BULK
        inserted = getattr(sf_target.bulk, sobject_name).insert(records, batch_size=min(len(records), CHUNK_SIZE_API))
        print(f"Bulk insert results: {inserted}")
        # Normalize result structure
        for res in inserted:
            results.append({
                "success": res.get("success", False),
                "id": res.get("id"),
                "errors": res.get("errors", []),
            })
        return results
    except Exception as bulk_err:
        logging.warning(f"Bulk insert not available for {sobject_name} or failed. Falling back to REST. Error: {bulk_err}")

    # Fallback to REST (one-by-one)
    for rec in records:
        try:
            res = getattr(sf_target, sobject_name).create(rec)
            results.append({"success": True, "id": res.get("id"), "errors": []})
        except Exception as e:
            results.append({"success": False, "id": None, "errors": [str(e)]})

    return results


# ---------------------------
# Migration: Attachments
# ---------------------------

def migrate_attachments(sf_source, sf_target, src_activity_ids: List[str], activity_map: Dict[str, str], results_out: List[Dict]):
    """
    Migrate classic Attachments from source activities → target activities.
    Binary download must be done via /sobjects/Attachment/{Id}/Body.
    """
    if not src_activity_ids:
        return

    for ids_clause in _soql_in_chunks(src_activity_ids):
        soql = f"SELECT Id, Name, ParentId, CreatedDate, CreatedById, OwnerId, ContentType FROM Attachment WHERE ParentId IN {ids_clause}"
        print("migrate_attachments soql", soql)
        attachments = sf_source.query_all(soql)["records"]
        createdBy_ids = set()
        createdBy_mappings = {}
        owner_ids = set()
        owner_mappings = {}

        # createdBy_ids = {att["CreatedById"] for att in attachments if att.get("CreatedById")}
        # createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)

        owner_ids = {att["OwnerId"] for att in attachments if att.get("OwnerId")}
        owner_mappings = build_owner_mapping(sf_source, sf_target, owner_ids)


        logging.info(f"[Attachments] Found {len(attachments)} for {len(ids_clause)} activities chunk")

        for att in attachments:
            src_parent = att["ParentId"]
            tgt_parent = activity_map.get(src_parent)
            if not tgt_parent:
                results_out.append({
                    "Type": "Attachment",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": None,
                    "SourceRecordId": att["Id"],
                    "TargetRecordId": None,
                    "Status": "Failed",
                    "Error": "No target parent mapping"
                })
                continue

            # Download binary
            try:
                body_url = f"{sf_source.base_url}sobjects/Attachment/{att['Id']}/Body"
                file_bytes = sf_source.session.get(body_url, headers=sf_source.headers).content
                payload = {
                    "Name": att.get("Name"),
                    "ParentId": tgt_parent,
                    "Body": base64.b64encode(file_bytes).decode("utf-8"),
                    "createdDate": att.get("CreatedDate"),
                }
                # optional: include ContentType if present
                if att.get("ContentType"):
                    payload["ContentType"] = att["ContentType"]

                # optional: remap CreatedById and OwnerId if present
                # if att.get("CreatedById") and att["CreatedById"] in createdBy_mappings:
                #     payload["CreatedById"] = createdBy_mappings[att["CreatedById"]]
                if att.get("OwnerId") and att["OwnerId"] in owner_mappings:
                    payload["OwnerId"] = owner_mappings[att["OwnerId"]]


                create_res = sf_target.Attachment.create(payload)
                results_out.append({
                    "Type": "Attachment",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": tgt_parent,
                    "SourceRecordId": att["Id"],
                    "TargetRecordId": create_res.get("id"),
                    "Status": "Success",
                    "Error": ""
                })
            except Exception as e:
                results_out.append({
                    "Type": "Attachment",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": tgt_parent,
                    "SourceRecordId": att["Id"],
                    "TargetRecordId": None,
                    "Status": "Failed",
                    "Error": str(e)
                })


# ---------------------------
# Migration: Files (ContentDocument / ContentVersion / ContentDocumentLink)
# ---------------------------

# ---------------------------
# Migration: Files (ContentDocumentLink only, no ContentVersion upload)
# ---------------------------

def migrate_files(sf_source, sf_target, src_activity_ids: List[str], activity_map: Dict[str, str], results_out: List[Dict], file_map):
    """
    Migrate Salesforce Files (ContentDocument) by copying latest ContentVersion binary to target
    and linking it to the mapped target record. Stores both ContentDocument and ContentVersion IDs.
    """
    if not src_activity_ids:
        print("⚠️ No source activity IDs provided, skipping migration.")
        return

    doc_cache: Dict[str, str] = {}  # sourceDocId -> targetDocId

    for ids_clause in _soql_in_chunks(src_activity_ids):
        cdl_soql = f"""
            SELECT Id, ContentDocumentId, LinkedEntityId, ShareType
            FROM ContentDocumentLink
            WHERE LinkedEntityId IN {ids_clause}
        """
        print("🔍 Fetching ContentDocumentLinks with query:", cdl_soql)
        links = sf_source.query_all(cdl_soql)["records"]
        print(f"✅ Found {len(links)} ContentDocumentLinks")

        if not links:
            continue

        for link in links:
            src_parent = link["LinkedEntityId"]
            tgt_parent = activity_map.get(src_parent)
            src_doc_id = link["ContentDocumentId"]

            print(f"\n📌 Processing link: SourceParent={src_parent}, TargetParent={tgt_parent}, DocId={src_doc_id}")

            if not tgt_parent:
                results_out.append({
                    "Type": "File",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": None,
                    "SourceDocumentId": src_doc_id,
                    "TargetDocumentId": None,
                    "SourceVersionId": None,
                    "TargetVersionId": None,
                    "Status": "Failed",
                    "Error": "No target parent mapping"
                })
                continue

            try:
                # ✅ Reuse already-migrated document
                if src_doc_id in doc_cache:
                    tgt_doc_id = doc_cache[src_doc_id]
                    print(f"🔄 Reusing cached ContentDocument {src_doc_id} -> {tgt_doc_id}")
                    sf_target.ContentDocumentLink.create({
                        "ContentDocumentId": tgt_doc_id,
                        "LinkedEntityId": tgt_parent,
                        "ShareType": link.get("ShareType") or "V"
                    })
                    results_out.append({
                        "Type": "File",
                        "SourceActivityId": src_parent,
                        "TargetActivityId": tgt_parent,
                        "SourceDocumentId": src_doc_id,
                        "TargetDocumentId": tgt_doc_id,
                        "SourceVersionId": None,  # not fetched again
                        "TargetVersionId": None,
                        "Status": "Success",
                        "Error": ""
                    })
                    continue

                # ✅ Get latest ContentVersion
                ver_soql = f"""
                    SELECT Id, Title, PathOnClient, VersionData
                    FROM ContentVersion
                    WHERE ContentDocumentId = '{src_doc_id}' AND IsLatest = true
                    LIMIT 1
                """
                ver_q = sf_source.query_all(ver_soql)["records"]
                if not ver_q:
                    results_out.append({
                        "Type": "File",
                        "SourceActivityId": src_parent,
                        "TargetActivityId": tgt_parent,
                        "SourceDocumentId": src_doc_id,
                        "TargetDocumentId": None,
                        "SourceVersionId": None,
                        "TargetVersionId": None,
                        "Status": "Failed",
                        "Error": "No latest ContentVersion found"
                    })
                    continue

                ver = ver_q[0]
                src_ver_id = ver["Id"]
                print(f"📄 Downloading file: Title={ver.get('Title')}, SourceVersionId={src_ver_id}")

                # ✅ Download file binary
                instance_url = f"https://{sf_source.sf_instance}"
                download_url = f"{instance_url}{ver['VersionData']}"
                resp = sf_source.session.get(download_url, headers=sf_source.headers)
                resp.raise_for_status()
                file_bytes = resp.content

                # ✅ Upload into target org
                cv_payload = {
                    "Title": ver.get("Title") or "File",
                    "PathOnClient": ver.get("PathOnClient") or "file.bin",
                    "VersionData": base64.b64encode(file_bytes).decode("utf-8"),
                    "FirstPublishLocationId": tgt_parent,
                    "Card_Legacy_Id__c": src_ver_id 
                }
                cv_create = sf_target.ContentVersion.create(cv_payload)
                tgt_ver_id = cv_create.get("id")
                print(f"✅ Created ContentVersion in target: {tgt_ver_id}")

                # ✅ Get new ContentDocumentId
                new_cv = sf_target.query(
                    f"SELECT ContentDocumentId FROM ContentVersion WHERE Id = '{tgt_ver_id}'"
                )["records"][0]
                tgt_doc_id = new_cv["ContentDocumentId"]

                # cache mapping
                file_map[src_ver_id] = tgt_ver_id
                doc_cache[src_doc_id] = tgt_doc_id

                results_out.append({
                    "Type": "File",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": tgt_parent,
                    "SourceDocumentId": src_doc_id,
                    "TargetDocumentId": tgt_doc_id,
                    "SourceVersionId": src_ver_id,
                    "TargetVersionId": tgt_ver_id,
                    "Status": "Success",
                    "Error": ""
                })

            except Exception as e:
                results_out.append({
                    "Type": "File",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": tgt_parent,
                    "SourceDocumentId": src_doc_id,
                    "TargetDocumentId": None,
                    "SourceVersionId": None,
                    "TargetVersionId": None,
                    "Status": "Failed",
                    "Error": str(e)
                })


# ---------------------------
# Migration: Feed (FeedItem / FeedComment)
# ---------------------------

def migrate_feed(sf_source, sf_target, src_activity_ids: List[str], activity_map: Dict[str, str], results_out: List[Dict], file_map):
    """
    Migrate Chatter posts (FeedItem) and comments (FeedComment).
    - Fetch FeedItems for all src parents in IN-chunks.
    - Insert FeedItems in batches (bulk if supported; else REST fallback).
    - Build source→target FeedItem map per batch.
    - Fetch and insert related FeedComments with correct target FeedItemId.
    """
    if not src_activity_ids:
        return
    print("file_map in migrate_feed", file_map)

    for ids_clause in _soql_in_chunks(src_activity_ids):
        # 1) Fetch FeedItems for this chunk of parents
        fi_soql = f"""
            SELECT Id, ParentId, Body, LinkUrl, Type, RelatedRecordId,CreatedById, CreatedDate,IsRichText,Visibility,Title
            FROM FeedItem
            WHERE ParentId IN {ids_clause}
        """

        feeditems = sf_source.query_all(fi_soql)["records"]
        if not feeditems:
            continue
        feeditems = related_recordid_mapping(sf_source,sf_target, feeditems,"Item")

        # Group FeedItems by Parent so we can map to correct tgt ParentId
        feeditems_by_parent = {}
        for fi in feeditems:
            feeditems_by_parent.setdefault(fi["ParentId"], []).append(fi)

        # 2) Insert FeedItems per parent, keeping a map SourceFI → TargetFI
        source_to_target_fi = {}  # {sourceFI: targetFI}

        for src_parent, fi_list in feeditems_by_parent.items():
            tgt_parent = activity_map.get(src_parent)
            if not tgt_parent:
                for fi in fi_list:
                    results_out.append({
                        "Type": "FeedItem",
                        "SourceActivityId": src_parent,
                        "TargetActivityId": None,
                        "SourceRecordId": fi["Id"],
                        "TargetRecordId": None,
                        "Status": "Failed",
                        "Error": "No target parent mapping"
                    })
                continue

            # Build payloads
            to_insert = []
            for fi in fi_list:
                feed_body =fi.get("Body") or ""
                payload = {
                    "ParentId": tgt_parent,
                    "Body":feed_body,
                    "createdDate": fi.get("CreatedDate"),
                    "CreatedById": fi.get("CreatedById"),
                    "IsRichText": fi.get("IsRichText"),
                    "Visibility": fi.get("Visibility"),
                    "Title": fi.get("Title"),
                    "LinkUrl": fi.get("LinkUrl"),
                }

                # ✅ remap RelatedRecordId if it points to a migrated file
                src_related = fi.get("RelatedRecordId")
                print("RelatedRecordId in FeedItem", src_related)
                if src_related:
                    tgt_related = file_map.get(src_related)
                    print(f"RelatedRecordId remap: {src_related} -> {tgt_related}")
                    if tgt_related:
                        payload["RelatedRecordId"] = tgt_related

                to_insert.append(payload)
                print(f"Prepared FeedItem for insert: {payload}")

            # Insert in CHUNK_SIZE_API batches; build mapping in-order
            for batch in _chunk_list(to_insert, CHUNK_SIZE_API):
                start_index = len(to_insert) - (len(to_insert) - to_insert.index(batch[0]))  # not reliable; avoid index()

                # safer: slice the corresponding fi_list portion
                batch_size = len(batch)
                batch_fis = fi_list[:batch_size]
                fi_list = fi_list[batch_size:]

                insert_results = _bulk_insert_with_fallback(sf_target, "FeedItem", batch)

                for fi_src, resp in zip(batch_fis, insert_results):
                    if resp["success"]:
                        source_to_target_fi[fi_src["Id"]] = resp["id"]
                        results_out.append({
                            "Type": "FeedItem",
                            "SourceActivityId": src_parent,
                            "TargetActivityId": tgt_parent,
                            "SourceRecordId": fi_src["Id"],
                            "TargetRecordId": resp["id"],
                            "Status": "Success",
                            "Error": ""
                        })
                    else:
                        results_out.append({
                            "Type": "FeedItem",
                            "SourceActivityId": src_parent,
                            "TargetActivityId": tgt_parent,
                            "SourceRecordId": fi_src["Id"],
                            "TargetRecordId": None,
                            "Status": "Failed",
                            "Error": "; ".join(
                                [err["message"] if isinstance(err, dict) and "message" in err else str(err)
                                for err in resp.get("errors", [])]
                            )
                        })

        # 3) Fetch & insert comments for all successfully-migrated FeedItems
        if not source_to_target_fi:
            continue

        src_feed_ids = list(source_to_target_fi.keys())
        for fi_ids_clause in _soql_in_chunks(src_feed_ids):
            fc_soql = f"""
                SELECT Id, FeedItemId, CommentBody, RelatedRecordId, CreatedDate, CreatedById,IsRichText
                FROM FeedComment
                WHERE FeedItemId IN {fi_ids_clause}
            """
            comments = sf_source.query_all(fc_soql)["records"]
            if not comments:
                continue
            comments = related_recordid_mapping(sf_source,sf_target, comments,"Comment")

            # Build payloads with mapped target FeedItemIds
            to_insert_comments = []
            comment_src_order = []  # keep source comment order to align with results

            for c in comments:
                tgt_fi = source_to_target_fi.get(c["FeedItemId"])
                if not tgt_fi:
                    # Skip comments whose parent feed failed to migrate
                    results_out.append({
                        "Type": "FeedComment",
                        "SourceActivityId": None,
                        "TargetActivityId": None,
                        "SourceRecordId": c["Id"],
                        "TargetRecordId": None,
                        "Status": "Failed",
                        "Error": "Parent FeedItem not migrated"
                    })
                    continue
                comment_body = c.get("CommentBody") or ""
                payload = {
                    "FeedItemId": tgt_fi,
                    "CommentBody": comment_body,
                    "createdDate": c.get("CreatedDate"),
                    "CreatedById": c.get("CreatedById"),
                    "IsRichText": c.get("IsRichText"),
                }

                # ✅ remap RelatedRecordId if it points to a migrated file
                src_related = c.get("RelatedRecordId")
                print("RelatedRecordId in FeedComment", src_related)
                if src_related:
                    tgt_related = file_map.get(src_related)
                    print(f"RelatedRecordId remap (FeedComment): {src_related} -> {tgt_related}")
                    if tgt_related:
                        payload["RelatedRecordId"] = tgt_related

                to_insert_comments.append(payload)
                comment_src_order.append(c)

            # Insert comments in batches with fallback
            for batch, src_batch in zip(_chunk_list(to_insert_comments, CHUNK_SIZE_API),
                                        _chunk_list(comment_src_order, CHUNK_SIZE_API)):
                insert_results = _bulk_insert_with_fallback(sf_target, "FeedComment", batch)
                for c_src, resp in zip(src_batch, insert_results):
                    if resp["success"]:
                        results_out.append({
                            "Type": "FeedComment",
                            "SourceActivityId": None,   # optional to fill, we don't have ParentId directly
                            "TargetActivityId": None,
                            "SourceRecordId": c_src["Id"],
                            "TargetRecordId": resp["id"],
                            "Status": "Success",
                            "Error": ""
                        })
                    else:
                        results_out.append({
                            "Type": "FeedComment",
                            "SourceActivityId": None,
                            "TargetActivityId": None,
                            "SourceRecordId": c_src["Id"],
                            "TargetRecordId": None,
                            "Status": "Failed",
                            "Error": "; ".join(
                                [err["message"] if isinstance(err, dict) and "message" in err else str(err)
                                for err in resp.get("errors", [])]
                            )
                        })

