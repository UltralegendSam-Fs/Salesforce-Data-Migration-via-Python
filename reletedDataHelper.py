#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import logging
import re
import os
import pandas as pd
from datetime import datetime
from typing import Dict, List
from utils.mappings import fetch_createdByIds, build_owner_mapping, related_recordid_mapping, fetch_target_mappings, FILES_DIR
from utils.retry_utils import safe_query
 


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



def process_body(body: str) -> str:
    """
    Process FeedItem or FeedComment body:
    - Replace sfdc://<ContentVersionId> with mapped target ID
    - Keep <img> tags if present
    - Strip other HTML if no <img> tags
    """
    if not body:
        return body
    logging.debug(f"Processing body content: {body[:100]}..." if len(body) > 100 else f"Processing body content: {body}")
    return re.sub(r"<[^>]+>", "", body)

def _bulk_insert_with_fallback(sf_target, sobject_name: str, records: List[Dict]):
    """
    Try Bulk API insert; if unsupported or fails, gracefully fall back to REST one-by-one.
    Returns a list of dicts with keys: success(bool), id(str or None), errors(list of str).
    """
    results = []
    if not records:
        return results
    logging.info(f"Attempting bulk insert of {len(records)} {sobject_name} records")

    try:
        # Try BULK
        inserted = getattr(sf_target.bulk, sobject_name).insert(records, batch_size=min(len(records), CHUNK_SIZE_API))
        logging.debug(f"Bulk insert completed for {sobject_name}")
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
    Migrate classic Attachments from source activities → target activities with validation and progress tracking.
    Binary download must be done via /sobjects/Attachment/{Id}/Body.
    """
    if not src_activity_ids:
        return

    
    
    total_attachments = 0

    for ids_clause in _soql_in_chunks(src_activity_ids):
        soql = f"SELECT Id, Name, ParentId, CreatedDate, CreatedById, OwnerId, ContentType FROM Attachment WHERE ParentId IN {ids_clause}"
        logging.debug(f"migrate_attachments soql: {soql}")
        attachments = safe_query(sf_source, soql)["records"]
        logging.info(f"migrate_attachments found {len(attachments)} attachments")
        
        if not attachments:
            continue
            
        # No validation: use all attachments
        valid_attachments = attachments
        
        total_attachments += len(valid_attachments)
        
        createdBy_ids = set()
        createdBy_mappings = {}
        owner_ids = set()
        owner_mappings = {}

        # createdBy_ids = {att["CreatedById"] for att in valid_attachments if att.get("CreatedById")}
        # createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)

        owner_ids = {att["OwnerId"] for att in valid_attachments if att.get("OwnerId")}
        owner_mappings = build_owner_mapping(sf_source, sf_target, owner_ids)
        logging.info(f"[Attachments] Found {len(valid_attachments)} for {len(ids_clause)} activities chunk")
        
        # Process attachments with progress tracking
        for att in valid_attachments:
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
                error_msg = f"Attachment migration failed: {str(e)}"
                logging.error(f"Failed to migrate attachment {att['Id']}: {e}")
                results_out.append({
                    "Type": "Attachment",
                    "SourceActivityId": src_parent,
                    "TargetActivityId": tgt_parent,
                    "SourceRecordId": att["Id"],
                    "TargetRecordId": None,
                    "Status": "Failed",
                    "Error": error_msg
                })

    logging.info(f"[INFO] Total Attachments processed: {total_attachments}")

# ---------------------------
# Migration: Files (ContentDocument / ContentVersion / ContentDocumentLink)
# ---------------------------

# ---------------------------
# Migration: Files (ContentDocumentLink only, no ContentVersion upload)
# ---------------------------

def reset_file_migration_cache():
    """Reset the persistent cache for file migration. Call this at the start of a new migration run."""
    if hasattr(migrate_files, 'doc_cache'):
        cache_size = len(migrate_files.doc_cache)
        migrate_files.doc_cache.clear()
        logging.info(f"File migration cache reset (cleared {cache_size} entries)")

def migrate_files(sf_source, sf_target, src_activity_ids: List[str], activity_map: Dict[str, str], results_out: List[Dict], file_map):
    """
    Migrate Salesforce Files (ContentDocument) by copying latest ContentVersion binary to target
    and linking it to the mapped target record. Stores both ContentDocument and ContentVersion IDs.
    """
    if not src_activity_ids:
        logging.warning("No source activity IDs provided, skipping file migration")
        return

    # Use a persistent cache that survives across chunks to prevent duplicates
    if not hasattr(migrate_files, 'doc_cache'):
        migrate_files.doc_cache = {}  # sourceDocId -> targetDocId
    doc_cache = migrate_files.doc_cache

    # First, collect ContentDocumentLinks for activities to find relevant documents
    activity_links = []
    relevant_document_ids = set()
    
    for ids_clause in _soql_in_chunks(src_activity_ids):
        cdl_soql = f"""
            SELECT Id, ContentDocumentId, LinkedEntityId, ShareType,Visibility
            FROM ContentDocumentLink
            WHERE LinkedEntityId IN {ids_clause}
        """
        logging.debug(f"Fetching activity ContentDocumentLinks with query: {cdl_soql}")
        links = safe_query(sf_source, cdl_soql)["records"]
        activity_links.extend(links)
        relevant_document_ids.update([link["ContentDocumentId"] for link in links])
    
    logging.info(f"Found {len(activity_links)} ContentDocumentLinks for activities")
    logging.info(f"Found {len(relevant_document_ids)} unique ContentDocuments to migrate")
    
    if not relevant_document_ids:
        return
    
    # Now fetch ALL ContentDocumentLinks for these documents (including User shares)
    all_links = []
    relevant_doc_list = list(relevant_document_ids)
    
    for doc_ids_clause in _soql_in_chunks(relevant_doc_list):
        all_cdl_soql = f"""
            SELECT Id, ContentDocumentId, LinkedEntityId, ShareType,Visibility, LinkedEntity.Type
            FROM ContentDocumentLink
            WHERE ContentDocumentId IN {doc_ids_clause}
        """
        logging.debug(f"Fetching ALL ContentDocumentLinks for documents: {all_cdl_soql}")
        all_doc_links = safe_query(sf_source, all_cdl_soql)["records"]
        all_links.extend(all_doc_links)
    
    logging.info(f"Found {len(all_links)} total ContentDocumentLinks (including User shares)")
    
    if not all_links:
        return
    
    # Group ContentDocumentLinks by ContentDocumentId
    links_by_document = {}
    for link in all_links:
        doc_id = link["ContentDocumentId"]
        if doc_id not in links_by_document:
            links_by_document[doc_id] = []
        links_by_document[doc_id].append(link)
    
    logging.info(f"Processing {len(links_by_document)} unique ContentDocuments")
    
    # Pre-fetch ALL User mappings for efficiency
    all_user_ids = set()
    for document_links in links_by_document.values():
        for link in document_links:
            linked_entity_type = link.get("LinkedEntity", {}).get("Type", "Unknown")
            if linked_entity_type == "User":
                all_user_ids.add(link["LinkedEntityId"])
    
    global_user_mappings = {}
    if all_user_ids:
        logging.info(f"Fetching target mappings for {len(all_user_ids)} unique Users across all documents")
        global_user_mappings = fetch_target_mappings(sf_target, "User", all_user_ids, 200)
        logging.info(f"Successfully mapped {len(global_user_mappings)}/{len(all_user_ids)} Users")
    
    # Process each ContentDocument once, then create multiple links
    for src_doc_id, document_links in links_by_document.items():
        logging.debug(f"Processing ContentDocument {src_doc_id} with {len(document_links)} links")
        
        # Check if we already have this document migrated
        tgt_doc_id = None
        tgt_ver_id = None
        src_ver_id = None
        
        if src_doc_id in doc_cache:
            # Document already migrated, reuse it
            cached_info = doc_cache[src_doc_id]
            tgt_doc_id = cached_info["doc_id"]
            tgt_ver_id = cached_info["ver_id"]
            src_ver_id = cached_info["src_ver_id"]
            logging.debug(f"Reusing cached ContentDocument {src_doc_id} -> {tgt_doc_id}")
            
            # Update file_map if not already there
            if src_ver_id not in file_map:
                file_map[src_ver_id] = tgt_ver_id
                
        else:
            # Need to migrate this document
            try:
                # Get latest ContentVersion
                ver_soql = f"""
                    SELECT Id, Title, PathOnClient, VersionData
                    FROM ContentVersion
                    WHERE ContentDocumentId = '{src_doc_id}' AND IsLatest = true
                    LIMIT 1
                """
                ver_q = safe_query(sf_source, ver_soql)["records"]
                if not ver_q:
                    # No ContentVersion found - mark all links as failed
                    for link in document_links:
                        src_parent = link["LinkedEntityId"]
                        tgt_parent = activity_map.get(src_parent)
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
                logging.info(f"Migrating new ContentDocument: Title={ver.get('Title')}, SourceVersionId={src_ver_id}")

                # Find the first valid target parent for FirstPublishLocationId
                first_target_parent = None
                for link in document_links:
                    tgt_parent = activity_map.get(link["LinkedEntityId"])
                    if tgt_parent:
                        first_target_parent = tgt_parent
                        break
                
                if not first_target_parent:
                    # No valid target parents - mark all links as failed
                    for link in document_links:
                        src_parent = link["LinkedEntityId"]
                        results_out.append({
                            "Type": "File",
                            "SourceActivityId": src_parent,
                            "TargetActivityId": None,
                            "SourceDocumentId": src_doc_id,
                            "TargetDocumentId": None,
                            "SourceVersionId": src_ver_id,
                            "TargetVersionId": None,
                            "Status": "Failed",
                            "Error": "No target parent mapping"
                        })
                    continue

                # Download file binary
                instance_url = f"https://{sf_source.sf_instance}"
                download_url = f"{instance_url}{ver['VersionData']}"
                resp = sf_source.session.get(download_url, headers=sf_source.headers)
                resp.raise_for_status()
                file_bytes = resp.content

                # Upload into target org (creates ContentDocument automatically)
                cv_payload = {
                    "Title": ver.get("Title") or "File",
                    "PathOnClient": ver.get("PathOnClient") or "file.bin",
                    "VersionData": base64.b64encode(file_bytes).decode("utf-8"),
                    "FirstPublishLocationId": first_target_parent,
                    "Card_Legacy_Id__c": src_ver_id 
                }
                cv_create = sf_target.ContentVersion.create(cv_payload)
                tgt_ver_id = cv_create.get("id")
                logging.info(f"Created ContentVersion in target: {tgt_ver_id}")

                # Get new ContentDocumentId
                new_cv = sf_target.query(
                    f"SELECT ContentDocumentId FROM ContentVersion WHERE Id = '{tgt_ver_id}'"
                )["records"][0]
                tgt_doc_id = new_cv["ContentDocumentId"]

                # Cache the mapping
                file_map[src_ver_id] = tgt_ver_id
                doc_cache[src_doc_id] = {
                    "doc_id": tgt_doc_id,
                    "ver_id": tgt_ver_id,
                    "src_ver_id": src_ver_id
                }
                logging.info(f"Cached ContentDocument mapping: {src_doc_id} -> {tgt_doc_id}")

            except Exception as e:
                error_msg = f"ContentDocument migration failed: {str(e)}"
                logging.error(f"Failed to migrate ContentDocument {src_doc_id}: {e}")
                # Mark all links for this document as failed
                for link in document_links:
                    src_parent = link["LinkedEntityId"]
                    tgt_parent = activity_map.get(src_parent)
                    results_out.append({
                        "Type": "File",
                        "SourceActivityId": src_parent,
                        "TargetActivityId": tgt_parent,
                        "SourceDocumentId": src_doc_id,
                        "TargetDocumentId": None,
                        "SourceVersionId": src_ver_id,
                        "TargetVersionId": None,
                        "Status": "Failed",
                        "Error": error_msg
                    })
                continue
        
        # Now create ContentDocumentLinks for all valid target parents (Activities + Users)
        links_created = 0
        
        for link in document_links:
            src_parent = link["LinkedEntityId"]
            linked_entity_type = link.get("LinkedEntity", {}).get("Type", "Unknown")
            tgt_parent = None
            error_reason = None
            
            if linked_entity_type in ["Task", "Event","EmailMessage"] or src_parent in activity_map:
                # This is an Activity link
                tgt_parent = activity_map.get(src_parent)
                if not tgt_parent:
                    error_reason = "No target activity mapping"
            elif linked_entity_type == "User":
                # This is a User link - use global pre-fetched mapping
                tgt_parent = global_user_mappings.get(src_parent)
                if not tgt_parent:
                    error_reason = "No target user mapping (User not found or no Card_Legacy_Id__c)"
                else:
                    logging.debug(f"Mapped User: {src_parent} -> {tgt_parent}")
            else:
                # Other entity types (Account, Contact, etc.)
                logging.debug(f"Skipping ContentDocumentLink for unsupported entity type: {linked_entity_type}")
                error_reason = f"Unsupported LinkedEntity type: {linked_entity_type}"
            
            if not tgt_parent:
                results_out.append({
                    "Type": "File",
                    "SourceActivityId": src_parent if linked_entity_type in ["Task", "Event","EmailMessage"] else None,
                    "TargetActivityId": None,
                    "SourceDocumentId": src_doc_id,
                    "TargetDocumentId": tgt_doc_id,
                    "SourceVersionId": src_ver_id,
                    "TargetVersionId": tgt_ver_id,
                    "Status": "Failed",
                    "Error": error_reason,
                    "LinkedEntityType": linked_entity_type
                })
                continue
            
            try:
                # Check if link already exists
                existing_link_query = f"""
                    SELECT Id FROM ContentDocumentLink 
                    WHERE ContentDocumentId = '{tgt_doc_id}' 
                    AND LinkedEntityId = '{tgt_parent}'
                    LIMIT 1
                """
                existing_links = safe_query(sf_target, existing_link_query)["records"]
                
                if not existing_links:
                    # Create new ContentDocumentLink
                    share_type = link.get("ShareType") or "V"
                    if share_type == "I":
                        share_type = "V"

                    sf_target.ContentDocumentLink.create({
                        "ContentDocumentId": tgt_doc_id,
                        "LinkedEntityId": tgt_parent,
                        "ShareType": share_type,
                        "Visibility": link.get("Visibility") or "AllUsers"
                    })
                    links_created += 1
                    logging.debug(f"Created ContentDocumentLink: {tgt_doc_id} -> {tgt_parent}")
                else:
                    logging.debug(f"ContentDocumentLink already exists: {tgt_doc_id} -> {tgt_parent}")
                
                results_out.append({
                    "Type": "File",
                    "SourceActivityId": src_parent if linked_entity_type in ["Task", "Event","EmailMessage"] else None,
                    "TargetActivityId": tgt_parent if linked_entity_type in ["Task", "Event","EmailMessage"] else None,
                    "SourceDocumentId": src_doc_id,
                    "TargetDocumentId": tgt_doc_id,
                    "SourceVersionId": src_ver_id,
                    "TargetVersionId": tgt_ver_id,
                    "SourceLinkedEntityId": src_parent,
                    "TargetLinkedEntityId": tgt_parent,
                    "LinkedEntityType": linked_entity_type,
                    "Status": "Success",
                    "Error": ""
                })
                
            except Exception as e:
                error_msg = f"ContentDocumentLink creation failed: {str(e)}"
                logging.error(f"Failed to create ContentDocumentLink for {tgt_parent}: {e}")
                results_out.append({
                    "Type": "File",
                    "SourceActivityId": src_parent if linked_entity_type in ["Task", "Event","EmailMessage"] else None,
                    "TargetActivityId": tgt_parent if linked_entity_type in ["Task", "Event","EmailMessage"] else None,
                    "SourceDocumentId": src_doc_id,
                    "TargetDocumentId": tgt_doc_id,
                    "SourceVersionId": src_ver_id,
                    "TargetVersionId": tgt_ver_id,
                    "SourceLinkedEntityId": src_parent,
                    "TargetLinkedEntityId": tgt_parent,
                    "LinkedEntityType": linked_entity_type,
                    "Status": "Failed",
                    "Error": error_msg
                })
        
        # Log summary of link types
        link_types = {}
        for link in document_links:
            entity_type = link.get("LinkedEntity", {}).get("Type", "Unknown")
            link_types[entity_type] = link_types.get(entity_type, 0) + 1
        
        type_summary = ", ".join([f"{count} {etype}" for etype, count in link_types.items()])
        logging.info(f"ContentDocument {src_doc_id}: Created {links_created} new links out of {len(document_links)} total ({type_summary})")


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
    logging.debug(f"Starting feed migration with file_map containing {len(file_map)} entries")

    for ids_clause in _soql_in_chunks(src_activity_ids):
        # 1) Fetch FeedItems for this chunk of parents
        fi_soql = f"""
            SELECT Id, ParentId, Body, LinkUrl, Type, RelatedRecordId,CreatedById, CreatedDate,IsRichText,Visibility,Title
            FROM FeedItem
            WHERE ParentId IN {ids_clause}
        """

        feeditems = safe_query(sf_source, fi_soql)["records"]
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
                if src_related:
                    tgt_related = file_map.get(src_related)
                    if tgt_related:
                        payload["RelatedRecordId"] = tgt_related
                        logging.debug(f"Remapped FeedItem RelatedRecordId: {src_related} -> {tgt_related}")
                    else:
                        logging.debug(f"No mapping found for FeedItem RelatedRecordId: {src_related}")

                to_insert.append(payload)
                logging.debug(f"Prepared FeedItem for insert: ParentId={payload.get('ParentId')}, Body length={len(payload.get('Body', ''))}")

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
            comments = safe_query(sf_source, fc_soql)["records"]
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
                if src_related:
                    tgt_related = file_map.get(src_related)
                    if tgt_related:
                        payload["RelatedRecordId"] = tgt_related
                        logging.debug(f"Remapped FeedComment RelatedRecordId: {src_related} -> {tgt_related}")
                    else:
                        logging.debug(f"No mapping found for FeedComment RelatedRecordId: {src_related}")

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

