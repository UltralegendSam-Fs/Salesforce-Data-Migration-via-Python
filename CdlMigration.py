#!/usr/bin/env python3
"""
contentversion_migration.py

Purpose:
  - Read ContentDocumentId & Target_Parent_Id (and optionally ShareType/Visibility/Source_Parent_Id) from CDL mapping CSV
  - Download latest ContentVersion binary from SOURCE org
  - Upload to TARGET org (once per ContentDocument)
  - Create ContentDocumentLink(s) in TARGET org with SAME ShareType/Visibility as source
  - Output mapping: Old_ContentVersionId, Old_ContentDocumentId, New_ContentVersionId, New_ContentDocumentId, Target_Parent_Id
"""

import os
import csv
import math
import base64
import logging
import re
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from simple_salesforce import Salesforce
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from utils.mappings import FILES_DIR

INPUT_MAPPING_FILE = os.path.join(FILES_DIR, "contentdocumentlink_mapping.csv")
OUTPUT_VERSION_MAPPING_FILE = os.path.join(FILES_DIR, "contentversion_migration_mapping.csv")
LOG_FILE = os.path.join(FILES_DIR, "contentversion_migration.log")

# === Logging configuration ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Runtime config
CHUNK_SIZE = 200
API_VERSION = "v59.0"
MAX_RETRIES = 5
RETRY_SLEEP_SECS = 2
REST_MAX_SIZE_BYTES = 50 * 1024 * 1024  # ~50MB REST limit
MAX_WORKERS = 5  # parallel threads for download/upload

INVALID_FS_CHARS = re.compile(r'[:<>"/\\|?*\x00-\x1F]')

def log_and_print(msg, level="info"):
    print(msg)
    getattr(logging, level)(msg)

# -----------------------------
# Read mapping file
# -----------------------------
def read_mapping_file():
    """
    Returns:
      rows: list of dicts with keys:
        ContentDocumentId, Target_Parent_Id, Source_Parent_Id (if present),
        ShareType (optional), Visibility (optional)
    """
    rows = []
    with open(INPUT_MAPPING_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        for row in reader:
            cdid = row.get("ContentDocumentId", "").strip()
            if not cdid:
                continue
            rows.append({
                "ContentDocumentId": cdid,
                "Target_Parent_Id": row.get("Target_Parent_Id", "").strip() or None,
                "Source_Parent_Id": row.get("Source_Parent_Id", "").strip() if "Source_Parent_Id" in fieldnames else None,
                "ShareType": row.get("ShareType", "").strip() if "ShareType" in fieldnames else None,
                "Visibility": row.get("Visibility", "").strip() if "Visibility" in fieldnames else None,
            })
    return rows

# -----------------------------
# SOQL helpers
# -----------------------------
def soql_list(ids):
    return ",".join(f"'{i}'" for i in ids)

def query_all(sf: Salesforce, soql: str):
    attempts = 0
    while True:
        try:
            return sf.query_all(soql).get("records", [])
        except Exception as e:
            attempts += 1
            if attempts > MAX_RETRIES:
                log_and_print(f"[ERROR] SOQL failed after {MAX_RETRIES} attempts: {e}", "error")
                raise
            wait = RETRY_SLEEP_SECS * (2 ** (attempts - 1))
            log_and_print(f"[WARN] SOQL retry {attempts}/{MAX_RETRIES} in {wait}s: {e}", "warning")
            time.sleep(wait)

# -----------------------------
# Fetch latest ContentVersions
# -----------------------------
def fetch_latest_versions(sf: Salesforce, content_doc_ids):
    """
    Returns dict: {ContentDocumentId: ContentVersion record}
      fields: Id, ContentDocumentId, Title, PathOnClient, ContentSize
    """
    out = {}
    ids = list(set(content_doc_ids))
    for i in range(0, len(ids), CHUNK_SIZE):
        chunk = ids[i:i+CHUNK_SIZE]
        soql = f"""
            SELECT Id, ContentDocumentId, Title, PathOnClient, ContentSize
            FROM ContentVersion
            WHERE IsLatest = true AND ContentDocumentId IN ({soql_list(chunk)})
        """
        for r in query_all(sf, soql):
            out[r["ContentDocumentId"]] = r
    return out

# -----------------------------
# Fetch ShareType/Visibility when missing
# -----------------------------
def fetch_link_meta_bulk(sf: Salesforce, pairs):
    """
    pairs: set of (ContentDocumentId, Source_Parent_Id)
    Returns dict[(cdid, src_parent)] = {"ShareType": "...", "Visibility": "..."}
    """
    out = {}
    # chunk by content doc id set; we need both conditions in where
    pairs = list(pairs)
    for i in range(0, len(pairs), CHUNK_SIZE):
        chunk = pairs[i:i+CHUNK_SIZE]
        cdids = list({cd for cd, _ in chunk})
        parents = list({p for _, p in chunk if p})
        # Query by both lists then filter in Python
        soql = f"""
            SELECT ContentDocumentId, LinkedEntityId, ShareType, Visibility
            FROM ContentDocumentLink
            WHERE ContentDocumentId IN ({soql_list(cdids)})
            {"AND LinkedEntityId IN (" + soql_list(parents) + ")" if parents else ""}
        """
        for r in query_all(sf, soql):
            key = (r["ContentDocumentId"], r["LinkedEntityId"])
            out[key] = {"ShareType": r.get("ShareType") or "V", "Visibility": r.get("Visibility") or "AllUsers"}
    return out

# -----------------------------
# Download & Upload helpers
# -----------------------------
def download_file_as_base64(sf: Salesforce, version_id: str) -> str:
    url = f"https://{sf.sf_instance}/services/data/{API_VERSION}/sobjects/ContentVersion/{version_id}/VersionData"
    resp = sf.session.get(url, headers={'Authorization': 'Bearer ' + sf.session_id}, stream=True, timeout=300)
    resp.raise_for_status()
    return base64.b64encode(resp.content).decode("utf-8")

def sanitize_path(title: str, path_on_client: str | None) -> str:
    candidate = (path_on_client or f"{title}.bin").strip()
    candidate = INVALID_FS_CHARS.sub("_", candidate) or "file.bin"
    return candidate

def create_cdl(sf_target: Salesforce, new_doc_id: str, parent_id: str, share_type: str, visibility: str):
    payload = {
        "ContentDocumentId": new_doc_id,
        "LinkedEntityId": parent_id,
        "ShareType": share_type or "V",
        "Visibility": visibility or "AllUsers",
    }
    sf_target.ContentDocumentLink.create(payload)

# -----------------------------
# Worker for parallel processing
# -----------------------------
def migrate_one_document(sf_source: Salesforce,
                         sf_target: Salesforce,
                         cdid: str,
                         version: dict,
                         rows_for_doc: list[dict],
                         link_meta: dict) -> list[dict]:
    results: list[dict] = []

    if not version:
        log_and_print(f"[WARN] No latest ContentVersion found for ContentDocumentId={cdid}; skipping upload", "warning")
        return results

    old_ver_id = version["Id"]
    old_doc_id = version["ContentDocumentId"]
    title = (version.get("Title") or "file").strip() or "file"
    path_on_client = sanitize_path(title, version.get("PathOnClient"))
    size_bytes = int(version.get("ContentSize") or 0)

    if size_bytes > REST_MAX_SIZE_BYTES:
        log_and_print(
            f"[ERROR] {old_doc_id} latest version {old_ver_id} is {size_bytes} bytes (>50MB). "
            f"Skipping REST upload. Implement multipart/resumable for large files.", "error"
        )
        return results

    try:
        file_b64 = download_file_as_base64(sf_source, old_ver_id)
    except Exception as e:
        log_and_print(f"[ERROR] Failed to download VersionData {old_ver_id}: {e}", "error")
        return results

    new_ver_id = ""
    new_doc_id = ""
    try:
        create_resp = sf_target.ContentVersion.create({
            "Title": title,
            "PathOnClient": path_on_client,
            "VersionData": file_b64,
            "Card_Legacy_Id__c": old_ver_id
        })
        new_ver_id = create_resp["id"]
        new_ver = sf_target.ContentVersion.get(new_ver_id)
        new_doc_id = new_ver["ContentDocumentId"]
        log_and_print(f"✅ Uploaded {old_ver_id} → {new_ver_id} (doc {old_doc_id} → {new_doc_id})")
    except Exception as e:
        log_and_print(f"[ERROR] Failed to create ContentVersion for {old_ver_id}: {e}", "error")
        return results

    # Create links for all requested target parents
    for m in rows_for_doc:
        target_parent_id = m.get("Target_Parent_Id")
        if not target_parent_id:
            results.append({
                "Old_ContentVersionId": old_ver_id,
                "Old_ContentDocumentId": old_doc_id,
                "New_ContentVersionId": new_ver_id or "",
                "New_ContentDocumentId": new_doc_id or "",
                "Target_Parent_Id": ""
            })
            continue

        share_type = m.get("ShareType")
        visibility = m.get("Visibility")

        if not (share_type and visibility):
            src_parent = m.get("Source_Parent_Id")
            meta = link_meta.get((cdid, src_parent), {}) if src_parent else {}
            share_type = share_type or meta.get("ShareType") or "V"
            visibility = visibility or meta.get("Visibility") or "AllUsers"

        try:
            create_cdl(sf_target, new_doc_id, target_parent_id, share_type, visibility)
            log_and_print(f"🔗 Linked {new_doc_id} → {target_parent_id} (ShareType={share_type}, Visibility={visibility})")
        except Exception as e:
            log_and_print(f"[ERROR] Failed linking {new_doc_id} → {target_parent_id}: {e}", "error")

        results.append({
            "Old_ContentVersionId": old_ver_id,
            "Old_ContentDocumentId": old_doc_id,
            "New_ContentVersionId": new_ver_id or "",
            "New_ContentDocumentId": new_doc_id or "",
            "Target_Parent_Id": target_parent_id or ""
        })

    return results

# -----------------------------
# Main migration
# -----------------------------
def migrate_versions(sf_source, sf_target, map_rows):
    results = []

    # Group mappings by ContentDocumentId
    by_doc = defaultdict(list)
    for row in map_rows:
        by_doc[row["ContentDocumentId"]].append(row)

    # Fetch latest versions for unique docs
    latest_by_doc = fetch_latest_versions(sf_source, list(by_doc.keys()))
    log_and_print(f"[INFO] Latest versions fetched for {len(latest_by_doc)}/{len(by_doc)} ContentDocuments")

    # Prepare link meta if missing
    missing_meta_pairs = set()
    for cdid, rows in by_doc.items():
        for r in rows:
            if (not r.get("ShareType") or not r.get("Visibility")) and r.get("Source_Parent_Id"):
                missing_meta_pairs.add((cdid, r["Source_Parent_Id"]))
    link_meta = fetch_link_meta_bulk(sf_source, missing_meta_pairs) if missing_meta_pairs else {}

    # Iterate by chunks of docs (to throttle) and process each chunk in parallel
    doc_ids = list(by_doc.keys())
    for c in range(0, len(doc_ids), CHUNK_SIZE):
        doc_chunk = doc_ids[c:c+CHUNK_SIZE]
        log_and_print(f"[INFO] Processing doc chunk {c//CHUNK_SIZE + 1}/{math.ceil(len(doc_ids)/CHUNK_SIZE)} ({len(doc_chunk)} docs)")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for cdid in doc_chunk:
                version = latest_by_doc.get(cdid)
                futures.append(
                    executor.submit(
                        migrate_one_document,
                        sf_source,
                        sf_target,
                        cdid,
                        version,
                        by_doc[cdid],
                        link_meta,
                    )
                )

            for f in as_completed(futures):
                doc_results = f.result() or []
                if doc_results:
                    results.extend(doc_results)

    return results

def write_mapping(results):
    with open(OUTPUT_VERSION_MAPPING_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Old_ContentVersionId",
                "Old_ContentDocumentId",
                "New_ContentVersionId",
                "New_ContentDocumentId",
                "Target_Parent_Id"
            ]
        )
        writer.writeheader()
        writer.writerows(results)

def main():
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    mappings = read_mapping_file()
    log_and_print(f"[INFO] Found {len(mappings)} mapping rows.")

    results = migrate_versions(sf_source, sf_target, mappings)
    write_mapping(results)

    log_and_print(f"[DONE] Migrated/linked {len(results)} rows.")
    log_and_print(f"[INFO] Output mapping file: {OUTPUT_VERSION_MAPPING_FILE}")

if __name__ == "__main__":
    import time
    main()
