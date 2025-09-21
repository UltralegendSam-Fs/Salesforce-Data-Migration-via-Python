#!/usr/bin/env python3
"""
EmailMessage_migration_safe.py

Safer migration of EmailMessage records from source -> target.
Features:
 - robust fetching without fragile SOQL polymorphic WHERE clause
 - mapping of FromId, RelatedToId, EmailTemplateId
 - skip records with unmapped required parents (and log them)
 - remove read-only system fields before insert
 - chunked bulk inserts with retries
 - detailed CSV outputs for review
"""

import csv
import os
import time
import logging
import pandas as pd
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from mappings import fetch_target_mappings, fetch_createdByIds, fetch_service_appointment_ids, FILES_DIR

EM_export = os.path.join(FILES_DIR, "emailMessage_export.csv")
EM_import = os.path.join(FILES_DIR, "emailMessage_import.csv")
emailtemplate_mapping = os.path.join(FILES_DIR, "emailtemplate_mapping.csv")
log_file = os.path.join(FILES_DIR, "emailmessage_migration.log")

# Logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

# Conditions for validating parents (as in your original)
OBJECT_CONDITIONS = {
    "Impact_Tracker__c": "Clients_Brands__c != null AND Clients_Brands__r.RecordType.Name IN ('Parent Company','Brand','Dealer')",
    "ServiceAppointment": ""  # handled by fetch_service_appointment_ids
}

# Fields that are safe to insert (we'll strip out system/read-only fields)
READ_ONLY_FIELDS = {
    "Id", "CreatedDate", "CreatedById", "LastModifiedDate", "LastModifiedById",
    "SystemModstamp", "IsDeleted", "OwnerId"
}

# Helper to chunk lists
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_em_records(sf):
    """
    Fetch EmailMessage records (last 365 days) that have a RelatedToId.
    We select RelatedTo.Type so we can group and apply conditions locally.
    """
    query = """
        SELECT Id, ParentId, TextBody, HtmlBody, ActivityId, Headers, Subject, FromName, FromAddress,
               ToAddress, CcAddress, BccAddress, Incoming, Status, MessageDate, ReplyToEmailMessageId,
               MessageIdentifier, ThreadIdentifier, ClientThreadIdentifier, FromId, IsClientManaged,
               AttachmentIds, RelatedToId, RelatedTo.Type, IsTracked, FirstOpenedDate, LastOpenedDate,
               IsBounced, EmailTemplateId, EmailRoutingAddressId, AutomationType
        FROM EmailMessage
        WHERE CreatedDate >= LAST_N_DAYS:365
        AND RelatedToId != NULL
    """
    logging.info("[DEBUG] Querying EmailMessage from source ...")
    result = sf.query_all(query)
    emails = result.get("records", [])
    logging.info(f"[DEBUG] Found {len(emails)} EmailMessage records (raw)")
    if not emails:
        logging.warning("No EmailMessages found.")
        return []

    # Build parent map by object type using RelatedTo.Type returned by API
    parent_map = {}
    for e in emails:
        related_to = e.get("RelatedTo") or {}
        obj_type = related_to.get("Type")
        parent_id = e.get("RelatedToId")
        if obj_type and parent_id:
            parent_map.setdefault(obj_type, set()).add(parent_id)

    logging.info(f"[DEBUG] Parent groups discovered: {list(parent_map.keys())}")

    # Validate parents according to conditions
    valid_parent_ids = set()
    for obj_name, ids in parent_map.items():
        if obj_name == "ServiceAppointment":
            # Your helper should accept (sf, sa_ids) or similar; adapt if signature differs
            sa_ids = fetch_service_appointment_ids(sf, sa_ids=ids)
            valid_parent_ids.update(sa_ids)
            logging.info(f"[DEBUG] Valid ServiceAppointment IDs: {len(sa_ids)}")
        elif obj_name in OBJECT_CONDITIONS:
            cond = OBJECT_CONDITIONS[obj_name]
            id_list = list(ids)
            # chunk to avoid too-long IN clause
            for chunk in chunks(id_list, 2000):
                ids_str = ",".join(f"'{i}'" for i in chunk)
                q = f"SELECT Id FROM {obj_name} WHERE Id IN ({ids_str}) AND {cond}"
                res = sf.query_all(q)
                found = [r["Id"] for r in res.get("records", [])]
                valid_parent_ids.update(found)
                logging.info(f"[DEBUG] {obj_name} valid in batch: {len(found)}")
        else:
            # unknown object type: we ignore by default (or add custom logic)
            logging.warning(f"[WARN] No validation rule for parent object: {obj_name} - skipping those parents")

    if not valid_parent_ids:
        logging.warning("No parent records passed validation conditions.")
        return []

    # Filter email messages that have valid parents
    filtered = [e for e in emails if e.get("RelatedToId") in valid_parent_ids]
    logging.info(f"[INFO] EmailMessages after parent validation: {len(filtered)}")
    return filtered

def fetch_target_ids(sf_target, records):
    """
    Map source FromId (User), RelatedToId (Impact_Tracker__c / ServiceAppointment), EmailTemplateId
    onto target IDs using your existing fetch_target_mappings and csv file for templates.
    """
    from_ids = set()
    sa_ids = set()
    it_ids = set()
    template_ids = set()

    for r in records:
        if r.get("FromId"):
            from_ids.add(r["FromId"])
        if r.get("RelatedToId"):
            t = (r.get("RelatedTo") or {}).get("Type")
            if t == "Impact_Tracker__c":
                it_ids.add(r["RelatedToId"])
            elif t == "ServiceAppointment":
                sa_ids.add(r["RelatedToId"])
        if r.get("EmailTemplateId"):
            template_ids.add(r["EmailTemplateId"])

    # map users and parents
    logging.info(f"[DEBUG] Fetching mappings: Users({len(from_ids)}), IT({len(it_ids)}), SA({len(sa_ids)})")
    from_map = fetch_target_mappings(sf_target, "User", from_ids, 200) if from_ids else {}
    it_map = fetch_target_mappings(sf_target, "Impact_Tracker__c", it_ids, 200) if it_ids else {}
    sa_map = fetch_target_mappings(sf_target, "ServiceAppointment", sa_ids, 200) if sa_ids else {}

    # load email template mapping csv (ensure header names exist)
    emailtemplate_mappings = {}
    if os.path.exists(emailtemplate_mapping):
        df = pd.read_csv(emailtemplate_mapping, dtype=str).fillna("")
        # allow both column name variants just in case
        if "SourceTemplateId" in df.columns and "TargetTemplateId" in df.columns:
            emailtemplate_mappings = dict(zip(df["SourceTemplateId"].astype(str), df["TargetTemplateId"].astype(str)))
        elif "SourceTemplate" in df.columns and "TargetTemplate" in df.columns:
            emailtemplate_mappings = dict(zip(df["SourceTemplate"].astype(str), df["TargetTemplate"].astype(str)))
        else:
            logging.warning("Email template mapping CSV does not have expected columns. Template mapping skipped.")
    else:
        logging.warning("Email template mapping CSV not found - templates will not be mapped.")

    # Attach mapping results back to records (keeps original values for audit)
    for r in records:
        r["Target_FromId"] = from_map.get(r.get("FromId"))
        r["Target_RelatedToId"] = (it_map.get(r.get("RelatedToId")) or sa_map.get(r.get("RelatedToId")))
        # cast keys to str for matching CSV keys if necessary
        r["Target_EmailTemplateId"] = emailtemplate_mappings.get(str(r.get("EmailTemplateId"))) if r.get("EmailTemplateId") else None

    return records

def sanitize_for_insert(record):
    """Return a copy of record with read-only/system fields removed."""
    rd = {k: v for k, v in record.items() if k not in READ_ONLY_FIELDS}
    # Remove helper mapping keys we use locally
    rd.pop("Target_FromId", None)
    rd.pop("Target_RelatedToId", None)
    rd.pop("Target_EmailTemplateId", None)
    rd.pop("RelatedTo", None)
    # Don't send AttachmentIds; you must migrate attachments separately.
    rd.pop("AttachmentIds", None)
    # If you want to preserve FromId/RelatedToId/EmailTemplateId set them from mapped values before calling this
    return rd

def export_activity(sf_source, sf_target, object_name="EmailMessage", insert_batch_size=200, max_retries=3):
    # Fetch & map
    em_records = fetch_em_records(sf_source)
    if not em_records:
        logging.info("No EmailMessage records to process.")
        return

    em_records = fetch_target_ids(sf_target, em_records)

    # Export mapping CSV for audit
    with open(EM_export, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        header = ["Source_EM_Id", "Source_RelatedId", "Source_FromId", "Target_FromId", "Target_RelatedToId", "Target_EmailTemplateId"]
        writer.writerow(header)
        for r in em_records:
            writer.writerow([
                r.get("Id"), r.get("RelatedToId", ""), r.get("FromId", ""),
                r.get("Target_FromId") or "", r.get("Target_RelatedToId") or "", r.get("Target_EmailTemplateId") or ""
            ])
    logging.info(f"[INFO] Exported mapping -> {EM_export}")

    # Prepare insert list but skip records with no mapped parent (must have RelatedToId mapped)
    prepared = []
    skipped = []
    for r in em_records:
        if not r.get("Target_RelatedToId"):
            skipped.append((r.get("Id"), "No mapped RelatedToId (parent missing on target)"))
            continue

        # set mapped ids onto the insert object
        insert_obj = r.copy()
        insert_obj["FromId"] = r.get("Target_FromId") or None
        insert_obj["RelatedToId"] = r.get("Target_RelatedToId")
        if r.get("Target_EmailTemplateId"):
            insert_obj["EmailTemplateId"] = r.get("Target_EmailTemplateId")
        # sanitize (remove read-only)
        final_obj = sanitize_for_insert(insert_obj)
        prepared.append((r.get("Id"), final_obj))

    logging.info(f"[INFO] Prepared {len(prepared)} EmailMessages for insert; skipped {len(skipped)} due to missing mappings.")

    # Open import CSV to write results as we attempt inserts
    with open(EM_import, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Source_Activity_Id", "Target_Activity_Id", "Success", "Errors"])

        # write skipped entries
        for sid, reason in skipped:
            writer.writerow([sid, "", "Skipped", reason])

        # Insert in chunks
        source_ids = [s for s, _ in prepared]
        insert_objs = [o for _, o in prepared]
        total_attempted = 0
        total_success = 0
        total_failed = 0

        for i, batch in enumerate(chunks(list(zip(source_ids, insert_objs)), insert_batch_size), start=1):
            batch_src_ids = [t[0] for t in batch]
            batch_objs = [t[1] for t in batch]
            attempt = 0
            while attempt <= max_retries:
                try:
                    logging.info(f"[INFO] Inserting batch {i} ({len(batch_objs)} records), attempt {attempt+1}")
                    results = sf_target.bulk.__getattr__(object_name).insert(batch_objs, batch_size=insert_batch_size)
                    # results is a list aligned with batch_objs
                    for src_id, res in zip(batch_src_ids, results):
                        total_attempted += 1
                        if res.get("success"):
                            total_success += 1
                            writer.writerow([src_id, res.get("id", ""), "True", ""])
                        else:
                            total_failed += 1
                            # collect error message(s)
                            errs = res.get("errors", [])
                            if isinstance(errs, list):
                                msgs = []
                                for e in errs:
                                    if isinstance(e, dict):
                                        msgs.append(e.get("message", str(e)))
                                    else:
                                        msgs.append(str(e))
                                errstr = ";".join(msgs)
                            else:
                                errstr = str(errs)
                            writer.writerow([src_id, "", "False", errstr])
                    # batch done, break retry loop
                    break
                except Exception as ex:
                    attempt += 1
                    logging.exception(f"[ERROR] Exception inserting batch {i} on attempt {attempt}: {ex}")
                    if attempt > max_retries:
                        # mark all as failed with exception
                        for src_id in batch_src_ids:
                            total_attempted += 1
                            total_failed += 1
                            writer.writerow([src_id, "", "False", f"Exception after {max_retries} retries: {ex}"])
                    else:
                        # exponential backoff
                        sleep_for = 2 ** attempt
                        logging.info(f"[INFO] Retry after {sleep_for}s")
                        time.sleep(sleep_for)

    logging.info(f"[SUMMARY] Total prepared: {len(prepared)}; attempted: {total_attempted}; success: {total_success}; failed: {total_failed}; skipped: {len(skipped)}")
    logging.info(f"[INFO] Import results saved -> {EM_import}")

if __name__ == "__main__":
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)
    export_activity(sf_source, sf_target, "EmailMessage", insert_batch_size=200)
