#!/usr/bin/env python3
"""
EmailMessage.py

Export EmailMessage from Salesforce source arg and insert in Taregt org.
"""

import csv
import os
import logging
import pandas as pd
from datetime import datetime
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from utils.mappings import fetch_target_mappings,fetch_createdByIds,fetch_service_appointment_ids,FILES_DIR
 
from utils.retry_utils import safe_query

EM_export = os.path.join(FILES_DIR, "emailMessage_export.csv")     
EM_import = os.path.join(FILES_DIR, "emailMessage_import.csv")     
EM_invalid = os.path.join(FILES_DIR, "emailMessage_invalid.csv")     
emailtemplate_mapping = os.path.join(FILES_DIR, "emailtemplate_mapping.csv")
log_file = os.path.join(FILES_DIR, "emailmessage_migration.log")

# === Logging setup ===
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)     

OBJECT_CONDITIONS = {
    "Impact_Tracker__c": "Clients_Brands__c != null AND Clients_Brands__r.RecordType.Name IN ('Parent Company','Brand','Dealer')",
    "ServiceAppointment": " "  
}

def fetch_em_records(sf):
    """Fetch EmailMessage records first, then filter based on parent object conditions."""

    # 1. Fetch EmailMessages with parent type
    query = """
        SELECT Id,ParentId,TextBody, HtmlBody,ActivityId,Headers,Subject,FromName,FromAddress,ValidatedFromAddress,ToAddress,CcAddress,BccAddress,Incoming,Status,MessageDate,ReplyToEmailMessageId,MessageIdentifier,ThreadIdentifier,ClientThreadIdentifier,FromId,IsClientManaged,AttachmentIds,RelatedToId, RelatedTo.Type,IsTracked,FirstOpenedDate,LastOpenedDate,IsBounced,EmailTemplateId,EmailRoutingAddressId,AutomationType
        FROM EmailMessage
        WHERE CreatedDate >= LAST_N_MONTHS:24 AND RelatedToId != NULL
        AND RelatedTo.Type IN ('Impact_Tracker__c','ServiceAppointment')
    """
    logging.info("[DEBUG] Fetching EmailMessages...")
    email_results = safe_query(sf, query)
    emails = email_results["records"]
    print(f"[DEBUG] Found {len(emails)} EmailMessages")

    if not emails:
        logging.warning("No EmailMessages found in last 24 months.")
        return []

    # 2. Group parent IDs by object type (RelatedTo.Type)
    parent_map = {}
    for e in emails:
        obj_type = e.get("RelatedTo", {}).get("Type")
        #obj_type = e.get("RelatedTo", {}).get("attributes", {}).get("type")
        print(f"[DEBUG] EmailMessage RelatedTo Type: {obj_type}")
        parent_id = e.get("RelatedToId")
        if obj_type and parent_id:
            parent_map.setdefault(obj_type, set()).add(parent_id)

    logging.info(f"[DEBUG] Parent object groups from EmailMessages: {list(parent_map.keys())}")

    # 3. Validate parents based on OBJECT_CONDITIONS
    valid_parent_ids = set()

    for obj_name, ids in parent_map.items():
        if obj_name in OBJECT_CONDITIONS and obj_name != "ServiceAppointment":
            cond = OBJECT_CONDITIONS[obj_name]
            id_chunks = [list(ids)[i:i+2000] for i in range(0, len(ids), 2000)]

            for chunk in id_chunks:
                ids_str = ",".join([f"'{i}'" for i in chunk])
                query = f"SELECT Id FROM {obj_name} WHERE Id IN ({ids_str}) AND {cond}"
                logging.info(f"[DEBUG] Fetching {obj_name} with condition: {cond} (batch {len(chunk)})")
                res = safe_query(sf, query)
                valid_parent_ids.update([r["Id"] for r in res["records"]])
                print(f"[DEBUG] Found {len(res['records'])} valid {obj_name} records")

        elif obj_name == "ServiceAppointment":
            sa_ids = fetch_service_appointment_ids(sf, sa_ids=ids)
            valid_parent_ids.update(sa_ids)
            print(f"[DEBUG] Found {len(sa_ids)} valid ServiceAppointment records")

    if not valid_parent_ids:
        logging.warning("No parent records matched given conditions.")
        return []

    # 4. Filter emails to keep only those with valid parents
    filtered_emails = [e for e in emails if e.get("RelatedToId") in valid_parent_ids]

    logging.info(f"[INFO] Total EmailMessages fetched after filtering: {len(filtered_emails)}")
    return filtered_emails

def fetch_target_ids(sf_target, records):
    from_ids = set()
    sa_related_ids = set()
    it_related_ids = set()
    emailTemplate_ids = set()

    from_mappings = {}
    relatedTo_mappings = {}    
    for rec in records:

        if rec.get("FromId") and rec["FromId"] not in from_ids and rec["FromId"] != "":
            from_ids.add(rec["FromId"])

        if rec.get("RelatedToId") and rec["RelatedToId"] != "":
            if rec["RelatedTo"]["Type"] == "Impact_Tracker__c":
                it_related_ids.add(rec["RelatedToId"])
            elif rec["RelatedTo"]["Type"] == "ServiceAppointment":
                sa_related_ids.add(rec["RelatedToId"])

        if rec.get("EmailTemplateId") and rec["EmailTemplateId"] not in emailTemplate_ids and rec["EmailTemplateId"] != "":
            emailTemplate_ids.add(rec["EmailTemplateId"])

    from_mappings = fetch_target_mappings(sf_target, "User", from_ids, 200)
    relatedTo_mappings.update(fetch_target_mappings(sf_target, "Impact_Tracker__c", it_related_ids, 200))
    relatedTo_mappings.update(fetch_target_mappings(sf_target, "ServiceAppointment", sa_related_ids, 200))
    
    df = pd.read_csv(emailtemplate_mapping)
    emailtemplate_mappings = dict(zip(df['SourceTemplateId'], df['TargetTemplateId']))
    
    for rec in records:
        rec["Target_FromId"] = from_mappings.get(rec.get("FromId"), None)
        rec["Target_RelatedToId"] = relatedTo_mappings.get(rec.get("RelatedToId"), None)
        rec["Target_EmailTemplateId"] = emailtemplate_mappings.get(rec.get("EmailTemplateId"), None)
    
    return records

def insert_emailmessages_with_retry(sf_target, object_name, prepared_records, skipped_records, batch_size=200, max_retries=3, retry_delay=5):
    """
    Insert EmailMessage records into target org in chunks with retry logic.
    Logs results and writes them into EM_import file.
    """
    import time

    with open(EM_import, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Source_Activity_Id", "Target_Activity_Id", "Success", "Errors"])

        # --- Write skipped records first ---
        for sid, reason in skipped_records:
            logging.warning(f"Skipped {sid}: {reason}")
            writer.writerow([sid, "", "Skipped", reason])

        # --- Insert prepared records in batches with retry ---
        for i in range(0, len(prepared_records), batch_size):
            batch = prepared_records[i:i + batch_size]
            source_ids = [sid for sid, _ in batch]
            objs = [obj for _, obj in batch]

            print(f"[INFO] Inserting batch {i//batch_size + 1} with {len(batch)} records...")
            
            # Retry logic for each batch
            results = None
            for attempt in range(max_retries):
                try:
                    results = sf_target.bulk.__getattr__(object_name).insert(objs, batch_size=batch_size)
                    break  # Success, exit retry loop
                except Exception as e:
                    if attempt == max_retries - 1:
                        logging.error(f"Batch {i//batch_size + 1} failed after {max_retries} attempts: {e}")
                        # Mark all records in this batch as failed
                        results = [{"success": False, "id": None, "errors": [str(e)]} for _ in batch]
                    else:
                        logging.warning(f"Batch {i//batch_size + 1} attempt {attempt + 1} failed: {e}. Retrying...")
                        time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff

            # Process results
            for sid, res in zip(source_ids, results):
                errors = res.get("errors", [])
                if not errors:
                    writer.writerow([sid, res.get("id", ""), True, ""])
                else:
                    error_msgs = []
                    for e in errors:
                        if isinstance(e, dict):
                            error_msgs.append(e.get("message", str(e)))
                        else:
                            error_msgs.append(str(e))
                    writer.writerow([sid, "", False, ";".join(error_msgs)])

    logging.info(f"[INFO] Migration complete. Inserted {len(prepared_records)} records, skipped {len(skipped_records)}.")
    logging.info(f"[INFO] Results saved to {EM_import}")

def insert_emailmessages(sf_target, object_name, prepared_records, skipped_records, batch_size=200):
    """Legacy function - now calls retry version"""
    return insert_emailmessages_with_retry(sf_target, object_name, prepared_records, skipped_records, batch_size)

def export_activity(sf_source, sf_target, object_name, batch_size=200):
    """Main export function for EM."""

    prepared_records = []
    skipped_records = []
    em_records = fetch_em_records(sf_source)
    em_records = fetch_target_ids(sf_target, em_records)

    for rec in em_records:
        # Skip if parent or email template mapping is missing
        if not rec.get("Target_RelatedToId"):
            skipped_records.append((rec["Id"], "No mapped parent"))
            continue
        if rec.get("EmailTemplateId") and not rec.get("Target_EmailTemplateId"):
            skipped_records.append((rec["Id"], "Unmapped EmailTemplate"))
            continue

        insert_data = rec.copy()
        insert_data["FromId"] = rec["Target_FromId"]
        insert_data["RelatedToId"] = rec["Target_RelatedToId"]
        insert_data["EmailTemplateId"] = rec["Target_EmailTemplateId"]
        insert_data["status"] = "5"  # Sent
        insert_data["Card_Legacy_Id__c"] = rec["Id"]  # Custom field to track legacy ID

        # Remove non-insertable fields
        for f in ["Id", "ActivityId", "RelatedTo", "Target_FromId", "Target_RelatedToId",
                  "Target_EmailTemplateId", "ValidatedFromAddress"]:
            insert_data.pop(f, None)

        prepared_records.append((rec["Id"], insert_data))

    # --- Separate records based on Target_RelatedToId presence ---
    valid_records = []
    invalid_records = []

    for rec in em_records:
        if rec.get("Target_RelatedToId") and (not rec.get("EmailTemplateId") or rec.get("Target_EmailTemplateId")):
            valid_records.append(rec)
        else:
            invalid_records.append(rec)

    # --- Export mapping for audit ---
    with open(EM_export, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        header = ["Source_EM_Id", "Source_RelatedId", "Source_FromId",
                  "Target_FromId", "Target_RelatedToId","Source_EmailTemplateId", "Target_EmailTemplateId"]
        writer.writerow(header)
        for rec in valid_records:
            writer.writerow([
                rec["Id"], 
                rec.get("RelatedToId", ""), 
                rec.get("FromId", ""),
                rec.get("Target_FromId", ""), 
                rec.get("Target_RelatedToId", ""),
                rec.get("EmailTemplateId", ""),
                rec.get("Target_EmailTemplateId", "")
            ])
    with open(EM_invalid, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        header = ["Source_EM_Id", "Source_RelatedId", "Source_FromId",
                  "Target_FromId", "Target_RelatedToId","Source_EmailTemplateId", "Target_EmailTemplateId", "Reason"]
        writer.writerow(header)
        for rec in invalid_records:
            reason = "No mapped parent" if not rec.get("Target_RelatedToId") else "Unmapped EmailTemplate"
            writer.writerow([
                rec["Id"], 
                rec.get("RelatedToId", ""), 
                rec.get("FromId", ""),
                rec.get("Target_FromId", ""), 
                rec.get("Target_RelatedToId", ""),
                rec.get("EmailTemplateId", ""),
                rec.get("Target_EmailTemplateId", ""),
                reason
            ])

    print(f"[SUCCESS] Prepared {len(prepared_records)} records, skipped {len(skipped_records)} → {EM_export}")


    # --- Insert records using the new method ---
    insert_emailmessages(sf_target, object_name, prepared_records, skipped_records, batch_size)


if __name__ == "__main__":
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    export_activity(sf_source, sf_target, "EmailMessage")
