#!/usr/bin/env python3
"""
activity_export.py

Export Task/Event records from SOURCE Salesforce org
and prepare mapping CSV for migration into TARGET org.
"""

import csv
import os
import logging
import pandas as pd
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from activity_config import ACTIVITY_CONFIG, OBJECT_CONDITIONS
from mappings import fetch_target_mappings

# === folders & files ===
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)

EM_export = os.path.join(FILES_DIR, "eventMessage_export.csv")     
EM_import = os.path.join(FILES_DIR, "eventMessage_import.csv")     
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

def fetch_em_records(sf):
    """Fetch Emailmessage records from source org with conditions."""
    
    query = f"select Id,HtmlBody, TextBody, ActivityId,Subject,FromName,FromAddress,ValidatedFromAddress,ToAddress,CcAddress,BccAddress,Incoming,Status,MessageDate,MessageIdentifier,ThreadIdentifier,FromId,IsClientManaged,AttachmentIds,RelatedToId,IsTracked,FirstOpenedDate,LastOpenedDate,IsBounced,EmailTemplateId,RelatedTo.Type from EmailMessage where CreatedDate >= LAST_N_MONTHS:24 and RelatedToId != null and RelatedTo.Type IN ('Impact_Tracker__c','ServiceAppointment')"
    print(f"[DEBUG] Fetching records: {query}")
    results = sf.query_all(query)
    return results["records"]


def fetch_target_ids(sf_target, records):
    activity_ids = set()
    from_ids = set()
    sa_related_ids = set()
    it_related_ids = set()
    emailTemplete_ids = set()

    activity_mappings = {}
    from_mappings = {}
    relatedTo_mappings = {}    
    for rec in records:
        if rec.get("ActivityId") and rec["ActivityId"] not in activity_ids and rec["ActivityId"] != "":
            activity_ids.add(rec["ActivityId"])
        if rec.get("FromId") and rec["FromId"] not in from_ids and rec["FromId"] != "":
            from_ids.add(rec["FromId"])
        if rec.get("RelatedToId") and rec["RelatedToId"] != "":
            if rec["RelatedTo"]["Type"] == "Impact_Tracker__c":
                it_related_ids.add(rec["RelatedToId"])
            elif rec["RelatedTo"]["Type"] == "ServiceAppointment":
                sa_related_ids.add(rec["RelatedToId"])
        if rec.get("EmailTemplateId") and rec["EmailTemplateId"] not in emailTemplete_ids and rec["EmailTemplateId"] != "":
            emailTemplete_ids.add(rec["EmailTemplateId"])

    activity_mappings = fetch_target_mappings(sf_target, "Task", activity_ids, 200)
    from_mappings = fetch_target_mappings(sf_target, "User", from_ids, 200)
    relatedTo_mappings.update(fetch_target_mappings(sf_target, "Impact_Tracker__c", it_related_ids, 200))
    relatedTo_mappings.update(fetch_target_mappings(sf_target, "ServiceAppointment", sa_related_ids, 200))
    
    df = pd.read_csv(emailtemplate_mapping)
    emailtemplate_mappings = dict(zip(df['SourceTemplateId'], df['TargetTemplateId']))

    return activity_mappings, from_mappings, relatedTo_mappings, emailtemplate_mappings



def export_activity(sf_source, sf_target, object_name, batch_size=200):
    """Main export function for EM."""

    # Fetch activities
    prepared_records = []
    em_records = fetch_em_records(sf_source)
    activity_mappings, from_mappings, relatedTo_mappings, emailtemplate_mappings = fetch_target_ids(sf_target, em_records)
    
    for rec in em_records:
        insert_data = rec.copy()
        # Map fields for target
        rec["Target_ActivityId"] = activity_mappings.get(rec["ActivityId"], "")
        rec["Target_FromId"] = from_mappings.get(rec["FromId"], "")
        rec["Target_RelatedToId"] = relatedTo_mappings.get(rec["RelatedToId"], "")
        rec["Target_EmailTemplateId"] = emailtemplate_mappings.get(rec["EmailTemplateId"], "")

        # Update insert_data with mapped IDs
        insert_data["ActivityId"] = rec["Target_ActivityId"]
        insert_data["FromId"] = rec["Target_FromId"]
        insert_data["RelatedToId"] = rec["Target_RelatedToId"]
        insert_data["EmailTemplateId"] = rec["Target_EmailTemplateId"]

        # Remove non-insertable fields
        insert_data.pop("Id", None)
        insert_data.pop("ActivityId", None)
        insert_data.pop("RelatedTo", None)
        prepared_records.append(insert_data)
    

    # Write export CSV
    with open(EM_export, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        header = ["Source_EM_Id", "Source_ActivityId", "Source_RelatedId", "Source_FromId", "Target_ActivityId", "Target_FromId", "Target_RelatedToId", "Target_EmailTemplateId"]
        writer.writerow(header)
        for rec in em_records:
            writer.writerow([rec["Id"], rec.get("ActivityId", ""), rec.get("RelatedToId", ""), rec.get("FromId", ""), rec.get("Target_ActivityId", ""), rec.get("Target_FromId", ""), rec.get("Target_RelatedToId", ""), rec.get("Target_EmailTemplateId", "")])

    print(f"[SUCCESS] Exported {len(em_records)} {object_name} records → {EM_export}")
    print(f"[INFO] Fetched {len(em_records)} {object_name} records from source")

    results = sf_target.bulk.__getattr__(object_name).insert(prepared_records, batch_size=200)
    print(f"[SUCCESS] Inserted {len(results)} {object_name} records into target")
    print(f"[INFO] Insert results sample: {results}")

    with open(EM_import, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["Source_Activity_Id", "Target_Activity_Id", "Success", "Errors"]
        writer.writerow(header)
        for rec, res in zip(em_records, results):
            writer.writerow([rec["Id"], res.get("id", ""), res.get("success", False), ";".join(res.get("errors", []))])
    logging.info(f"New records with status saved → {EM_import}")

    logging.info(f"Migration complete. {len(prepared_records)} records processed.")

if __name__ == "__main__":
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    # Run for both Task and Event
    export_activity(sf_source, sf_target, "EmailMessage")
