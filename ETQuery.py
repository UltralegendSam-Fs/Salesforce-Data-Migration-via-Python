#!/usr/bin/env python3
"""
EmailMessage.py

Export EmailMessage from Salesforce source arg and insert in Taregt org.
"""

import csv
import os
import logging
import pandas as pd
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from utils.mappings import fetch_target_mappings,fetch_createdByIds,fetch_service_appointment_ids,FILES_DIR
from utils.retry_utils import safe_query

template_mapping_file = os.path.join(FILES_DIR, "emailtemplate_mapping_file.csv")
log_file = os.path.join(FILES_DIR, "emailtemplate_migration.log")

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


def export_template(sf_source, sf_target, object_name, batch_size=200):
    """Main export function for EM."""
    template_ids = set()
    
    em_records = fetch_em_records(sf_source)
    template_ids.update(r["EmailTemplateId"] for r in em_records if r.get("EmailTemplateId"))
    logging.info(f"[INFO] Found {len(template_ids)} unique EmailTemplateIds")
    if not template_ids:
        return
    id_list = list(template_ids)
    print("id_list", id_list)
    # Chunk into batches of 2000 (SOQL limit)
    for i in range(0, len(id_list), 2000):
        chunk = id_list[i:i+2000]
        ids_str = ",".join([f"'{i}'" for i in chunk])
        query = f"""
            SELECT Id, Name, DeveloperName, ApiVersion, FolderId, 
                   Subject, HtmlValue, Body, TemplateType
            FROM EmailTemplate
            WHERE Id IN ({ids_str})
        """
        logging.info(query)
 
    

if __name__ == "__main__":
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    export_template(sf_source, sf_target, "EmailMessage")