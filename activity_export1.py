#!/usr/bin/env python3
"""
activity_export.py

Export Task/Event records from SOURCE Salesforce org
and prepare mapping CSV for migration into TARGET org.
"""

import csv
import os
import logging
from datetime import datetime


from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from activity_config import ACTIVITY_CONFIG, OBJECT_CONDITIONS
from utils.mappings import fetch_target_mappings, fetch_service_appointment_ids,FILES_DIR
from utils.retry_utils import safe_query
 

task_export = os.path.join(FILES_DIR, "task_export.csv")     
event_export = os.path.join(FILES_DIR, "event_export.csv")
batch_size=200

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(FILES_DIR, 'activity_export.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_activity_records(sf, object_name, fields, condition):
    """Fetch Task/Event records from source org with conditions."""
    con_soql = f"Select Id from Contact where Account.RecordType.Name IN ('Parent Company','Brand','Dealer') AND Account.IsPersonAccount = false AND Account.DE_Is_Shell_Account__c = false"
    con_results = safe_query(sf, con_soql)
    con_ids = [rec['Id'] for rec in con_results['records']]
    
    field_str = ", ".join(fields)
    query = f"SELECT {field_str}, What.Type FROM {object_name} WHERE {condition}"
    results = safe_query(sf, query)
    
    # Filter records based on Contact.Account condition
    filtered_records = []
    for rec in results["records"]:
        if rec.get("WhatId"):
            # Keep record if WhatId exists
            filtered_records.append(rec)
        elif rec.get("WhoId") in con_ids:
            # Keep record if WhatId is null but WhoId is eligible
            filtered_records.append(rec)
    results["records"] = filtered_records
    return results["records"]


def filter_parent_ids_by_object(sf, parent_ids, object_name):
    """Apply OBJECT_CONDITIONS filters for parent objects."""
    if not parent_ids or object_name not in OBJECT_CONDITIONS:
        return parent_ids  # nothing to filter
    
    # Special case: ServiceAppointment → needs two queries
    if object_name == "ServiceAppointment":
        filtered_ids = set()
        filtered_ids=fetch_service_appointment_ids(sf,parent_ids)
        return filtered_ids

    # Generic case (other objects with single condition)
    condition = OBJECT_CONDITIONS[object_name]
    if not condition:
        return parent_ids  # no extra filter
    
    filtered_ids = set()
    parent_ids = list(parent_ids)

    for i in range(0, len(parent_ids), batch_size):
        batch = parent_ids[i : i + batch_size]
        ids_str = ",".join([f"'{pid}'" for pid in batch])
        soql = f"SELECT Id FROM {object_name} WHERE Id IN ({ids_str}) AND {condition}"
        try:
            results = safe_query(sf, soql)["records"]
            filtered_ids.update(r["Id"] for r in results)
        except Exception as e:
            logger.error(f"Failed filtering {object_name} batch {i//batch_size+1}: {e}")
            # Decide: skip this batch or re-raise
            continue  

    return filtered_ids


def export_activity(sf_source, sf_target, object_name, output_file, batch_size=200):
    """Main export function for Task/Event with progress tracking (no validation)."""
    logger.info(f"Starting {object_name} export process")
    config = ACTIVITY_CONFIG[object_name]
    fields = config["fields"]
    condition = config["condition"]

    # Fetch activities
    logger.info(f"Fetching {object_name} records from source...")
    activities = fetch_activity_records(sf_source, object_name, fields, condition)
    logger.info(f"Fetched {len(activities)} {object_name} records from source")

    # No validation: proceed with all fetched activities
    if activities:
        valid_activities = activities
    else:
        logger.warning(f"No {object_name} records found")
        valid_activities = []

    # Collect parent IDs with progress tracking
    logger.info(f"Processing parent IDs for {len(valid_activities)} records...")
    
    what_ids_by_type = {}
    who_ids = set()

    for rec in valid_activities:
        if rec.get("WhatId") and "Type" in rec["What"]:
            obj_type = rec["What"]["Type"]
            what_ids_by_type.setdefault(obj_type, set()).add(rec["WhatId"])
        if rec.get("WhoId"):
            who_ids.add(rec["WhoId"])

    # Apply OBJECT_CONDITIONS filtering + fetch target mappings
    parent_mappings = {}
    for obj_type, ids in what_ids_by_type.items():
        filtered_ids = filter_parent_ids_by_object(sf_source, ids, obj_type)
        if filtered_ids:
            mapping = fetch_target_mappings(sf_target, obj_type, filtered_ids, batch_size)
            parent_mappings.update(mapping)

    # Handle WhoId (normally Contact/Lead/User, assume Legacy_ID__c)
    if who_ids:
        mapping = fetch_target_mappings(sf_target, "Contact", who_ids, batch_size)
        parent_mappings.update(mapping)
        
    
    # Write export CSVs with progress tracking
    logger.info(f"Writing {len(valid_activities)} {object_name} records to {output_file}...")

    exported_count = 0
    unmatched_count = 0
    unmatched_file = os.path.join(FILES_DIR, f"{object_name.lower()}_unmatched.csv")

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile, \
         open(unmatched_file, "w", newline="", encoding="utf-8") as unmatched_csvfile:
        writer = csv.writer(csvfile)
        unmatched_writer = csv.writer(unmatched_csvfile)
        header = ["Source_Activity_Id", "Source_WhatId", "Target_WhatId", "Source_WhoId", "Target_WhoId","Type"]
        writer.writerow(header)
        unmatched_writer.writerow(header)

        for rec in valid_activities:
            src_id = rec["Id"]
            src_what = rec.get("WhatId")
            src_who = rec.get("WhoId")
            src_type = rec["What"]["Type"] if rec.get("What") else ""

            tgt_what = parent_mappings.get(src_what) if src_what else ""
            tgt_who = parent_mappings.get(src_who) if src_who else ""

            is_unmatched = (src_what and not tgt_what) or (src_who and not tgt_who) or (not tgt_what and not tgt_who )

            if is_unmatched:
                unmatched_writer.writerow([src_id, src_what, tgt_what, src_who, tgt_who, src_type])
                unmatched_count += 1
            else:
                writer.writerow([src_id, src_what, tgt_what, src_who, tgt_who, src_type])
                exported_count += 1

    logger.info(f"Successfully exported {exported_count} {object_name} records to {output_file}")
    logger.info(f"Wrote {unmatched_count} {object_name} records with missing target IDs to {unmatched_file}")
    # Export done


if __name__ == "__main__":
    try:
        print(SF_SOURCE)
        print(os.getenv("SF_SOURCE_USERNAME"))
        logger.info("Starting activity export process")
        sf_source = connect_salesforce(SF_SOURCE)
        sf_target = connect_salesforce(SF_TARGET)
        logger.info("Successfully connected to both Salesforce orgs")

        # Run for both Task and Event
        export_activity(sf_source, sf_target, "Task", task_export)
        export_activity(sf_source, sf_target, "Event", event_export)
        
        logger.info("Activity export process completed successfully")
    except Exception as e:
        logger.error(f"Activity export process failed: {e}", exc_info=True)
        raise
