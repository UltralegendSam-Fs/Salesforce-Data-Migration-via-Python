#!/usr/bin/env python3
"""
activity_export.py

Export Task/Event records from SOURCE Salesforce org
and prepare mapping CSV for migration into TARGET org.
"""

import csv
import os
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from activity_config import ACTIVITY_CONFIG, OBJECT_CONDITIONS
from mappings import fetch_target_mappings

# === folders & files ===
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)

task_export = os.path.join(FILES_DIR, "task_export.csv")     
event_export = os.path.join(FILES_DIR, "event_export.csv")

def fetch_activity_records(sf, object_name, fields, condition):
    """Fetch Task/Event records from source org with conditions."""
    field_str = ", ".join(fields)
    query = f"SELECT {field_str}, What.Type FROM {object_name} WHERE {condition}"
    print(f"[DEBUG] Fetching {object_name} records: {query}")
    results = sf.query_all(query)
    return results["records"]


def filter_parent_ids_by_object(sf, parent_ids, object_name):
    """Apply OBJECT_CONDITIONS filters for parent objects."""
    if not parent_ids or object_name not in OBJECT_CONDITIONS:
        return parent_ids  # nothing to filter

    condition = OBJECT_CONDITIONS[object_name]
    if not condition:
        return parent_ids  # no extra filter

    ids_str = ",".join([f"'{pid}'" for pid in parent_ids])
    soql = f"SELECT Id FROM {object_name} WHERE Id IN ({ids_str}) AND {condition}"
    print(f"[DEBUG] Filtering {object_name} IDs: {soql}")
    results = sf.query_all(soql)["records"]
    return set(r["Id"] for r in results)


def export_activity(sf_source, sf_target, object_name, output_file, batch_size=200):
    """Main export function for Task/Event."""
    config = ACTIVITY_CONFIG[object_name]
    fields = config["fields"]
    condition = config["condition"]

    # Fetch activities
    activities = fetch_activity_records(sf_source, object_name, fields, condition)
    print(f"[INFO] Fetched {len(activities)} {object_name} records from source")

    # Collect parent IDs
    what_ids_by_type = {}
    who_ids = set()

    for rec in activities:
        if rec.get("WhatId") and "Type" in rec["What"]:
            obj_type = rec["What"]["Type"]
            what_ids_by_type.setdefault(obj_type, set()).add(rec["WhatId"])
        if rec.get("WhoId"):
            who_ids.add(rec["WhoId"])

    # Apply OBJECT_CONDITIONS filtering + fetch target mappings
    parent_mappings = {}
    for obj_type, ids in what_ids_by_type.items():
        filtered_ids = filter_parent_ids_by_object(sf_source, ids, obj_type)
        print(f"[DEBUG] Filtered {len(filtered_ids)} {obj_type} IDs")
        if filtered_ids:
            mapping = fetch_target_mappings(sf_target, obj_type, filtered_ids, batch_size)
            parent_mappings.update(mapping)

    # Handle WhoId (normally Contact/Lead/User, assume Legacy_ID__c)
    if who_ids:
        mapping = fetch_target_mappings(sf_target, "Contact", who_ids, batch_size)
        parent_mappings.update(mapping)

    # Write export CSV
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        header = ["Source_Activity_Id", "Source_WhatId", "Target_WhatId", "Source_WhoId", "Target_WhoId"]
        writer.writerow(header)

        for rec in activities:
            src_id = rec["Id"]
            src_what = rec.get("WhatId")
            src_who = rec.get("WhoId")

            tgt_what = parent_mappings.get(src_what) if src_what else ""
            tgt_who = parent_mappings.get(src_who) if src_who else ""

            if (src_what and not tgt_what):
                continue

            writer.writerow([src_id, src_what, tgt_what, src_who, tgt_who])

    print(f"[SUCCESS] Exported {len(activities)} {object_name} records → {output_file}")


if __name__ == "__main__":
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    # Run for both Task and Event
    export_activity(sf_source, sf_target, "Task", task_export)
    export_activity(sf_source, sf_target, "Event", event_export)
