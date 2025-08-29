#!/usr/bin/env python3
"""
activity_import.py

Purpose:
  - Migrate Task & Event records from SOURCE to TARGET Salesforce org
  - Reads mapping CSV with Source/Target Parent Ids
  - Fetches full records from SOURCE using Source_Activity_Id
  - Replaces parent Ids with Target Ids
  - Inserts into TARGET org
  - Logs results
"""

import csv
import os
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from activity_config import TASK_FIELDS, EVENT_FIELDS

# === folders & files ===
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)

task_export = os.path.join(FILES_DIR, "task_export.csv")     
task_import_log = os.path.join(FILES_DIR, "task_import_log.csv")
event_export = os.path.join(FILES_DIR, "event_export.csv")
event_import_log = os.path.join(FILES_DIR, "event_import_log.csv")

def fetch_records(sf, object_name, ids, fields):
    """Fetch full Task/Event records from SOURCE org."""
    soql = f"SELECT {', '.join(fields)} FROM {object_name} WHERE Id IN ({','.join([f"'{i}'" for i in ids])})"
    print(f"[DEBUG] Fetching records: {soql}")
    return sf.query_all(soql)["records"]

def build_record(source_record, mapping_row, valid_fields, object_name):
    """Build record for insert into TARGET, remapping parents + special Request__c handling."""
    record = {}

    # Copy fields from source if they exist and are not None
    for field in valid_fields:
        if field == "Id":
            record["Card_Legacy_Id__c"] = source_record.get("Id")
            continue
        if field in source_record and source_record[field] is not None:
            record[field] = source_record[field]

    # Override with mapped Target parent Ids
    tgt_what = mapping_row.get("Target_WhatId")
    tgt_who = mapping_row.get("Target_WhoId")

    if tgt_what:
        record["WhatId"] = tgt_what
    if tgt_who:
        record["WhoId"] = tgt_who

    # 🔹 Special case: If WhatId is a Request__c, also populate Request__c field
    #    We can detect this because mapping_row["Source_WhatId"] is from a Request__c
    if object_name == "Task" and mapping_row.get("Source_WhatId") and tgt_what:
        # If the source WhatId belonged to a Request__c, duplicate into Request__c field
        if mapping_row["Source_WhatId"].startswith("a19"):  # Example: Request__c prefix is "a0X"
            record["Request__c"] = tgt_what

    # 🔹 Hardcode RecordTypeId for Task/Event
    if object_name == "Task":
        record["RecordTypeId"] = "0121K000001QPrPQAW"
    elif object_name == "Event":
        record["RecordTypeId"] = "0124U0000015EnZQAU"

    return record


def import_activities(sf_source, sf_target, object_name, fields, mapping_csv, log_csv):
    """Migrate Task/Event using mapping file."""
    # Read mappings
    with open(mapping_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        mappings = list(reader)

    source_ids = [m["Source_Activity_Id"] for m in mappings]

    # Fetch full records from SOURCE
    print(f"[INFO] Fetching {len(source_ids)} {object_name} records from SOURCE...")
    source_records = fetch_records(sf_source, object_name, source_ids, fields)

    # Index by Id for quick lookup
    source_map = {rec["Id"]: rec for rec in source_records}

    # Prepare records for insert
    records_to_insert, logs = [], []
    for m in mappings:
        source_id = m["Source_Activity_Id"]
        source_record = source_map.get(source_id)

        if not source_record:
            logs.append({
                "Source_Activity_Id": source_id,
                "Target_Activity_Id": None,
                "Success": False,
                "Errors": "Source record not found"
            })
            continue

        record = build_record(source_record, m, fields,object_name)
        records_to_insert.append(record)

    # Bulk insert into TARGET
    print(f"[INFO] Inserting {len(records_to_insert)} {object_name} records into TARGET...")
    results = sf_target.bulk.__getattr__(object_name).insert(records_to_insert, batch_size=200)

    # Log results
    for m, res in zip(mappings, results):
        logs.append({
            "Source_Activity_Id": m["Source_Activity_Id"],
            "Target_Activity_Id": res.get("id") if res["success"] else None,
            "Success": res["success"],
            "Errors": "; ".join([e["message"] for e in res["errors"]]) if not res["success"] else "",
        })

    with open(log_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Source_Activity_Id", "Target_Activity_Id", "Success", "Errors"])
        writer.writeheader()
        writer.writerows(logs)

    print(f"[INFO] Inserted {sum(1 for l in logs if l['Success'])}/{len(logs)} {object_name} records.")
    failed = [l for l in logs if not l["Success"]]
    if failed:
        print(f"[ERROR] {len(failed)} failed inserts. Check {log_csv} for details.")


def main():
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    # Import Tasks
    import_activities(
        sf_source=sf_source,
        sf_target=sf_target,
        object_name="Task",
        fields=TASK_FIELDS,
        mapping_csv=task_export,
        log_csv=task_import_log,
    )

    # Import Events
    import_activities(
        sf_source=sf_source,
        sf_target=sf_target,
        object_name="Event",
        fields=EVENT_FIELDS,
        mapping_csv=event_export,
        log_csv=event_import_log,
    )


if __name__ == "__main__":
    main()
