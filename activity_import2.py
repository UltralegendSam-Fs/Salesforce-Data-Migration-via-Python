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
import time
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from activity_config import TASK_FIELDS, EVENT_FIELDS
from mappings import fetch_createdByIds, build_owner_mapping,FILES_DIR

task_export = os.path.join(FILES_DIR, "task_export.csv")     
task_import_log = os.path.join(FILES_DIR, "task_import_log.csv")
event_export = os.path.join(FILES_DIR, "event_export.csv")
event_import_log = os.path.join(FILES_DIR, "event_import_log.csv")

# def fetch_records(sf, object_name, ids, fields):
#     """Fetch full Task/Event records from SOURCE org."""

#     soql = f"SELECT {', '.join(fields)} FROM {object_name} WHERE Id IN ({','.join([f"'{i}'" for i in ids])})"
#     print(f"[DEBUG] Fetching records: {soql}")
#     return sf.query_all(soql)["records"]

def fetch_records(sf, object_name, ids, fields, batch_size=200):
    """Fetch full Task/Event records from SOURCE org in batches to avoid URI too long."""
    records = []
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]
        soql = f"SELECT {', '.join(fields)} FROM {object_name} WHERE Id IN ({','.join([f"'{id}'" for id in batch_ids])})"
        print(f"[DEBUG] Fetching {object_name} batch {i//batch_size+1}: {soql}")
        res = sf.query_all(soql)["records"]
        records.extend(res)
    return records


def build_record(source_record, mapping_row, valid_fields, object_name,  owner_mappings):
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
    if object_name == "Task" and mapping_row.get("Source_WhatId") and tgt_what:
        if mapping_row["Source_WhatId"].startswith("a19"):  # Example: Request__c prefix is "a0X"
            record["Request__c"] = tgt_what
    
    # 🔹 Map CreatedById using createdBy_mappings
    # src_createdBy = source_record.get("CreatedById")
    # if src_createdBy and src_createdBy in createdBy_mappings:
    #     record["CreatedById"] = createdBy_mappings[src_createdBy]
    
    # 🔹 Map OwnerId using owner_mappings
    src_owner = source_record.get("OwnerId")
    if src_owner and src_owner in owner_mappings:
        record["OwnerId"] = owner_mappings[src_owner]

    # 🔹 Hardcode RecordTypeId for Task/Event
    if object_name == "Task":
        record["RecordTypeId"] = "0121K000001QPrPQAW"
    elif object_name == "Event":
        record["RecordTypeId"] = "0121K000001QProQAG"

    return record

# def bulk_insert(sf_target, object_name, records, batch_size=200):
#     results = []
#     for i in range(0, len(records), batch_size):
#         batch = records[i:i+batch_size]
#         res = sf_target.bulk.__getattr__(object_name).insert(batch, batch_size=batch_size)
#         results.extend(res)
#     return results
# import time

def bulk_insert_with_retry(sf_target, object_name, records, batch_size=200, max_retries=3, retry_delay=5):
    """
    Insert records into Salesforce with retries for failed ones.
    """
    attempt = 1
    all_results = []

    # Work with a dynamic retry queue
    to_retry = records

    while attempt <= max_retries and to_retry:
        print(f"[INFO] Attempt {attempt}: inserting {len(to_retry)} {object_name} records...")
        results = []
        for i in range(0, len(to_retry), batch_size):
            batch = to_retry[i:i+batch_size]
            res = sf_target.bulk.__getattr__(object_name).insert(batch, batch_size=batch_size)
            results.extend(res)

        all_results.extend(results)

        # Collect failed records for retry
        failed_records = []
        for rec, res in zip(to_retry, results):
            if not res["success"]:
                failed_records.append(rec)

        if failed_records:
            print(f"[WARN] {len(failed_records)} {object_name} records failed in attempt {attempt}. Retrying...")
            to_retry = failed_records
            attempt += 1
            time.sleep(retry_delay)  # wait before retry
        else:
            break  # no failures left

    if to_retry:
        print(f"[ERROR] {len(to_retry)} {object_name} records permanently failed after {max_retries} retries.")

    return all_results


def import_activities(sf_source, sf_target, object_name, fields, mapping_csv, log_csv):
    """Migrate Task/Event using mapping file."""

    # createdBy_mappings = {}

    # Read mappings
    with open(mapping_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        mappings = list(reader)

    source_ids = [m["Source_Activity_Id"] for m in mappings]
    if not source_ids:
        print(f"[WARN] No Source_Activity_Id found in {mapping_csv}. Skipping.")
        return

    # Fetch full records from SOURCE
    print(f"[INFO] Fetching {len(source_ids)} {object_name} records from SOURCE...")
    source_records = fetch_records(sf_source, object_name, source_ids, fields)

    # createdBy_ids = {fi["CreatedById"] for fi in source_records}
    # createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)
    
    ownerIds = {fi["OwnerId"] for fi in source_records}
    owner_mappings = build_owner_mapping(sf_source, sf_target, ownerIds)

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
        # 🚨 Skip if both Target_WhatId and Target_WhoId are missing
        if not m.get("Target_WhatId") and not m.get("Target_WhoId"):
            logs.append({
                "Source_Activity_Id": source_id,
                "Target_Activity_Id": None,
                "Success": False,
                "Errors": "Skipped: No Target_WhatId or Target_WhoId"
            })
            continue

        record = build_record(source_record, m, fields,object_name,owner_mappings)
        records_to_insert.append(record)

    # Bulk insert into TARGET
    print(f"[INFO] Inserting {len(records_to_insert)} {object_name} records into TARGET...")
    # results = bulk_insert(sf_target, object_name, records_to_insert, batch_size=200)
    results = bulk_insert_with_retry(sf_target, object_name, records_to_insert, batch_size=200, max_retries=3, retry_delay=5)


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
        print("results:", res)


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
    # import_activities(
    #     sf_source=sf_source,
    #     sf_target=sf_target,
    #     object_name="Event",
    #     fields=EVENT_FIELDS,
    #     mapping_csv=event_export,
    #     log_csv=event_import_log,
    # )


if __name__ == "__main__":
    main()
