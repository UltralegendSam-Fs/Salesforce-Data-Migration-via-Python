import os
import pandas as pd
import logging
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from mappings import fetch_target_mappings   # ← Imported here

# Objects with their filter conditions
OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Retired','Dealer') AND IsPersonAccount = false",
    "FSL__Optimization_Data__c ": ""
}

BATCH_SIZE = 2000

# === Ensure files folder exists ===
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)

# === Output file path inside files folder ===
OUTPUT_FILE = os.path.join(FILES_DIR, "parent_id_mapping.xlsx")
LOG_FILE = os.path.join(FILES_DIR, "migration.log")

# === Logging configuration ===
logging.basicConfig(filename=LOG_FILE,level=logging.INFO,format="%(asctime)s - %(message)s")

def build_prefix_map(sf, object_list):
    prefix_map = {}
    for obj in object_list:
        desc = getattr(sf, obj).describe()
        prefix_map[desc["keyPrefix"]] = obj
    return prefix_map

def fetch_all_attachments(sf):
    soql = "SELECT Id, ParentId FROM Attachment"
    results = sf.query_all(soql)["records"]
    logging.info(f"Total attachments fetched: {len(results)}")
    return results

def fetch_filtered_attachments(sf, object_name, condition):
    """Fetch only attachments whose parent meets the object condition."""
    if not condition:
        # No condition — fetch all attachments for this object type
        soql = f"""
            SELECT Id, ParentId
            FROM Attachment
            WHERE Parent.Type = '{object_name}'
        """
    else:
        soql = f"""
            SELECT Id, ParentId
            FROM Attachment
            WHERE ParentId IN (
                SELECT Id FROM {object_name} WHERE {condition}
            )
        """
    print("soql: ", soql)
    results = sf.query_all(soql)["records"]
    logging.info(f"Fetched {len(results)} attachments for {object_name} with condition: {condition}")
    return results


def filter_parent_ids_by_conditions(sf, object_name, parent_ids, condition):
    if not condition:
        return set(parent_ids)
    ids_str = ",".join([f"'{pid}'" for pid in parent_ids])
    soql = f"SELECT Id FROM {object_name} WHERE Id IN ({ids_str}) AND {condition}"
    print("soql: ", soql)
    return {r["Id"] for r in sf.query_all(soql)["records"]}

def main():
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    all_mappings = []

    for obj_name, condition in OBJECT_CONDITIONS.items():
        logging.info(f"Processing object: {obj_name}")
        
        # Fetch attachments for this object that meet the condition
        relevant_attachments = fetch_filtered_attachments(sf_source, obj_name, condition)
        if not relevant_attachments:
            continue

        # Extract parent IDs for mapping
        parent_ids = {att["ParentId"] for att in relevant_attachments}

        # Get mapping for target org
        target_mapping = fetch_target_mappings(sf_target, obj_name, parent_ids, BATCH_SIZE)

        for att in relevant_attachments:
            src_parent = att["ParentId"]
            tgt_parent = target_mapping.get(src_parent, "")
            all_mappings.append({
                "ParentObject": obj_name,
                "AttachmentId": att["Id"],
                "SourceParentId": src_parent,
                "TargetParentId": tgt_parent
            })

    df = pd.DataFrame(all_mappings)
    df.to_excel(OUTPUT_FILE, index=False)
    logging.info(f"Mapping saved to {OUTPUT_FILE}")


# def main():
#     sf_source = connect_salesforce(SF_SOURCE)
#     sf_target = connect_salesforce(SF_TARGET)

#     prefix_map = build_prefix_map(sf_source, list(OBJECT_CONDITIONS.keys()))
#     all_attachments = fetch_all_attachments(sf_source)

#     object_parent_map = {}
#     for att in all_attachments:
#         prefix = att["ParentId"][:3]
#         obj_name = prefix_map.get(prefix)
#         if obj_name:
#             object_parent_map.setdefault(obj_name, set()).add(att["ParentId"])

#     all_mappings = []

#     for obj_name, parent_ids in object_parent_map.items():
#         logging.info(f"Processing object: {obj_name} with {len(parent_ids)} parent records")

#         filtered_ids = filter_parent_ids_by_conditions(sf_source, obj_name, parent_ids, OBJECT_CONDITIONS[obj_name])
#         if not filtered_ids:
#             logging.info(f"No parent records match conditions for {obj_name}")
#             continue

#         relevant_attachments = [att for att in all_attachments if att["ParentId"] in filtered_ids]
#         target_mapping = fetch_target_mappings(sf_target, obj_name, filtered_ids, BATCH_SIZE)

#         for att in relevant_attachments:
#             src_parent = att["ParentId"]
#             tgt_parent = target_mapping.get(src_parent, "")
#             all_mappings.append({
#                 "ParentObject": obj_name,
#                 "AttachmentId": att["Id"],
#                 "SourceParentId": src_parent,
#                 "TargetParentId": tgt_parent
#             })

#     df = pd.DataFrame(all_mappings)
#     df.to_excel(OUTPUT_FILE, index=False)
#     logging.info(f"Mapping saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
