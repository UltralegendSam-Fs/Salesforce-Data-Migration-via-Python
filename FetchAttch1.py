import os
import pandas as pd
import logging
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from utils.mappings import fetch_target_mappings,FILES_DIR
from utils.retry_utils import safe_query

# Objects with their filter conditions
OBJECT_CONDITIONS = {
    "Account": "RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false AND DE_Is_Shell_Account__c = false",
    "FSL__Optimization_Data__c ": ""
}

BATCH_SIZE = 2000


# === Output file path inside files folder ===
# OUTPUT_FILE = os.path.join(FILES_DIR, "parent_id_mapping.xlsx")
OUTPUT_FILE = os.path.join(FILES_DIR, "parent_id_mapping.csv")   # ← CSV instead of XLSX

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
    results = safe_query(sf, soql)["records"]
    logging.info(f"Total attachments fetched: {len(results)}")
    return results

def fetch_filtered_attachments(sf, object_name, condition):
    """Fetch only attachments whose parent meets the object condition."""
    if not condition:
        # No condition — fetch all attachments for this object type
        soql = f"""
            SELECT Id, ParentId
            FROM Attachment
            WHERE CreatedDate >= LAST_N_MONTHS:24 AND Parent.Type = '{object_name}'
        """
    else:
        soql = f"""
            SELECT Id, ParentId
            FROM Attachment
            WHERE CreatedDate >= LAST_N_MONTHS:24 AND ParentId IN (
                SELECT Id FROM {object_name} WHERE {condition}
            )
        """
    print("soql: ", soql)
    results = safe_query(sf, soql)["records"]
    logging.info(f"Fetched {len(results)} attachments for {object_name} with condition: {condition}")
    return results


def filter_parent_ids_by_conditions(sf, object_name, parent_ids, condition):
    if not condition:
        return set(parent_ids)
    ids_str = ",".join([f"'{pid}'" for pid in parent_ids])
    soql = f"SELECT Id FROM {object_name} WHERE Id IN ({ids_str}) AND {condition}"
    print("soql: ", soql)
    return {r["Id"] for r in safe_query(sf, soql)["records"]}

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

    # df = pd.DataFrame(all_mappings)
    # df.to_excel(OUTPUT_FILE, index=False)
    # logging.info(f"Mapping saved to {OUTPUT_FILE}")

    df = pd.DataFrame(all_mappings)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")  # ← CSV export
    logging.info(f"Mapping saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
