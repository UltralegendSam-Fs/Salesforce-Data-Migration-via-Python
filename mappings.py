import os

def fetch_target_mappings(sf, object_name, source_parent_ids, batch_size):
    """Fetch target org record Ids by Legacy_ID__c"""
    mapping = {}
    parent_list = list(source_parent_ids)
    
    for i in range(0, len(parent_list), batch_size):
        chunk = parent_list[i:i+batch_size]
        ids_str = ",".join([f"'{pid}'" for pid in chunk])
        soql = f"SELECT Id, Card_Legacy_Id__c FROM {object_name} WHERE Card_Legacy_Id__c IN ({ids_str})"
        
        results = sf.query_all(soql)["records"]
        
        for r in results:
            mapping[r["Card_Legacy_Id__c"]] = r["Id"]

    return mapping

def fetch_createdByIds(sf_target, createdByIds):
    """Fetch CreatedById and LastModifiedById mappings from User object."""
    batch_size = 200
    integration_user_id = "0054U00000IESFZQA5"  # Replace with actual integration user Id in target org
    user_mapping = {}
    id_list = list(createdByIds)
    
    for i in range(0, len(id_list), batch_size):
        chunk = id_list[i:i+batch_size]
        ids_str = ",".join([f"'{uid}'" for uid in chunk])
        soql = f"SELECT Id, Card_Legacy_Id__c FROM User WHERE Card_Legacy_Id__c IN ({ids_str})"
        # print(f"[DEBUG] Fetching Users for CreatedById: {soql}")
        results = sf_target.query_all(soql)["records"]
        
        for r in results:
            if r.get("Card_Legacy_Id__c"):
                user_mapping[r["Card_Legacy_Id__c"]] = r["Id"]

        # Assign integration user for any missing ones
        for legacy_id in chunk:
            if legacy_id not in user_mapping:
                user_mapping[legacy_id] = integration_user_id

    return user_mapping

def build_owner_mapping(sf_source, sf_target, ownerIds):
    """
    Build mapping from source OwnerId → target OwnerId.
    - Users matched via Card_Legacy_Id__c
    - Groups (Queues) matched via DeveloperName
    - Fallback to Integration User Id if no match found
    """
    owner_mapping = {}
    integration_user_id = "0054U00000IESFZQA5"  # Replace with actual integration user Id in target org
    if not ownerIds:
        return owner_mapping

    id_list = list(ownerIds)
    batch_size = 200

    # --- 1. Build Group Mapping (source → target) ---
    source_group_ids = [oid for oid in id_list if oid.startswith("00G")]
    group_mapping = {}
    if source_group_ids:
        ids_str = ",".join([f"'{gid}'" for gid in source_group_ids])

        # Fetch source Groups
        soql_source = f"""
            SELECT Id, DeveloperName
            FROM Group
            WHERE Id IN ({ids_str})
            AND Type = 'Queue'
        """
        source_groups = sf_source.query_all(soql_source)["records"]

        if source_groups:
            dev_names = [f"'{r['DeveloperName']}'" for r in source_groups if r.get("DeveloperName")]

            if dev_names:
                soql_target = f"""
                    SELECT Id, DeveloperName
                    FROM Group
                    WHERE DeveloperName IN ({",".join(dev_names)})
                    AND Type = 'Queue'
                """
                target_groups = sf_target.query_all(soql_target)["records"]
                target_map = {r["DeveloperName"]: r["Id"] for r in target_groups}

                # Build mapping
                for r in source_groups:
                    dev_name = r.get("DeveloperName")
                    if dev_name and dev_name in target_map:
                        group_mapping[r["Id"]] = target_map[dev_name]

    # --- 2. Process in Chunks for Users + Groups ---
    for i in range(0, len(id_list), batch_size):
        chunk = id_list[i:i+batch_size]

        # Users
        user_ids = [oid for oid in chunk if oid.startswith("005")]
        if user_ids:
            ids_str = ",".join([f"'{oid}'" for oid in user_ids])
            soql_user = f"""
                SELECT Id, Card_Legacy_Id__c
                FROM User
                WHERE Card_Legacy_Id__c IN ({ids_str})
            """
            user_results = sf_target.query_all(soql_user)["records"]
            for r in user_results:
                if r.get("Card_Legacy_Id__c"):
                    owner_mapping[r["Card_Legacy_Id__c"]] = r["Id"]

        # Groups
        for oid in chunk:
            if oid.startswith("00G") and oid in group_mapping:
                owner_mapping[oid] = group_mapping[oid]

        # Fallback → Integration User
        for oid in chunk:
            if oid not in owner_mapping:
                owner_mapping[oid] = integration_user_id

    return owner_mapping

def fetch_service_appointment_ids(sf, sa_ids=None):
    """
    Fetch ServiceAppointment IDs based on two conditions:
    1. ParentRecord.RecordType.Name in ('Parent Company','Brand','Dealer')
    2. ParentRecordId in WorkOrders with specific RecordTypes + CreatedDate = LAST_N_YEARS:3
    Returns: set of ServiceAppointment Ids
    """
    related_ids = set()

    # Build optional Id filter
    id_filter = ""
    if sa_ids:
        ids_str = ",".join([f"'{pid}'" for pid in sa_ids])
        id_filter = f" AND Id IN ({ids_str})"

    # Condition 1
    query_sa1 = f"""
        SELECT Id
        FROM ServiceAppointment
        WHERE ParentRecord.RecordType.Name IN ('Parent Company','Brand','Dealer')
        {id_filter}
    """
    #print(f"[DEBUG] Fetching ServiceAppointment IDs (Condition 1): {query_sa1}")
    results_sa1 = sf.query_all(query_sa1)
    related_ids.update(r["Id"] for r in results_sa1["records"])

    # Condition 2
    query_sa2 = f"""
        SELECT Id
        FROM ServiceAppointment
        WHERE ParentRecordId IN (
            SELECT Id FROM WorkOrder
            WHERE Field_Win_Win__r.RecordType.DeveloperName IN ('Field_WIN_WIN','Gift_Card_Procurement','Incentive')
        )
        {id_filter}
    """
    #print(f"[DEBUG] Fetching ServiceAppointment IDs (Condition 2): {query_sa2}")
    results_sa2 = sf.query_all(query_sa2)
    related_ids.update(r["Id"] for r in results_sa2["records"])

    print(f"[INFO] Total ServiceAppointment IDs fetched: {len(related_ids)}")
    return related_ids


# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.path.join(BASE_DIR, "files")
os.makedirs(FILES_DIR, exist_ok=True)
