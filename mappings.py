import os
import re

def fetch_target_mappings(sf, object_name, source_parent_ids, batch_size):
    """Fetch target org record Ids by Legacy_ID__c"""
    print("fetch_target_mappings")
    mapping = {}
    SOQL_SIZE = 500
    parent_list = list(source_parent_ids)
    print(f"Parent List Size: {len(parent_list)}")

    for i in range(0, len(parent_list), SOQL_SIZE):
        chunk = parent_list[i : i + SOQL_SIZE]
        ids_str = ",".join([f"'{pid}'" for pid in chunk])
        soql = f"""
            SELECT Id, Card_Legacy_Id__c 
            FROM {object_name} 
            WHERE Card_Legacy_Id__c IN ({ids_str})
        """
        # print(f"soql: {soql}")
        print(f"[DEBUG] Fetching {object_name} mappings batch {i//SOQL_SIZE+1} (size {len(chunk)})")

        try:
            results = sf.query_all(soql)["records"]
            for r in results:
                if r.get("Card_Legacy_Id__c"):
                    mapping[r["Card_Legacy_Id__c"]] = r["Id"]
        except Exception as e:
            print(f"[ERROR] Failed fetching {object_name} mappings batch {i//SOQL_SIZE+1}: {e}")
            continue  

    return mapping

def fetch_user_ids(sf):
    """Fetch User Ids from a local file."""
    user_ids = set()
    soql = f"""
            SELECT Id, Card_Legacy_Id__c 
            FROM user
            WHERE Card_Legacy_Id__c != null
        """
    try:
        results = sf.query_all(soql)["records"]
        user_ids = {r["Card_Legacy_Id__c"] for r in results if r.get("Card_Legacy_Id__c")}
        print(f"[INFO] Fetched {len(user_ids)} User IDs from source org.")
    except Exception as e:
        print(f"[ERROR] Failed fetching user: {e}")
    
    return user_ids
          

def fetch_createdByIds(sf_target, createdByIds):
    """Fetch CreatedById and LastModifiedById mappings from User object."""
    batch_size = 500
    integration_user_id = "005DP000009MeHkYAK"  # Replace with actual integration user Id in target org
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
    integration_user_id = "005DP000009MeHkYAK"  # Replace with actual integration user Id in target org
    if not ownerIds:
        return owner_mapping

    id_list = list(ownerIds)
    batch_size = 500

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

def fetch_service_appointment_ids(sf, sa_ids=None, batch_size=500):
    """
    Fetch ServiceAppointment IDs based on two conditions:
    1. ParentRecord.RecordType.Name in ('Parent Company','Brand','Dealer')
    2. ParentRecordId in WorkOrders with specific RecordTypes
    Returns: set of ServiceAppointment Ids
    """
    related_ids = set()

    if sa_ids:
        sa_ids = list(sa_ids)
    else:
        sa_ids = [None]  # single run without filtering

    for i in range(0, len(sa_ids), batch_size):
        batch = sa_ids[i : i + batch_size]
        id_filter = ""
        if batch and batch[0] is not None:
            ids_str = ",".join([f"'{pid}'" for pid in batch])
            id_filter = f" AND Id IN ({ids_str})"

        # Condition 1
        query_sa1 = f"""
            SELECT Id  FROM ServiceAppointment
            WHERE ParentRecordId IN (Select Id from Account Where RecordType.Name IN ('Parent Company','Brand','Dealer') AND IsPersonAccount = false AND DE_Is_Shell_Account__c = false)
            {id_filter}
        """
        try:
            # print(f"[DEBUG] Fetching ServiceAppointment (Condition 1) batch {i//batch_size+1}: {query_sa1}...")
            results_sa1 = sf.query_all(query_sa1)
            related_ids.update(r["Id"] for r in results_sa1["records"])
        except Exception as e:
            print(f"[ERROR] Failed fetching ServiceAppointment (Condition 1) batch {i//batch_size+1}: {e}")
            continue

        # Condition 2
        query_sa2 = f"""
            SELECT Id
            FROM ServiceAppointment
            WHERE ParentRecordId IN (
                SELECT Id
                FROM WorkOrder
                WHERE Field_Win_Win__r.RecordType.Name IN ('Field Win Win','Gift Card Procurement','Incentive')
                AND Account.RecordType.Name IN ('Parent Company','Brand','Dealer')
            )
            {id_filter}
        """
        try:
            # print(f"[DEBUG] Fetching ServiceAppointment (Condition 2) batch {i//batch_size+1}: {query_sa2}...")
            results_sa2 = sf.query_all(query_sa2)
            related_ids.update(r["Id"] for r in results_sa2["records"])
        except Exception as e:
            print(f"[ERROR] Failed fetching ServiceAppointment (Condition 2) batch {i//batch_size+1}: {e}")
            continue

    print(f"[INFO] Total ServiceAppointment IDs fetched: {len(related_ids)}")
    return related_ids

def related_recordid_mapping(sf_source,sf_target,records,object_type):
    doc_ids = set()
    createdBy_ids = set()
    createdBy_mappings = {}

    createdBy_ids = {rec["CreatedById"] for rec in records if rec.get("CreatedById")}
    createdBy_mappings = fetch_createdByIds(sf_target, createdBy_ids)


    for rec in records:
        rec["CreatedById"] = createdBy_mappings.get(rec.get("CreatedById"), None)
        if object_type=="Comment":
            body = rec.get("CommentBody") or ""
        else:
            body = rec.get("Body") or ""
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        for doc_id in matches:
            doc_ids.add(doc_id)

    if not doc_ids:
        print("⚠️ No <img> tags found in FeedItem bodies, skipping RelatedRecordId mapping")
        return records  # return unchanged
    
    # Step 2: Fetch latest ContentVersion for all unique ContentDocumentIds
    content_map = {}  # {ContentDocumentId: ContentVersionId}

    if doc_ids:
        ids_str = ",".join([f"'{d}'" for d in doc_ids])
        ver_soql = f"""
            SELECT ContentDocumentId, Id
            FROM ContentVersion
            WHERE ContentDocumentId IN ({ids_str}) AND IsLatest = true
        """
        ver_q = sf_source.query_all(ver_soql)["records"]
        
        for v in ver_q:
            content_map[v["ContentDocumentId"]] = v["Id"]

    # Step 3: Update each record with RelatedRecordId if image found
    for rec in records:
        if object_type=="Comment":
            body = rec.get("CommentBody") or ""
            new_body = re.sub(r'<img[^>]*>(?:</img>)?', '', body, flags=re.IGNORECASE)
            rec["CommentBody"] = new_body.strip()

        else:
            body = rec.get("Body") or ""
            new_body = re.sub(r'<img[^>]*>(?:</img>)?', '', body, flags=re.IGNORECASE)
            rec["Body"] = new_body.strip()
            
        matches = re.findall(r'<img[^>]+src="sfdc://([^"]+)"', body)
        if matches:
            doc_id = matches[0]  # pick first if multiple
            if doc_id in content_map:
                rec["RelatedRecordId"] = content_map[doc_id]
                
                print(f"🔗 Mapped FeedItem {rec}")
    return records

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.path.join(BASE_DIR, "files")
os.makedirs(FILES_DIR, exist_ok=True)
