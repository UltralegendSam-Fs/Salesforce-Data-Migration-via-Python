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
