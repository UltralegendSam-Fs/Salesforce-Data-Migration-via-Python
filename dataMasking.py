import math
import time
import csv
from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_TARGET

# === Salesforce Connection ===
sf_target = connect_salesforce(SF_TARGET)

# === Masking Utilities ===
def mask_phone(phone):
    return '0000000000' if phone else phone

def mask_email(email):
    return f"{email}.invalid" if email and not email.endswith('.invalid') else email

def mask_text(text_val):
    return 'MASKED' if text_val else text_val

# === Field definitions per object ===
FIELDS = {
    'Account': [
        'Phone','Fax','PersonMobilePhone','PersonHomePhone','PersonOtherPhone','PersonAssistantPhone',
        'PersonEmail','Support_Phone_Number__c','Support_Email_Address__c','Bank_Account_Number__c',
        'Bank_Routing_Number__c','Federal_Tax_ID__c','Last_4_of_SSN__c','DE_Business_Fax__c',
        'DE_Business_Phone__c','DE_Social_Security_Number__c','Medical_License_Number__c',
        'DE_Physician_License_Number__c','PayPal_Credit_Account_Number__c','PayPal_Credit_Account_Number_Key__c'
    ],
    'Contact': ['Phone','Fax','MobilePhone','HomePhone','OtherPhone','AssistantPhone','Email','Alt_Email__c','Social_Security_Number__c'],
    'Asset': ['Acceptance_Phone_Number__c'],
    'User': ['SenderEmail','Phone','Fax','MobilePhone']
}

# === Fields that cannot be used in WHERE ===
NON_FILTERABLE_FIELDS = {
    'Account': ['Bank_Account_Number__c','Bank_Routing_Number__c','PayPal_Credit_Account_Number__c'],
    'Contact': [],
    'Asset': [],
    'User': []
}

MASK_FUNCTIONS = {
    'Phone': mask_phone,
    'Fax': mask_phone,
    'PersonMobilePhone': mask_phone,
    'PersonHomePhone': mask_phone,
    'PersonOtherPhone': mask_phone,
    'PersonAssistantPhone': mask_phone,
    'Support_Phone_Number__c': mask_phone,
    'DE_Business_Fax__c': mask_phone,
    'DE_Business_Phone__c': mask_phone,
    'MobilePhone': mask_phone,
    'HomePhone': mask_phone,
    'OtherPhone': mask_phone,
    'AssistantPhone': mask_phone,
    'Acceptance_Phone_Number__c': mask_phone,
    
    'Email': mask_email,
    'SenderEmail': mask_email,
    'PersonEmail': mask_email,
    'Support_Email_Address__c': mask_email,
    'Alt_Email__c': mask_email,
    
    'Bank_Account_Number__c': mask_text,
    'Bank_Routing_Number__c': mask_text,
    'Federal_Tax_ID__c': mask_text,
    'Last_4_of_SSN__c': mask_text,
    'DE_Social_Security_Number__c': mask_text,
    'Medical_License_Number__c': mask_text,
    'DE_Physician_License_Number__c': mask_text,
    'PayPal_Credit_Account_Number__c': mask_text,
    'PayPal_Credit_Account_Number_Key__c': mask_text,
    'Social_Security_Number__c': mask_text
}

# === File to log failed records ===
FAILED_CSV = 'failed_records.csv'

# === Batch Processing Function with Retry and CSV Logging ===
def data_masking_batch(sobject_type, batch_size=5000, max_retries=3, retry_delay=5):
    fields = FIELDS.get(sobject_type)
    if not fields:
        print(f"No fields defined for {sobject_type}")
        return

    non_filterable = set(NON_FILTERABLE_FIELDS.get(sobject_type, []))
    filterable_fields = [f for f in fields if f not in non_filterable]

    where_clause = " OR ".join([f"{f} != NULL" for f in filterable_fields])
    query = f"SELECT Id, {', '.join(fields)} FROM {sobject_type}"
    if where_clause:
        query += f" WHERE ( {where_clause} ) AND Card_Legacy_Id__c != NULL"

    print(f"Querying {sobject_type}: {query}")
    records = sf_target.query_all(query)['records']
    total_records = len(records)
    print(f"Found {total_records} {sobject_type} records to mask.")

    total_batches = math.ceil(total_records / batch_size)
    
    for batch_num in range(total_batches):
        start_index = batch_num * batch_size
        end_index = start_index + batch_size
        batch = records[start_index:end_index]

        updates = []
        for rec in batch:
            updated = {'Id': rec['Id']}
            for field in fields:
                if field in rec:
                    updated[field] = MASK_FUNCTIONS.get(field, lambda x: x)(rec[field])
            updates.append(updated)

        # Retry logic
        retry_count = 0
        failed_records = updates
        while retry_count < max_retries and failed_records:
            results = sf_target.bulk.__getattr__(sobject_type).update(failed_records)
            temp_failures = []
            for idx, res in enumerate(results):
                if not res.get('success', False):
                    temp_failures.append({
                        'Id': failed_records[idx]['Id'],
                        'errors': res.get('errors')
                    })
            failed_records = temp_failures
            if failed_records:
                retry_count += 1
                print(f"Retry {retry_count} for {len(failed_records)} failed records...")
                time.sleep(retry_delay)
            else:
                break

        if failed_records:
            print(f"Batch {batch_num+1}/{total_batches} completed with {len(failed_records)} permanently failed records.")
            # Write failures to CSV
            with open(FAILED_CSV, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['Id', 'Object', 'Error'])
                if csvfile.tell() == 0:
                    writer.writeheader()
                for rec in failed_records:
                    writer.writerow({
                        'Id': rec['Id'],
                        'Object': sobject_type,
                        'Error': rec['errors']
                    })
        else:
            print(f"Batch {batch_num+1}/{total_batches} updated successfully.")

if __name__ == "__main__":
    data_masking_batch('Contact', batch_size=5000)
    data_masking_batch('Account', batch_size=5000)
    data_masking_batch('Asset', batch_size=5000)
    data_masking_batch('User', batch_size=5000)
