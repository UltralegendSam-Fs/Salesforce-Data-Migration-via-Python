import os
import pandas as pd
import base64
import logging
import requests
from Auth_Cred.config import SF_SOURCE, SF_TARGET, MAPPING_FILE   # ← imported instead of redefining
from Auth_Cred.auth import connect_salesforce                     # ← imported instead of redefining

BATCH_LOG_INTERVAL = 50  # Log after every 50 uploads

# === Ensure files folder exists ===
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)

LOG_FILE = os.path.join(FILES_DIR, "migration.log")

# === Logging configuration ===
logging.basicConfig(filename=LOG_FILE,level=logging.INFO,format="%(asctime)s - %(message)s")


def download_attachment(sf, att_id):
    """Download attachment binary from source"""
    url = f"{sf.base_url}sobjects/Attachment/{att_id}/Body"
    headers = {"Authorization": f"Bearer {sf.session_id}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return base64.b64encode(response.content).decode("utf-8")
    else:
        logging.error(f"Failed to download {att_id}: {response.status_code} {response.text}")
        return None


def migrate_attachment(sf_target, parent_id, name, body_base64):
    """Create attachment in target"""
    data = {
        "ParentId": parent_id,
        "Name": name,
        "Body": body_base64
    }
    try:
        result = sf_target.Attachment.create(data)
        return result.get("id")
    except Exception as e:
        logging.error(f"Failed to create attachment for {parent_id}: {e}")
        return None


def main():
    # Step 1: Connect to both orgs
    sf_source = connect_salesforce(SF_SOURCE)
    sf_target = connect_salesforce(SF_TARGET)

    # Step 2: Read mapping file
    df = pd.read_excel(MAPPING_FILE)

    success_count = 0
    fail_count = 0

    for index, row in df.iterrows():
        attachment_id = row["AttachmentId"]
        target_parent_id = row["TargetParentId"]

        # Skip if target parent missing
        if pd.isna(target_parent_id) or not str(target_parent_id).strip():
            continue

        try:
            att_record = sf_source.Attachment.get(attachment_id)
            att_name = att_record["Name"]
            body_base64 = download_attachment(sf_source, attachment_id)

            if body_base64:
                new_att_id = migrate_attachment(sf_target, target_parent_id, att_name, body_base64)
                if new_att_id:
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1

        except Exception as e:
            logging.error(f"Error processing attachment {attachment_id}: {e}")
            fail_count += 1

        if (success_count + fail_count) % BATCH_LOG_INTERVAL == 0:
            logging.info(f"Processed {success_count + fail_count} attachments: Success={success_count}, Fail={fail_count}")

    logging.info(f"Migration complete. Success={success_count}, Fail={fail_count}")


if __name__ == "__main__":
    main()
