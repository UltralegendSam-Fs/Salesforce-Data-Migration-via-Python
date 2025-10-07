# 🚀 Salesforce Data Migration via Python

Migrate Salesforce data between orgs using a robust set of Python scripts built on Simple Salesforce. Includes resilient querying, ID mapping helpers, and content/attachment handling.

<p align="center">
  <b>Source → Transform → Import</b>
</p>

---

## 📚 Table of Contents
- [✨ Features](#-features)
- [🧰 Prerequisites](#-prerequisites)
- [⚙️ Installation](#️-installation)
- [🔐 Environment Setup (.env)](#-environment-setup-env)
- [🗂️ Project Structure](#️-project-structure)
- [▶️ Quick Start](#️-quick-start)
- [🧪 Common Workflows](#-common-workflows)
- [📁 Files Directory](#-files-directory)
- [🛡️ Security Notes](#️-security-notes)
- [🪵 Logging & Retries](#-logging--retries)
- [🧯 Troubleshooting](#-troubleshooting)
- [📄 License](#-license)

---

## ✨ Features
- 🔑 Unified auth for Source/Target orgs via `Auth_Cred/auth.py`
- 🔄 Resilient SOQL with retries (`utils/retry_utils.py`)
- 🧭 Smart ID mapping utilities (`utils/mappings.py`)
- 📦 Content and attachment migration support
- 🧱 Works in manageable chunks to avoid limits

## 🧰 Prerequisites
- Python 3.10+
- API access to both Salesforce orgs (Source and Target)
- Integration user in Target org for fallback ownership mapping

## ⚙️ Installation
1. Clone the repository
2. Create and activate a virtual environment
```bash
python -m venv .venv
. .venv/Scripts/activate  # PowerShell: .venv\Scripts\Activate.ps1
```
3. Install dependencies
```bash
pip install -r requirements.txt
```

## 🔐 Environment Setup (.env)
Create a `.env` in the project root with the following:
```ini
# Source org
SF_SOURCE_USERNAME=
SF_SOURCE_PASSWORD=
SF_SOURCE_SECURITY_TOKEN=
SF_SOURCE_DOMAIN=login  # or 'test' for sandbox

# Target org
SF_TARGET_USERNAME=
SF_TARGET_PASSWORD=
SF_TARGET_SECURITY_TOKEN=
SF_TARGET_DOMAIN=login  # or 'test' for sandbox

# General
MIGRATION_CHUNK_SIZE=200
INTEGRATION_USER_ID=005XXXXXXXXXXXX  # target org fallback Owner/CreatedBy
```
💡 Tips:
- Use `login` for production and `test` for sandbox domains.
- `INTEGRATION_USER_ID` is used when a user/owner cannot be mapped.

## 🗂️ Project Structure
```
📦 Salesforce-Data-Migration-via-Python/
├── Auth_Cred/
│   ├── config.py        # Loads .env, exposes SF_SOURCE, SF_TARGET, Batch_Size
│   └── auth.py          # connect_salesforce(config) → returns authenticated client
│
├── utils/
│   ├── mappings.py      # ID mapping + file directory setup
│   └── retry_utils.py   # safe_query helper for resilient SOQL queries
│
├── activity_export1.py
├── activity_import2.py
├── FeedItemMigration.py
├── FeedCommentMigration.py
├── EmailMessageMigration.py
├── FetchAttch1.py
├── MigrateAttch2.py
├── fetchCDL1.py
├── CdlMigration.py
└── reletedDataMain.py
```

## ▶️ Quick Start
1. ✅ Configure `.env`
2. ✅ Activate venv
3. ✅ Run an export/import script
```bash
# Export activities from Source
python activity_export1.py

# Import activities into Target
python activity_import2.py
```

## 🧪 Common Workflows
- 🗓️ Activities
```bash
python activity_export1.py
python activity_import2.py
```
- 🧵 Feed items and comments
```bash
python FeedItemMigration.py
python FeedCommentMigration.py
```
- ✉️ Email Messages
```bash
python EmailMessageMigration.py
```
- 📎 Content & Attachments
```bash
python FetchAttch1.py
python MigrateAttch2.py
```
- 📇 CDL & Related Data
```bash
python fetchCDL1.py
python CdlMigration.py
python reletedDataMain.py
```

All scripts share:
- Auth via `Auth_Cred.auth.connect_salesforce`
- Config via `Auth_Cred.config.SF_SOURCE` and `Auth_Cred.config.SF_TARGET`
- Resilient SOQL via `utils.retry_utils.safe_query`
- Mapping helpers and `files/` path via `utils.mappings`

## ✅ Shared Components

- 🔐 Auth via `Auth_Cred.auth.connect_salesforce`
- ⚙️ Config via `Auth_Cred.config`
- 🧠 Resilient SOQL via `utils.retry_utils.safe_query`
- 🗃️ Mapping helpers & file directory via `utils.mappings`

## 📁 Files Directory
A `files/` folder is automatically created during runtime (via `utils/mappings.py`).
Make sure the process has write permissions ✍️

## 🧾 Logging & Retry Handling
- 🪵 Built-in logging to track migration progress
- 🔁 `safe_query` automatically retries transient Salesforce API errors
- 💡 Tip: redirect console output to a log file for large migrations

## 🔒 Security
- 🚫 Never commit your `.env` file
- 🔐 Credentials are loaded securely via `python-dotenv`
- ⚠️ `Auth_Cred/auth.py` disables TLS verification (`session.verify = False`)
  - For strict TLS, set `session.verify = True` or provide a CA bundle

## 🧩 Troubleshooting
**Common Issues & Fixes**

| Issue | Possible Fix |
| --- | --- |
| 🔑 Invalid login | Check username, password, and security token |
| 🚫 Owner mapping fallback | Verify `INTEGRATION_USER_ID` exists in target org |
| 🧩 Sandbox connection | Use `SF_*_DOMAIN=test` |
| ⚡ API limit errors | Reduce `MIGRATION_CHUNK_SIZE` and retry |

## 📜 License
🛡️ Proprietary / Internal Use Only. Unauthorized redistribution or modification is not allowed.

## ❤️ Contributing
Got ideas or improvements? Feel free to open a PR or share feedback — contributions are always welcome! 🙌
