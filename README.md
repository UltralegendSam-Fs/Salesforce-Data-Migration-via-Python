# Salesforce Data Migration via Python

A collection of Python scripts to export, transform, and import Salesforce data between orgs. It uses Simple Salesforce APIs with resilient querying, ID mapping helpers, and file/attachment handling.

## Prerequisites
- Python 3.10+
- Access to both source and target Salesforce orgs with API enabled
- Integration user in target org for fallback ownership mapping

## Installation
1. Clone the repo
2. Create and activate a virtual environment
```bash
python -m venv .venv
. .venv/Scripts/activate  # on Windows PowerShell: .venv\Scripts\Activate.ps1
```
3. Install dependencies
```bash
pip install -r requirements.txt
```

## Environment configuration
Create a `.env` file in the project root with the following variables:
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
Notes:
- `INTEGRATION_USER_ID` is used as a fallback when a user/owner mapping cannot be found in the target org.
- Domains: use `login` for production, `test` for sandbox.

## Project structure
- `Auth_Cred/`
  - `config.py`: loads `.env`, exposes `SF_SOURCE`, `SF_TARGET`, `Batch_Size`
  - `auth.py`: `connect_salesforce(config)` returns an authenticated `Salesforce` client
- `utils/`
  - `mappings.py`: helper functions for ID mappings and file directory setup
  - `retry_utils.py`: `safe_query` decorator/helper for resilient SOQL queries
- Top-level scripts handle different migration domains (activities, feeds, email messages, files/attachments, CDL, etc.)

## Usage
Activate your venv and run the scripts you need. Each script imports shared auth/config and utilities.

Examples:
```bash
# Export activities from source
python activity_export1.py

# Import activities into target
python activity_import2.py

# Migrate feed items and comments
python FeedItemMigration.py
python FeedCommentMigration.py

# Migrate email messages
python EmailMessageMigration.py

# Fetch/migrate content and attachments
python FetchAttch1.py
python MigrateAttch2.py

# CDL and related data flows
python fetchCDL1.py
python CdlMigration.py
python reletedDataMain.py
```

Most scripts use the shared helpers:
- Auth via `Auth_Cred.auth.connect_salesforce`
- Config via `Auth_Cred.config.SF_SOURCE` and `Auth_Cred.config.SF_TARGET`
- Resilient SOQL via `utils.retry_utils.safe_query`
- Mapping helpers and `files/` directory via `utils.mappings`

## Files directory
A `files/` directory is created automatically at runtime (see `utils/mappings.py`). Ensure the process has write permissions.

## Logging and retries
Several scripts use logging and the `safe_query` helper to retry transient API errors. Consider redirecting output to a file when running long migrations.

## Security
- Never commit your `.env` file.
- Credentials are read via environment variables using `python-dotenv`.
- `Auth_Cred/auth.py` disables TLS verification for the session (`session.verify = False`). If you require strict TLS, set `session.verify = True` or supply a CA bundle.

## Troubleshooting
- Ensure your security token is correct and your IP is allowed or the token is appended to password.
- If you see owner/user mapping fallbacks, verify `INTEGRATION_USER_ID` exists in target org.
- For sandboxes, set `SF_*_DOMAIN=test`.

## License
Proprietary/internal use unless specified otherwise.
