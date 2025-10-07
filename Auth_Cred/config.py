import os
from dotenv import load_dotenv

# Load environment variables from .env file located at project root
# This allows os.getenv(...) below to resolve values during local runs
load_dotenv()

# Try environment variables first, fallback to hardcoded values
SF_SOURCE = {
    "username": os.getenv("SF_SOURCE_USERNAME"),
    "password": os.getenv("SF_SOURCE_PASSWORD"),
    "security_token": os.getenv("SF_SOURCE_SECURITY_TOKEN"),
    "domain": os.getenv("SF_SOURCE_DOMAIN")
}

SF_TARGET = {
    "username": os.getenv("SF_TARGET_USERNAME"),
    "password": os.getenv("SF_TARGET_PASSWORD"),
    "security_token": os.getenv("SF_TARGET_SECURITY_TOKEN"),
    "domain": os.getenv("SF_TARGET_DOMAIN")
}

Batch_Size = int(os.getenv("MIGRATION_CHUNK_SIZE", "200"))
