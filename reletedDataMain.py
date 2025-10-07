#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import logging
from datetime import datetime

from Auth_Cred.auth import connect_salesforce
from Auth_Cred.config import SF_SOURCE, SF_TARGET
from reletedDataHelper import (migrate_attachments, migrate_files, migrate_feed, reset_file_migration_cache, CHUNK_SIZE_ACTIVITIES)
from utils.mappings import FILES_DIR

# === Input files for all activity types ===
INPUT_FILES = [
    os.path.join(FILES_DIR, "task_import_log.csv"),          # Task
    # os.path.join(FILES_DIR, "event_import_log.csv"),         # Event
    # os.path.join(FILES_DIR, "eventMessage_import.csv"),      # EventMessage
]
OUTPUT_FILE = os.path.join(FILES_DIR, "activity_related_migration.csv")
LOG_FILE = os.path.join(FILES_DIR, "activity_related_migration.log")

# === Enhanced logging setup ===
def setup_logging(log_level=logging.INFO, enable_debug=False):
    """Setup comprehensive logging with file and console handlers."""
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Set root logger level
    root_logger.setLevel(logging.DEBUG if enable_debug else log_level)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    simple_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )
    
    # File handler (detailed logging)
    file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG if enable_debug else logging.INFO)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler (simplified logging)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)
    
    # Log startup message
    logging.info("="*60)
    logging.info("Activity Related Migration Started")
    logging.info(f"Log level: {'DEBUG' if enable_debug else logging.getLevelName(log_level)}")
    logging.info("="*60)

# Initialize logging
setup_logging(logging.INFO, enable_debug=False)

def process_file(sf_source, sf_target, input_file):
    """Process one input mapping file for activity-related migration."""
    file_basename = os.path.basename(input_file)
    
    try:
        # Validate input file exists
        if not os.path.exists(input_file):
            logging.warning(f"Skipping missing file: {input_file}")
            return

        logging.info(f"Starting migration for file: {file_basename}")
        
        # Reset file migration cache for each new input file to prevent cross-file duplicates
        reset_file_migration_cache()
        
        # Read and validate input file
        try:
            df_map = pd.read_csv(input_file)
            logging.info(f"[{file_basename}] Loaded {len(df_map)} rows from input file")
        except Exception as e:
            logging.error(f"[{file_basename}] Failed to read CSV file: {e}")
            raise
        
        # Validate required columns
        required_columns = ["Source_Activity_Id", "Target_Activity_Id"]
        missing_columns = [col for col in required_columns if col not in df_map.columns]
        if missing_columns:
            raise ValueError(f"[{file_basename}] Missing required columns: {missing_columns}")
        
        # Data preprocessing and validation
        original_count = len(df_map)
        df_map = df_map[required_columns].dropna().drop_duplicates()
        cleaned_count = len(df_map)
        
        if original_count != cleaned_count:
            logging.info(f"[{file_basename}] Data cleanup: {original_count} → {cleaned_count} records (removed {original_count - cleaned_count} invalid/duplicate entries)")
        
        if cleaned_count == 0:
            logging.warning(f"[{file_basename}] No valid records found after cleanup")
            return
        
        # Build activity mapping
        activity_map = dict(zip(df_map["Source_Activity_Id"], df_map["Target_Activity_Id"]))
        src_ids_all = list(activity_map.keys())
        
        logging.info(f"[{file_basename}] Total activities to process: {len(src_ids_all)}")
        logging.info(f"[{file_basename}] Chunk size: {CHUNK_SIZE_ACTIVITIES}, Total chunks: {(len(src_ids_all) + CHUNK_SIZE_ACTIVITIES - 1) // CHUNK_SIZE_ACTIVITIES}")

        # Initialize tracking variables
        all_results = []
        file_map = {}  # Keep file_map persistent across all chunks
        total_processed = 0
        
        # Process in chunks
        for start in range(0, len(src_ids_all), CHUNK_SIZE_ACTIVITIES):
            chunk_num = (start // CHUNK_SIZE_ACTIVITIES) + 1
            total_chunks = (len(src_ids_all) + CHUNK_SIZE_ACTIVITIES - 1) // CHUNK_SIZE_ACTIVITIES
            src_chunk = src_ids_all[start:start + CHUNK_SIZE_ACTIVITIES]
            
            logging.info(f"[{file_basename}] Processing chunk {chunk_num}/{total_chunks} ({len(src_chunk)} activities)")
            
            try:
                # 1) Attachments
                logging.info(f"[{file_basename}] Migrating Attachments for chunk {chunk_num}...")
                attachments_before = len([r for r in all_results if r.get('Type') == 'Attachment'])
                migrate_attachments(sf_source, sf_target, src_chunk, activity_map, all_results)
                attachments_after = len([r for r in all_results if r.get('Type') == 'Attachment'])
                logging.info(f"[{file_basename}] Processed {attachments_after - attachments_before} attachments")

                # 2) Files
                logging.info(f"[{file_basename}] Migrating Files for chunk {chunk_num}...")
                files_before = len([r for r in all_results if r.get('Type') == 'File'])
                migrate_files(sf_source, sf_target, src_chunk, activity_map, all_results, file_map)
                files_after = len([r for r in all_results if r.get('Type') == 'File'])
                logging.info(f"[{file_basename}] Processed {files_after - files_before} files")

                # 3) Feed
                logging.info(f"[{file_basename}] Migrating Feed for chunk {chunk_num}...")
                feed_before = len([r for r in all_results if r.get('Type') in ['FeedItem', 'FeedComment']])
                migrate_feed(sf_source, sf_target, src_chunk, activity_map, all_results, file_map)
                feed_after = len([r for r in all_results if r.get('Type') in ['FeedItem', 'FeedComment']])
                logging.info(f"[{file_basename}] Processed {feed_after - feed_before} feed items/comments")

                total_processed += len(src_chunk)
                
                # Save checkpoint
                output_file = os.path.join(
                    FILES_DIR, f"{os.path.splitext(file_basename)[0]}_related_migration.csv"
                )
                pd.DataFrame(all_results).to_csv(output_file, index=False, encoding="utf-8-sig")
                logging.info(f"[{file_basename}] Checkpoint saved: {len(all_results)} total results → {os.path.basename(output_file)}")
                
            except Exception as e:
                logging.error(f"[{file_basename}] Error processing chunk {chunk_num}: {e}")
                logging.debug(f"[{file_basename}] Chunk {chunk_num} error details:", exc_info=True)
                # Continue with next chunk rather than failing entire file
                continue

        # Final summary
        success_count = len([r for r in all_results if r.get('Status') == 'Success'])
        failed_count = len([r for r in all_results if r.get('Status') == 'Failed'])
        
        logging.info(f"[{file_basename}] Migration completed!")
        logging.info(f"[{file_basename}] Summary: {success_count} successful, {failed_count} failed, {len(all_results)} total operations")
        logging.info(f"[{file_basename}] File mappings created: {len(file_map)}")
        
    except Exception as e:
        logging.error(f"[{file_basename}] Fatal error during file processing: {e}")
        logging.debug(f"[{file_basename}] Fatal error details:", exc_info=True)
        raise


def main():
    """Main entry point for the migration process."""
    start_time = datetime.now()
    
    try:
        # Connect to Salesforce orgs
        logging.info("Initializing Salesforce connections...")
        
        try:
            sf_source = connect_salesforce(SF_SOURCE)
            logging.info("✅ Connected to source org")
        except Exception as e:
            logging.error(f"❌ Failed to connect to source org: {e}")
            raise
            
        try:
            sf_target = connect_salesforce(SF_TARGET)
            logging.info("✅ Connected to target org")
        except Exception as e:
            logging.error(f"❌ Failed to connect to target org: {e}")
            raise
        
        logging.info("🚀 Starting migration process...")
        
        # Process each input file
        processed_files = 0
        failed_files = 0
        
        for input_file in INPUT_FILES:
            file_basename = os.path.basename(input_file)
            try:
                logging.info(f"📁 Processing file {processed_files + 1}/{len(INPUT_FILES)}: {file_basename}")
                process_file(sf_source, sf_target, input_file)
                processed_files += 1
                logging.info(f"✅ Successfully processed: {file_basename}")
            except Exception as e:
                failed_files += 1
                logging.error(f"❌ Failed to process {file_basename}: {e}")
                logging.debug(f"File processing error details:", exc_info=True)
                # Continue with next file rather than stopping entire process
                continue
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info("="*60)
        logging.info("🎉 MIGRATION PROCESS COMPLETED")
        logging.info(f"📊 Files processed: {processed_files}/{len(INPUT_FILES)}")
        logging.info(f"❌ Files failed: {failed_files}")
        logging.info(f"⏱️  Total duration: {duration}")
        logging.info("="*60)
        
        if failed_files > 0:
            logging.warning(f"⚠️  {failed_files} file(s) failed to process completely. Check logs for details.")
            
    except Exception as e:
        logging.error(f"💥 Fatal error in main process: {e}")
        logging.debug("Main process error details:", exc_info=True)
        raise



if __name__ == "__main__":
    main()
