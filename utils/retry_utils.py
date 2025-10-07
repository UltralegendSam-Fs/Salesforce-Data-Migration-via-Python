#!/usr/bin/env python3
"""
Simple retry utilities for Salesforce operations
Drop-in retry logic for any function
"""

import time
import logging
from functools import wraps
from typing import Callable, Any

def retry_on_failure(max_retries: int = 3, delay: int = 2, backoff_multiplier: float = 2.0):
    """
    Decorator for retrying functions on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Base delay between retries
        backoff_multiplier: Multiplier for exponential backoff
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"{func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    wait_time = delay * (backoff_multiplier ** attempt)
                    logger.warning(f"{func.__name__} attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            
            return None
        return wrapper
    return decorator

def retry_salesforce_operation(operation_func: Callable, *args, max_retries: int = 3, delay: int = 2, **kwargs) -> Any:
    """
    Retry a Salesforce operation with exponential backoff.
    
    Args:
        operation_func: Function to retry
        *args: Arguments for the function
        max_retries: Maximum number of retry attempts
        delay: Base delay between retries
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result of the operation or raises the last exception
    """
    logger = logging.getLogger(__name__)
    
    for attempt in range(max_retries + 1):
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Operation failed after {max_retries} retries: {e}")
                raise
            
            wait_time = delay * (2 ** attempt)
            logger.warning(f"Operation attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
    
    return None

def safe_bulk_insert(sf_connection, object_name: str, records: list, batch_size: int = 200, 
                    max_retries: int = 3, delay: int = 2) -> list:
    """
    Safely insert records with retry logic.
    
    Args:
        sf_connection: Salesforce connection
        object_name: Object name for bulk insert
        records: Records to insert
        batch_size: Batch size for insert
        max_retries: Maximum retry attempts
        delay: Base delay between retries
        
    Returns:
        List of insert results
    """
    logger = logging.getLogger(__name__)
    
    def _bulk_insert():
        return getattr(sf_connection.bulk, object_name).insert(records, batch_size=batch_size)
    
    try:
        return retry_salesforce_operation(_bulk_insert, max_retries=max_retries, delay=delay)
    except Exception as e:
        logger.error(f"Bulk insert failed after all retries: {e}")
        # Return failed results for all records
        return [{"success": False, "id": None, "errors": [str(e)]} for _ in records]

def safe_query(sf_connection, query: str, max_retries: int = 3, delay: int = 2) -> dict:
    """
    Safely execute SOQL query with retry logic.
    
    Args:
        sf_connection: Salesforce connection
        query: SOQL query string
        max_retries: Maximum retry attempts
        delay: Base delay between retries
        
    Returns:
        Query result dictionary
    """
    def _query():
        return sf_connection.query_all(query)
    
    return retry_salesforce_operation(_query, max_retries=max_retries, delay=delay)
