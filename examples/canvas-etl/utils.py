"""
utils.py

This module contains helper functions and mock data for the ETL example.
In a real application, these functions would interact with actual databases,
APIs, and file systems.
"""

import json
import logging
import random
import time
from datetime import datetime
from typing import Dict, List, Any, Union, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mock data for demonstration purposes
MOCK_CUSTOMER_DATA = [
    {"id": 1, "name": "Alice", "email": "alice@example.com", "country": "USA", "signup_date": "2023-01-15"},
    {"id": 2, "name": "Bob", "email": "bob@example.com", "country": "Canada", "signup_date": "2023-02-20"},
    {"id": 3, "name": "Charlie", "email": "charlie@example.com", "country": "UK", "signup_date": "2023-03-10"},
    {"id": 4, "name": "Diana", "email": "diana@example.com", "country": "Australia", "signup_date": "2023-04-05"},
    {"id": 5, "name": "Eve", "email": "eve@example.com", "country": "Germany", "signup_date": "2023-05-12"},
]

MOCK_ORDER_DATA = [
    {"id": 101, "customer_id": 1, "amount": 150.75, "date": "2023-06-01", "status": "completed"},
    {"id": 102, "customer_id": 2, "amount": 89.99, "date": "2023-06-02", "status": "pending"},
    {"id": 103, "customer_id": 1, "amount": 25.50, "date": "2023-06-03", "status": "completed"},
    {"id": 104, "customer_id": 3, "amount": 210.25, "date": "2023-06-04", "status": "completed"},
    {"id": 105, "customer_id": 4, "amount": 99.99, "date": "2023-06-05", "status": "shipped"},
    {"id": 106, "customer_id": 5, "amount": 175.00, "date": "2023-06-06", "status": "pending"},
    {"id": 107, "customer_id": 2, "amount": 45.25, "date": "2023-06-07", "status": "completed"},
    {"id": 108, "customer_id": 3, "amount": 125.50, "date": "2023-06-08", "status": "shipped"},
]

MOCK_PRODUCT_DATA = [
    {"id": 201, "name": "Laptop", "category": "Electronics", "price": 999.99, "stock": 50},
    {"id": 202, "name": "Smartphone", "category": "Electronics", "price": 499.99, "stock": 100},
    {"id": 203, "name": "Headphones", "category": "Accessories", "price": 99.99, "stock": 200},
    {"id": 204, "name": "T-shirt", "category": "Clothing", "price": 19.99, "stock": 500},
    {"id": 205, "name": "Jeans", "category": "Clothing", "price": 59.99, "stock": 300},
]

# Simulate database connections
def get_database_connection(db_name: str) -> Dict:
    """
    Simulate a database connection.
    
    Args:
        db_name: Name of the database to connect to
        
    Returns:
        A mock connection object
    """
    logger.info(f"Connecting to database: {db_name}")
    time.sleep(0.5)  # Simulate connection time
    return {"name": db_name, "connected": True, "timestamp": datetime.now().isoformat()}

# Simulate API requests
def api_request(endpoint: str, params: Optional[Dict] = None) -> Dict:
    """
    Simulate an API request.
    
    Args:
        endpoint: API endpoint
        params: Request parameters
        
    Returns:
        A mock API response
    """
    logger.info(f"Making API request to: {endpoint}")
    time.sleep(0.7)  # Simulate API request time
    
    if params:
        logger.info(f"With parameters: {params}")
    
    # Simulate different responses based on endpoint
    if endpoint == "customers":
        return {"status": "success", "data": MOCK_CUSTOMER_DATA}
    elif endpoint == "orders":
        return {"status": "success", "data": MOCK_ORDER_DATA}
    elif endpoint == "products":
        return {"status": "success", "data": MOCK_PRODUCT_DATA}
    else:
        return {"status": "error", "message": "Unknown endpoint"}

# Simulate file operations
def read_csv_file(filename: str) -> List[Dict]:
    """
    Simulate reading data from a CSV file.
    
    Args:
        filename: Name of the CSV file
        
    Returns:
        A list of dictionaries representing the rows of the CSV
    """
    logger.info(f"Reading CSV file: {filename}")
    time.sleep(0.5)  # Simulate file read time
    
    # Return different mock data based on filename
    if "customer" in filename.lower():
        return MOCK_CUSTOMER_DATA
    elif "order" in filename.lower():
        return MOCK_ORDER_DATA
    elif "product" in filename.lower():
        return MOCK_PRODUCT_DATA
    else:
        return []

def write_to_database(data: List[Dict], table_name: str) -> Dict:
    """
    Simulate writing data to a database table.
    
    Args:
        data: List of records to write
        table_name: Target table name
        
    Returns:
        A status dictionary
    """
    logger.info(f"Writing {len(data)} records to table: {table_name}")
    time.sleep(0.8)  # Simulate database write time
    
    # Simulate occasional failures
    if random.random() < 0.05:  # 5% chance of failure
        logger.error(f"Failed to write to table: {table_name}")
        raise Exception(f"Database write error for table {table_name}")
    
    return {
        "status": "success",
        "records_written": len(data),
        "table": table_name,
        "timestamp": datetime.now().isoformat()
    }

def export_to_json(data: List[Dict], filename: str) -> str:
    """
    Simulate exporting data to a JSON file.
    
    Args:
        data: Data to export
        filename: Target filename
        
    Returns:
        Path to the exported file
    """
    logger.info(f"Exporting data to JSON file: {filename}")
    time.sleep(0.3)  # Simulate file write time
    
    # In a real application, you would actually write to a file here
    return f"/path/to/exported/{filename}"

def partition_data(data: List[Dict], num_partitions: int) -> List[List[Dict]]:
    """
    Partition a dataset into multiple smaller chunks for parallel processing.
    
    Args:
        data: The dataset to partition
        num_partitions: Number of partitions to create
        
    Returns:
        A list of data partitions
    """
    if not data:
        return []
    
    # Ensure we don't create more partitions than we have data items
    num_partitions = min(num_partitions, len(data))
    
    # Calculate partition size
    partition_size = len(data) // num_partitions
    remainder = len(data) % num_partitions
    
    partitions = []
    start_idx = 0
    
    for i in range(num_partitions):
        # Add one extra item to the first 'remainder' partitions
        end_idx = start_idx + partition_size + (1 if i < remainder else 0)
        partitions.append(data[start_idx:end_idx])
        start_idx = end_idx
    
    return partitions

def simulate_long_running_job(duration: float = 2.0) -> None:
    """
    Simulate a long-running job that takes time to complete.
    
    Args:
        duration: Time in seconds for the job to run
    """
    logger.info(f"Starting long-running job (duration: {duration}s)")
    time.sleep(duration)
    logger.info("Long-running job completed")
