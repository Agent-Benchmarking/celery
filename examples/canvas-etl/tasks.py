"""
tasks.py

This module defines all the Celery tasks for the ETL process.
The tasks are organized into three categories:
- Extract tasks: Retrieve data from various sources
- Transform tasks: Process and clean the data
- Load tasks: Store the processed data in target destinations
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Union, Optional

from celery import shared_task, Task
from celery.exceptions import MaxRetriesExceededError

from utils import (
    get_database_connection, 
    api_request, 
    read_csv_file, 
    write_to_database,
    export_to_json,
    simulate_long_running_job
)

# Set up logging
logger = logging.getLogger(__name__)

# Custom base task class for common functionality
class ETLTask(Task):
    """Base task class with common ETL functionality."""
    
    # Default retry settings
    max_retries = 3
    default_retry_delay = 30  # 30 seconds
    
    # For demonstration, we're tracking task execution times
    def __call__(self, *args, **kwargs):
        """Override Task.__call__ to add timing and additional logging."""
        start_time = time.time()
        logger.info(f"Starting task {self.name}")
        
        try:
            # Execute the task
            result = super().__call__(*args, **kwargs)
            
            # Log completion time
            execution_time = time.time() - start_time
            logger.info(f"Task {self.name} completed in {execution_time:.2f} seconds")
            return result
            
        except Exception as exc:
            # Log failure
            execution_time = time.time() - start_time
            logger.error(
                f"Task {self.name} failed after {execution_time:.2f} seconds: {str(exc)}"
            )
            raise

# -------------------------------------------------------------------------
# Extract Tasks
# -------------------------------------------------------------------------

@shared_task(bind=True, base=ETLTask)
def extract_from_database(self, database_name: str, table_name: str, 
                         query_params: Optional[Dict] = None) -> List[Dict]:
    """
    Extract data from a database table.
    
    Args:
        database_name: Name of the database to connect to
        table_name: Name of the table to query
        query_params: Additional query parameters
        
    Returns:
        List of extracted records
    """
    logger.info(f"Extracting data from database: {database_name}, table: {table_name}")
    
    try:
        # In a real application, this would query an actual database
        connection = get_database_connection(database_name)
        
        # Simulating data extraction
        if table_name == "customers":
            from utils import MOCK_CUSTOMER_DATA
            data = MOCK_CUSTOMER_DATA
        elif table_name == "orders":
            from utils import MOCK_ORDER_DATA
            data = MOCK_ORDER_DATA
        elif table_name == "products":
            from utils import MOCK_PRODUCT_DATA
            data = MOCK_PRODUCT_DATA
        else:
            data = []
        
        # Simulate some processing time
        time.sleep(1.0)
        
        logger.info(f"Extracted {len(data)} records from {table_name}")
        return data
        
    except Exception as e:
        logger.error(f"Error extracting from database: {str(e)}")
        
        # Retry the task
        try:
            self.retry(exc=e, countdown=self.default_retry_delay * (2 ** self.request.retries))
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for database extraction: {database_name}.{table_name}")
            raise
            
@shared_task(bind=True, base=ETLTask)
def extract_from_api(self, api_endpoint: str, params: Optional[Dict] = None) -> Dict:
    """
    Extract data from an API endpoint.
    
    Args:
        api_endpoint: API endpoint to query
        params: Additional request parameters
        
    Returns:
        API response data
    """
    logger.info(f"Extracting data from API: {api_endpoint}")
    
    try:
        # In a real application, this would make an actual API request
        response = api_request(api_endpoint, params)
        
        if response.get("status") == "error":
            raise Exception(f"API error: {response.get('message', 'Unknown error')}")
            
        logger.info(f"Successfully extracted data from API: {api_endpoint}")
        return response.get("data", [])
        
    except Exception as e:
        logger.error(f"Error extracting from API: {str(e)}")
        
        # Retry the task
        try:
            self.retry(exc=e, countdown=self.default_retry_delay * (2 ** self.request.retries))
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for API extraction: {api_endpoint}")
            raise

@shared_task(bind=True, base=ETLTask)
def extract_from_csv(self, file_path: str) -> List[Dict]:
    """
    Extract data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of records from the CSV
    """
    logger.info(f"Extracting data from CSV: {file_path}")
    
    try:
        # In a real application, this would read an actual CSV file
        data = read_csv_file(file_path)
        
        logger.info(f"Extracted {len(data)} records from CSV: {file_path}")
        return data
        
    except Exception as e:
        logger.error(f"Error extracting from CSV: {str(e)}")
        
        # Retry the task
        try:
            self.retry(exc=e, countdown=self.default_retry_delay * (2 ** self.request.retries))
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for CSV extraction: {file_path}")
            raise

# -------------------------------------------------------------------------
# Transform Tasks
# -------------------------------------------------------------------------

@shared_task(bind=True, base=ETLTask)
def transform_customer_data(self, customer_data: List[Dict]) -> List[Dict]:
    """
    Transform customer data by cleaning and enriching it.
    
    Args:
        customer_data: Raw customer data
        
    Returns:
        Transformed customer data
    """
    logger.info(f"Transforming {len(customer_data)} customer records")
    
    transformed_data = []
    
    for customer in customer_data:
        # Create a copy to avoid modifying the original
        transformed = customer.copy()
        
        # Example transformations:
        
        # 1. Standardize country codes (e.g., United States -> US)
        if transformed.get("country") == "USA":
            transformed["country"] = "US"
        elif transformed.get("country") == "United Kingdom":
            transformed["country"] = "UK"
            
        # 2. Add a new field for the customer's full name (if first/last name are separate)
        if "first_name" in transformed and "last_name" in transformed:
            transformed["full_name"] = f"{transformed['first_name']} {transformed['last_name']}"
            
        # 3. Add a flag for customers from certain regions
        if transformed.get("country") in ["US", "Canada", "Mexico"]:
            transformed["region"] = "North America"
        elif transformed.get("country") in ["UK", "Germany", "France", "Spain", "Italy"]:
            transformed["region"] = "Europe"
            
        # 4. Parse dates into a standardized format
        if "signup_date" in transformed:
            try:
                # In a real application, you would use proper date parsing
                # For this example, we'll just assume the date is already in a good format
                transformed["signup_year"] = transformed["signup_date"].split("-")[0]
            except:
                transformed["signup_year"] = "Unknown"
                
        transformed_data.append(transformed)
    
    # Simulate some processing time
    time.sleep(0.5)
    
    logger.info(f"Transformed {len(transformed_data)} customer records")
    return transformed_data

@shared_task(bind=True, base=ETLTask)
def transform_order_data(self, order_data: List[Dict]) -> List[Dict]:
    """
    Transform order data by cleaning and enriching it.
    
    Args:
        order_data: Raw order data
        
    Returns:
        Transformed order data
    """
    logger.info(f"Transforming {len(order_data)} order records")
    
    transformed_data = []
    
    for order in order_data:
        # Create a copy to avoid modifying the original
        transformed = order.copy()
        
        # Example transformations:
        
        # 1. Convert amount to a proper numeric type (if it's a string)
        if isinstance(transformed.get("amount"), str):
            try:
                transformed["amount"] = float(transformed["amount"])
            except:
                transformed["amount"] = 0.0
                
        # 2. Categorize orders by amount
        amount = transformed.get("amount", 0)
        if amount < 50:
            transformed["amount_category"] = "small"
        elif amount < 150:
            transformed["amount_category"] = "medium"
        else:
            transformed["amount_category"] = "large"
            
        # 3. Standardize status values
        status = transformed.get("status", "").lower()
        if status in ["complete", "completed", "done", "finished"]:
            transformed["status"] = "completed"
        elif status in ["ship", "shipped", "shipping", "in transit", "in-transit"]:
            transformed["status"] = "shipped"
        elif status in ["pend", "pending", "wait", "waiting", "processing"]:
            transformed["status"] = "pending"
        else:
            transformed["status"] = "other"
            
        # 4. Parse order date
        if "date" in transformed:
            try:
                # In a real application, you would use proper date parsing
                date_parts = transformed["date"].split("-")
                transformed["order_year"] = date_parts[0]
                transformed["order_month"] = date_parts[1]
            except:
                transformed["order_year"] = "Unknown"
                transformed["order_month"] = "Unknown"
                
        transformed_data.append(transformed)
    
    # Simulate some processing time
    time.sleep(0.7)
    
    logger.info(f"Transformed {len(transformed_data)} order records")
    return transformed_data

@shared_task(bind=True, base=ETLTask)
def enrich_customer_orders(self, customer_data: List[Dict], order_data: List[Dict]) -> List[Dict]:
    """
    Enrich customer data with their order information.
    
    Args:
        customer_data: Customer records
        order_data: Order records
        
    Returns:
        Enriched customer data with order information
    """
    logger.info(f"Enriching {len(customer_data)} customers with {len(order_data)} orders")
    
    # Create a dictionary of customer orders
    customer_orders = {}
    for order in order_data:
        customer_id = order.get("customer_id")
        if customer_id:
            if customer_id not in customer_orders:
                customer_orders[customer_id] = []
            customer_orders[customer_id].append(order)
    
    # Enrich customer data with order information
    enriched_data = []
    for customer in customer_data:
        # Create a copy of the customer record
        enriched = customer.copy()
        
        # Add order information
        customer_id = customer.get("id")
        if customer_id in customer_orders:
            orders = customer_orders[customer_id]
            
            # Calculate total order amount
            total_amount = sum(order.get("amount", 0) for order in orders)
            enriched["total_order_amount"] = total_amount
            
            # Add number of orders
            enriched["order_count"] = len(orders)
            
            # Find latest order date
            order_dates = [order.get("date") for order in orders if "date" in order]
            if order_dates:
                enriched["latest_order_date"] = max(order_dates)
                
            # Calculate order status counts
            status_counts = {}
            for order in orders:
                status = order.get("status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1
            enriched["order_status_counts"] = status_counts
            
        else:
            # Customer has no orders
            enriched["total_order_amount"] = 0
            enriched["order_count"] = 0
            enriched["order_status_counts"] = {}
            
        enriched_data.append(enriched)
    
    # Simulate some processing time
    time.sleep(1.0)
    
    logger.info(f"Enriched {len(enriched_data)} customer records with order data")
    return enriched_data

@shared_task(bind=True, base=ETLTask)
def filter_data(self, data: List[Dict], filter_criteria: Dict) -> List[Dict]:
    """
    Filter data based on specified criteria.
    
    Args:
        data: Data to filter
        filter_criteria: Dictionary of field:value pairs to filter on
        
    Returns:
        Filtered data
    """
    logger.info(f"Filtering {len(data)} records with criteria: {filter_criteria}")
    
    filtered_data = []
    
    for item in data:
        # Check if the item matches all filter criteria
        include = True
        for field, value in filter_criteria.items():
            if field not in item or item[field] != value:
                include = False
                break
                
        if include:
            filtered_data.append(item)
    
    logger.info(f"Filtered to {len(filtered_data)} records")
    return filtered_data

@shared_task(bind=True, base=ETLTask)
def aggregate_data(self, data: List[Dict], group_by: str, 
                  aggregate_fields: List[Dict]) -> List[Dict]:
    """
    Aggregate data by grouping and applying aggregate functions.
    
    Args:
        data: Data to aggregate
        group_by: Field to group by
        aggregate_fields: List of dictionaries specifying field and function
                         e.g., [{"field": "amount", "function": "sum"}]
        
    Returns:
        Aggregated data
    """
    logger.info(f"Aggregating {len(data)} records by {group_by}")
    
    # Group data
    groups = {}
    for item in data:
        group_value = item.get(group_by)
        if group_value not in groups:
            groups[group_value] = []
        groups[group_value].append(item)
    
    # Apply aggregate functions
    results = []
    for group_value, group_items in groups.items():
        result = {group_by: group_value}
        
        for agg in aggregate_fields:
            field = agg["field"]
            function = agg["function"]
            
            if function == "count":
                result[f"{field}_count"] = len(group_items)
            elif function == "sum":
                result[f"{field}_sum"] = sum(item.get(field, 0) for item in group_items)
            elif function == "avg":
                values = [item.get(field, 0) for item in group_items]
                result[f"{field}_avg"] = sum(values) / len(values) if values else 0
            elif function == "min":
                values = [item.get(field, 0) for item in group_items]
                result[f"{field}_min"] = min(values) if values else 0
            elif function == "max":
                values = [item.get(field, 0) for item in group_items]
                result[f"{field}_max"] = max(values) if values else 0
        
        results.append(result)
    
    # Simulate some processing time
    time.sleep(0.5)
    
    logger.info(f"Created {len(results)} aggregate records")
    return results

# -------------------------------------------------------------------------
# Load Tasks
# -------------------------------------------------------------------------

@shared_task(bind=True, base=ETLTask)
def load_to_database_table(self, data: List[Dict], database_name: str, 
                           table_name: str) -> Dict:
    """
    Load data into a database table.
    
    Args:
        data: Data to load
        database_name: Target database name
        table_name: Target table name
        
    Returns:
        Status of the load operation
    """
    logger.info(f"Loading {len(data)} records to {database_name}.{table_name}")
    
    try:
        # In a real application, this would write to an actual database
        connection = get_database_connection(database_name)
        
        # Simulate writing to database
        result = write_to_database(data, table_name)
        
        logger.info(f"Successfully loaded {result['records_written']} records to {database_name}.{table_name}")
        return result
        
    except Exception as e:
        logger.error(f"Error loading to database: {str(e)}")
        
        # Retry the task
        try:
            self.retry(exc=e, countdown=self.default_retry_delay * (2 ** self.request.retries))
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for database load: {database_name}.{table_name}")
            raise

@shared_task(bind=True, base=ETLTask)
def export_json_file(self, data: List[Dict], filename: str) -> str:
    """
    Export data to a JSON file.
    
    Args:
        data: Data to export
        filename: Target filename
        
    Returns:
        Path to the exported file
    """
    logger.info(f"Exporting {len(data)} records to JSON file: {filename}")
    
    try:
        # In a real application, this would write to an actual file
        file_path = export_to_json(data, filename)
        
        logger.info(f"Successfully exported data to {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error exporting to JSON: {str(e)}")
        
        # Retry the task
        try:
            self.retry(exc=e, countdown=self.default_retry_delay * (2 ** self.request.retries))
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for JSON export: {filename}")
            raise

@shared_task(bind=True, base=ETLTask)
def send_notification(self, message: str, 
                     notification_type: str = "email") -> Dict:
    """
    Send a notification about the ETL process.
    
    Args:
        message: Notification message
        notification_type: Type of notification (email, slack, etc.)
        
    Returns:
        Status of the notification
    """
    logger.info(f"Sending {notification_type} notification: {message}")
    
    # Simulate sending notification
    time.sleep(0.3)
    
    return {
        "status": "success",
        "notification_type": notification_type,
        "message": message,
        "timestamp": datetime.now().isoformat()
    }
