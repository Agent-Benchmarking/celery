"""
etl_app.py

This is the main application file that demonstrates various ETL workflows
using Celery's Canvas feature. It showcases different patterns for building
data pipelines with Celery.

Usage:
    $ python etl_app.py

This will run several examples of ETL workflows, from simple to complex.
Each example illustrates a different pattern and set of Canvas features.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Union, Optional

from celery import Celery, chain, group, chord, signature
from celery.result import AsyncResult, GroupResult
from celery.utils.log import get_task_logger

from utils import partition_data, MOCK_CUSTOMER_DATA, MOCK_ORDER_DATA, MOCK_PRODUCT_DATA

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Celery app
app = Celery('etl_app')
# app.config_from_object('celery.conf.default')
app.conf.update(
    broker_url='memory://',  # Use in-memory broker for testing
    result_backend='cache',  # Use memory cache for results
    cache_backend='memory',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    result_expires=3600,  # Results expire after 1 hour
    task_always_eager=True,  # Run tasks locally and synchronously for testing
)

# Import tasks
from tasks import (
    # Extract tasks
    extract_from_database,
    extract_from_api,
    extract_from_csv,
    
    # Transform tasks
    transform_customer_data,
    transform_order_data,
    enrich_customer_orders,
    filter_data,
    aggregate_data,
    
    # Load tasks
    load_to_database_table,
    export_json_file,
    send_notification
)

# -------------------------------------------------------------------------
# ETL Pipeline Examples
# -------------------------------------------------------------------------

def simple_etl_pipeline():
    """
    Example 1: Simple sequential ETL pipeline
    
    This is the most basic ETL pattern: a simple chain of extract -> transform -> load.
    """
    logger.info("Running Example 1: Simple sequential ETL pipeline")
    
    # Define the workflow as a chain of tasks
    workflow = chain(
        # Extract data from a "customers" table in a database
        extract_from_database.s("sales_db", "customers"),
        
        # Transform customer data
        transform_customer_data.s(),
        
        # Load the transformed data to a data warehouse
        load_to_database_table.s("data_warehouse", "dim_customers")
    )
    
    # Execute the workflow
    result = workflow.apply_async()
    
    logger.info(f"Started simple ETL pipeline with ID: {result.id}")
    return result

def parallel_extract_etl_pipeline():
    """
    Example 2: Parallel extraction ETL pipeline
    
    This pattern extracts data from multiple sources in parallel,
    then combines the results for transformation and loading.
    
    Uses group() for parallel tasks and chord() to wait for all extracts
    before proceeding to transform and load.
    """
    logger.info("Running Example 2: Parallel extraction ETL pipeline")
    
    # Define parallel extract tasks
    extract_tasks = group(
        extract_from_database.s("sales_db", "customers"),
        extract_from_api.s("orders"),
        extract_from_csv.s("product_catalog.csv")
    )
    
    # Define the workflow as a chord
    # The chord will execute the parallel extract tasks,
    # then call enrich_customer_orders with the results as arguments
    workflow = chord(
        extract_tasks,
        # The callback receives a list of results from the group
        # In this case, [customer_data, order_data, product_data]
        # We only need the first two for our enrichment task
        enrich_customer_orders.s()  # This will get results as a list of arguments
        |  # Pipe the enriched customer data to the load task
        load_to_database_table.s("data_warehouse", "enriched_customers")
        |  # Send a notification upon completion
        send_notification.s("Customer data enrichment completed", "email")
    )
    
    # Execute the workflow
    result = workflow.apply_async()
    
    logger.info(f"Started parallel extraction ETL pipeline with ID: {result.id}")
    return result

def partitioned_data_etl_pipeline():
    """
    Example 3: Partitioned data ETL pipeline
    
    This pattern splits a large dataset into partitions for parallel
    processing, then aggregates the results.
    
    Demonstrates how to handle large datasets by processing chunks in parallel.
    """
    logger.info("Running Example 3: Partitioned data ETL pipeline")
    
    # Extract data first (in a real scenario, this might be a very large dataset)
    customer_data = MOCK_CUSTOMER_DATA  # For demonstration, we're using mock data
    
    # Partition the data into 2 chunks for parallel processing
    partitions = partition_data(customer_data, 2)
    
    # Create a group of transform tasks, one for each partition
    transform_tasks = group(
        transform_customer_data.s(partition) for partition in partitions
    )
    
    # Define a task to combine the results
    @app.task
    def combine_results(results):
        """Combine multiple result sets into one."""
        return [item for sublist in results for item in sublist]
    
    # Build a chord to process partitions in parallel and then aggregate results
    workflow = chord(
        transform_tasks,
        # The callback concatenates all the transformed partitions
        # and loads them to the database
        (
            combine_results.s()
            |  # Pipe the combined results to the load task
            load_to_database_table.s("data_warehouse", "dim_customers")
        )
    )
    
    # Execute the workflow
    result = workflow.apply_async()
    
    logger.info(f"Started partitioned data ETL pipeline with ID: {result.id}")
    return result

def conditional_etl_pipeline():
    """
    Example 4: Conditional ETL pipeline
    
    This pattern demonstrates how to implement conditional logic in a workflow.
    Based on data characteristics, different transformation paths are taken.
    """
    logger.info("Running Example 4: Conditional ETL pipeline")
    
    # First, extract order data
    order_data = MOCK_ORDER_DATA  # For demonstration, we're using mock data
    
    # Create a filter for different order types
    completed_orders_filter = {"status": "completed"}
    pending_orders_filter = {"status": "pending"}
    
    # Define workflow with conditional paths
    extract_task = extract_from_api.s("orders")
    
    # Process completed orders
    completed_orders_path = chain(
        filter_data.s(completed_orders_filter),
        transform_order_data.s(),
        load_to_database_table.s("data_warehouse", "completed_orders")
    )
    
    # Process pending orders
    pending_orders_path = chain(
        filter_data.s(pending_orders_filter),
        transform_order_data.s(),
        load_to_database_table.s("data_warehouse", "pending_orders")
    )
    
    # Create the full workflow
    # First extract, then process each order type in parallel
    workflow = chain(
        extract_task,
        group(
            completed_orders_path,
            pending_orders_path
        )
    )
    
    # Execute the workflow
    result = workflow.apply_async()
    
    logger.info(f"Started conditional ETL pipeline with ID: {result.id}")
    return result

def aggregation_etl_pipeline():
    """
    Example 5: Aggregation ETL pipeline
    
    This pattern demonstrates how to aggregate data as part of an ETL process.
    It extracts data, performs transformations, and then aggregates the results.
    """
    logger.info("Running Example 5: Aggregation ETL pipeline")
    
    # Define the workflow
    workflow = chain(
        # Extract data from the orders API
        extract_from_api.s("orders"),
        
        # Transform the order data
        transform_order_data.s(),
        
        # Perform aggregation on the transformed data
        # Group by country and aggregate amount (sum and avg)
        aggregate_data.s(
            group_by="status",
            aggregate_fields=[
                {"field": "amount", "function": "sum"},
                {"field": "amount", "function": "avg"},
                {"field": "amount", "function": "count"}
            ]
        ),
        
        # Export the aggregated data to a JSON file
        export_json_file.s("order_aggregates.json")
    )
    
    # Execute the workflow
    result = workflow.apply_async()
    
    logger.info(f"Started aggregation ETL pipeline with ID: {result.id}")
    return result

def error_handling_etl_pipeline():
    """
    Example 6: ETL pipeline with error handling
    
    This pattern demonstrates how to handle errors in an ETL pipeline.
    It includes retry logic and error reporting.
    """
    logger.info("Running Example 6: ETL pipeline with error handling")
    
    # Define the workflow
    workflow = chain(
        # Extract data (this task may fail and be retried)
        extract_from_database.s("problematic_db", "orders"),
        
        # Transform the data
        transform_order_data.s(),
        
        # Load the data
        load_to_database_table.s("data_warehouse", "dim_orders")
    )
    
    # Execute the workflow
    try:
        result = workflow.apply_async()
        logger.info(f"Started error handling ETL pipeline with ID: {result.id}")
        return result
    except Exception as e:
        logger.error(f"Failed to start error handling ETL pipeline: {str(e)}")
        
        # Send notification about the error
        send_notification.delay(
            f"ETL pipeline failed to start: {str(e)}",
            "email"
        )
        return None

# -------------------------------------------------------------------------
# Main Execution
# -------------------------------------------------------------------------

def run_all_examples():
    """Run all ETL pipeline examples."""
    logger.info("Starting ETL pipeline examples")
    
    examples = [
        simple_etl_pipeline,
        parallel_extract_etl_pipeline,
        partitioned_data_etl_pipeline,
        conditional_etl_pipeline,
        aggregation_etl_pipeline,
        error_handling_etl_pipeline
    ]
    
    results = []
    for i, example_func in enumerate(examples, 1):
        try:
            logger.info(f"Starting example {i}")
            result = example_func()
            results.append(result)
            
            # Give the tasks some time to execute (for demo purposes)
            # In a real scenario, you might want to wait for task completion
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error running example {i}: {str(e)}")
    
    logger.info("All examples have been started")
    
    # Return the results for reference
    return results

if __name__ == "__main__":
    results = run_all_examples()
    
    logger.info("Example ETL pipelines have been initiated")
    logger.info("Check Celery worker logs to see task execution details")
    
    # In a real application, you might want to add code here to wait for 
    # all pipelines to complete and report on their status
