#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ETL pipeline tasks using Celery's Canvas feature.

This example demonstrates how to build a flexible ETL (Extract, Transform, Load)
pipeline using Celery's various canvas primitives like chains, groups, and chords.
"""
from __future__ import absolute_import, unicode_literals

import csv
import json
import os
import time
from datetime import datetime

from celery import Celery, chord, group, chain
from celery.utils.log import get_task_logger

# Create the Celery app
app = Celery('etl_pipeline')

# Configure the app
app.conf.update(
    broker_url='amqp://guest:guest@localhost:5672//',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    task_routes={
        'etl_pipeline.tasks.*': {'queue': 'etl_queue'}
    },
)

# Setup logging
logger = get_task_logger(__name__)


# Extract tasks
@app.task(bind=True, name='etl.extract.file')
def extract_from_file(self, filename, **kwargs):
    """Extract data from a file (csv, json, etc).

    Args:
        filename (str): Path to the input file

    Returns:
        list: Extracted raw data
    """
    logger.info(f"Extracting data from file: {filename}")

    # Simulate file reading
    try:
        file_ext = os.path.splitext(filename)[1].lower()
        data = []

        if file_ext == '.csv':
            # Read CSV file
            with open(filename, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(dict(row))
        elif file_ext == '.json':
            # Read JSON file
            with open(filename, 'r') as f:
                data = json.load(f)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        logger.info(f"Successfully extracted {len(data)} records from {filename}")
        return data
    except Exception as exc:
        logger.error(f"Error extracting data from {filename}: {exc}")
        raise


@app.task(bind=True, name='etl.extract.api')
def extract_from_api(self, api_url, **kwargs):
    """Extract data from an API.

    Args:
        api_url (str): URL of the API endpoint

    Returns:
        list: Extracted raw data
    """
    logger.info(f"Extracting data from API: {api_url}")

    # Simulate API request
    time.sleep(2)  # Simulate network latency

    # Mock data (in a real scenario, you would use requests.get() or similar)
    data = [{"id": i, "name": f"Item {i}", "value": i * 10} for i in range(1, 6)]

    logger.info(f"Successfully extracted {len(data)} records from API")
    return data


# Transform tasks
@app.task(bind=True, name='etl.transform.clean')
def clean_data(self, data, **kwargs):
    """Clean and standardize the data.

    Args:
        data (list): Raw data to clean, can be a list of dicts or a list of lists of dicts (from group)

    Returns:
        list: Cleaned data
    """
    logger.info(f"Cleaning data with {len(data)} records/groups")

    # If data is a list of lists (from a group of extract tasks), flatten it
    if data and isinstance(data[0], list):
        logger.info("Flattening nested results from group tasks")
        flattened_data = []
        for sublist in data:
            flattened_data.extend(sublist)
        data = flattened_data
        logger.info(f"Flattened into {len(data)} records")

    # Example cleaning operations:
    # 1. Remove empty records
    cleaned_data = [item for item in data if item]

    # 2. Standardize fields
    for item in cleaned_data:
        # Convert string numbers to actual numbers
        for key, value in item.items():
            if isinstance(value, str) and value.isdigit():
                item[key] = int(value)

        # Add timestamp
        item['processed_at'] = datetime.utcnow().isoformat()

    logger.info(f"Cleaned data, {len(cleaned_data)} records remain")
    return cleaned_data


@app.task(bind=True, name='etl.transform.enrich')
def enrich_data(self, data, **kwargs):
    """Enrich data with additional information.

    Args:
        data (list): Data to enrich

    Returns:
        list: Enriched data
    """
    logger.info(f"Enriching {len(data)} records")

    # Example enrichment operations:
    # Add calculated fields
    for item in data:
        if 'value' in item:
            item['value_squared'] = item['value'] ** 2

        # Add metadata
        item['enriched_at'] = datetime.utcnow().isoformat()

    logger.info(f"Enriched {len(data)} records")
    return data


@app.task(bind=True, name='etl.transform.filter')
def filter_data(self, data, min_value=0, **kwargs):
    """Filter data based on criteria.

    Args:
        data (list): Data to filter
        min_value (int): Minimum value to keep

    Returns:
        list: Filtered data
    """
    logger.info(f"Filtering {len(data)} records")

    # Example filter operation:
    filtered_data = [item for item in data if item.get('value', 0) > min_value]

    logger.info(f"Filtered data, {len(filtered_data)} records remain")
    return filtered_data


# Load tasks
@app.task(bind=True, name='etl.load.file')
def load_to_file(self, data, output_file, **kwargs):
    """Load data to a file.

    Args:
        data (list): Data to load
        output_file (str): Path to the output file

    Returns:
        str: Path to the output file
    """
    logger.info(f"Loading {len(data)} records to file: {output_file}")

    try:
        file_ext = os.path.splitext(output_file)[1].lower()

        if file_ext == '.csv':
            # Write to CSV file
            if data:
                fieldnames = data[0].keys()
                with open(output_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
        elif file_ext == '.json':
            # Write to JSON file
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        logger.info(f"Successfully loaded data to {output_file}")
        return output_file
    except Exception as exc:
        logger.error(f"Error loading data to {output_file}: {exc}")
        raise


@app.task(bind=True, name='etl.load.database')
def load_to_database(self, data, connection_string, table_name, **kwargs):
    """Load data to a database.

    Args:
        data (list): Data to load
        connection_string (str): Database connection string
        table_name (str): Name of the target table

    Returns:
        int: Number of records loaded
    """
    logger.info(f"Loading {len(data)} records to database table: {table_name}")

    # Simulate database insertion
    time.sleep(2)  # Simulate database operations

    logger.info(f"Successfully loaded {len(data)} records to database")
    return len(data)


# Summary task
@app.task(bind=True, name='etl.summarize')
def summarize_etl_job(self, results, **kwargs):
    """Summarize the ETL job results.

    Args:
        results (list): Results from previous tasks

    Returns:
        dict: Job summary
    """
    logger.info("Summarizing ETL job results")

    # Create a summary dictionary
    summary = {
        'job_id': self.request.id,
        'completed_at': datetime.utcnow().isoformat(),
        'results': results,
        'record_count': sum(result for result in results if isinstance(result, int)),
    }

    logger.info("ETL job completed")
    return summary


# Helper for pipeline creation
def create_simple_etl_pipeline(input_file, output_file, min_value=0):
    """Create a simple ETL pipeline using Celery Canvas.

    Args:
        input_file (str): Path to the input file
        output_file (str): Path to the output file
        min_value (int): Minimum value to include

    Returns:
        Signature: The ETL pipeline signature
    """
    # Create a chain of tasks
    return chain(
        extract_from_file.s(input_file),
        clean_data.s(),
        enrich_data.s(),
        filter_data.s(min_value=min_value),
        load_to_file.s(output_file),
    )


def create_parallel_etl_pipeline(input_files, output_file, min_value=0):
    """Create a parallel ETL pipeline using Celery Canvas.

    Args:
        input_files (list): List of input file paths
        output_file (str): Path to the output file
        min_value (int): Minimum value to include

    Returns:
        Signature: The ETL pipeline signature
    """
    # Create a group for parallel extraction
    extract_group = group(
        extract_from_file.s(filename) for filename in input_files
    )

    # Create a chord to process data in parallel and then combine results
    return chord(
        extract_group,
        chain(
            # This task will receive a list of results from all extract tasks
            clean_data.s(),
            enrich_data.s(),
            filter_data.s(min_value=min_value),
            load_to_file.s(output_file),
            summarize_etl_job.s(),
        )
    )


def create_complex_etl_pipeline(input_file, api_url, output_file, db_connection, table_name):
    """Create a complex ETL pipeline using Celery Canvas.

    This pipeline extracts data from both a file and an API, processes them in parallel,
    and then loads to both a file and a database.

    Args:
        input_file (str): Path to the input file
        api_url (str): URL of the API endpoint
        output_file (str): Path to the output file
        db_connection (str): Database connection string
        table_name (str): Name of the target table

    Returns:
        Signature: The ETL pipeline signature
    """
    # Extract data from two sources in parallel
    extract_group = group(
        extract_from_file.s(input_file),
        extract_from_api.s(api_url)
    )

    # Transform the data and load to two destinations
    # Using chord to wait for all extract tasks to complete
    return chord(
        extract_group,
        chain(
            # clean_data will properly flatten the results from extract_group
            clean_data.s(),
            enrich_data.s(),
            filter_data.s(min_value=10),
            # After transformations, create a group for parallel loading
            group(
                load_to_file.s(output_file),
                load_to_database.s(db_connection, table_name)
            ),
            summarize_etl_job.s()
        )
    )


if __name__ == '__main__':
    app.start()
