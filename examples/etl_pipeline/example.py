#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Example usage of the ETL pipeline with Celery Canvas.

This script demonstrates how to use the ETL pipelines defined in tasks.py.
It creates sample input data and runs various pipeline configurations.
"""
from __future__ import absolute_import, unicode_literals

import argparse
import json
import os
import random

from tasks import (
    create_simple_etl_pipeline,
    create_parallel_etl_pipeline,
    create_complex_etl_pipeline,
)


def ensure_dir(file_path):
    """Ensure directory exists for a file."""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)


def create_sample_data(data_dir):
    """Create sample data files for the ETL example."""
    # Convert to absolute path
    abs_data_dir = os.path.abspath(data_dir)
    output_dir = os.path.join(abs_data_dir, 'output')
    ensure_dir(output_dir)

    # Create sample input files
    sample_data = []

    # Create sample_data1.json
    for i in range(10):
        sample_data.append({
            "id": i,
            "name": f"Product {i}",
            "value": random.randint(0, 100),
            "category": random.choice(["A", "B", "C"]),
            "in_stock": random.choice([True, False])
        })

    json_file1 = os.path.join(abs_data_dir, 'sample_data1.json')
    with open(json_file1, 'w') as f:
        json.dump(sample_data, f, indent=2)

    # Create sample_data2.json with different structure
    sample_data2 = []
    for i in range(5):
        sample_data2.append({
            "id": i + 100,
            "name": f"Service {i}",
            "value": str(random.randint(0, 100)),  # String value to test conversion
            "availability": random.randint(1, 5)
        })

    json_file2 = os.path.join(abs_data_dir, 'sample_data2.json')
    with open(json_file2, 'w') as f:
        json.dump(sample_data2, f, indent=2)

    print(f"Created sample data files in {abs_data_dir}")
    return {
        'file1': json_file1,
        'file2': json_file2,
        'output_dir': output_dir
    }


def run_simple_pipeline(data_paths):
    """Run a simple ETL pipeline."""
    print("\n=== Running Simple ETL Pipeline ===")

    input_file = data_paths['file1']
    output_file = os.path.join(data_paths['output_dir'], 'simple_output.json')

    # Create and execute the pipeline
    pipeline = create_simple_etl_pipeline(input_file, output_file, min_value=30)

    # Show the pipeline structure
    print(f"Pipeline: {pipeline}")

    # Execute the pipeline
    result = pipeline.delay()

    print(f"Pipeline started with task ID: {result.id}")
    print("Check Celery worker logs for progress")

    # If you want to wait for the result, uncomment the following lines
    # print("Waiting for pipeline completion...")
    # output_file_path = result.get(timeout=60)
    # print(f"Pipeline completed. Output saved to: {output_file_path}")

    return result


def run_parallel_pipeline(data_paths):
    """Run a parallel ETL pipeline."""
    print("\n=== Running Parallel ETL Pipeline ===")

    input_files = [data_paths['file1'], data_paths['file2']]
    output_file = os.path.join(data_paths['output_dir'], 'parallel_output.json')

    # Create and execute the pipeline
    pipeline = create_parallel_etl_pipeline(input_files, output_file, min_value=20)

    # Show the pipeline structure
    print(f"Pipeline: {pipeline}")

    # Execute the pipeline
    result = pipeline.delay()

    print(f"Pipeline started with task ID: {result.id}")
    print("Check Celery worker logs for progress")

    return result


def run_complex_pipeline(data_paths):
    """Run a complex ETL pipeline."""
    print("\n=== Running Complex ETL Pipeline ===")

    input_file = data_paths['file1']
    api_url = "https://api.example.com/data"  # This is simulated in the task
    output_file = os.path.join(data_paths['output_dir'], 'complex_output.json')
    db_connection = "postgresql://user:password@localhost:5432/etl_db"  # Simulated
    table_name = "processed_data"

    # Create and execute the pipeline
    pipeline = create_complex_etl_pipeline(
        input_file, api_url, output_file, db_connection, table_name
    )

    # Show the pipeline structure
    print(f"Pipeline: {pipeline}")

    # Execute the pipeline
    result = pipeline.delay()

    print(f"Pipeline started with task ID: {result.id}")
    print("Check Celery worker logs for progress")

    return result


def main():
    """Main function to run the ETL examples."""
    parser = argparse.ArgumentParser(description='Run ETL pipeline examples with Celery Canvas')
    parser.add_argument(
        '--data-dir',
        default='./data',
        help='Directory for sample data (default: ./data)'
    )
    parser.add_argument(
        '--pipeline',
        choices=['simple', 'parallel', 'complex', 'all'],
        default='all',
        help='Pipeline to run (default: all)'
    )

    args = parser.parse_args()

    # Get the absolute path of the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # If data_dir is relative, make it absolute relative to the script directory
    if not os.path.isabs(args.data_dir):
        args.data_dir = os.path.join(script_dir, args.data_dir)

    # Create sample data
    data_paths = create_sample_data(args.data_dir)

    # Run the selected pipeline(s)
    if args.pipeline == 'simple' or args.pipeline == 'all':
        run_simple_pipeline(data_paths)

    if args.pipeline == 'parallel' or args.pipeline == 'all':
        run_parallel_pipeline(data_paths)

    if args.pipeline == 'complex' or args.pipeline == 'all':
        run_complex_pipeline(data_paths)

    print("\nAll requested pipelines have been submitted to Celery.")
    print("Check the Celery worker logs for execution details.")


if __name__ == '__main__':
    main()
