#!/usr/bin/env python
"""
Example of using Celery canvas features to build an ETL pipeline.
This example demonstrates how to chain and group tasks for data processing.

This ETL pipeline example shows how to use Celery's powerful canvas features
to build a data processing workflow that:
1. Extracts data from multiple sources in parallel
2. Merges the extracted data
3. Transforms the data with computed fields and filtering
4. Loads the results into a JSON file

Key Celery Features Demonstrated:
- Parallel task execution using `group`
- Task chaining using the `|` operator
- Result passing between tasks
- Error handling and logging

Pipeline Flow:
    [extract(source_a), extract(source_b), extract(source_c)] (in parallel)
                           ↓
                     merge_data
                           ↓
                      transform
                           ↓
                        load

To run this example:

1. Start Redis (broker and result backend):
   ```bash
   redis-server
   ```

2. Start a Celery worker:
   ```bash
   celery -A examples.etl_example worker --loglevel=INFO
   ```

3. Run the example:
   ```bash
   python examples/etl_example.py
   ```

The pipeline will extract sample data from three sources in parallel,
merge the results, apply transformations (adding computed fields and filtering),
and save the final results to a timestamped JSON file.
"""

from celery import Celery
from celery import group
import json
import random
from datetime import datetime
from typing import List, Dict

# Initialize Celery app
app = Celery('etl_example')
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0'
)

# Simulated data sources
DATA_SOURCES = ['source_a', 'source_b', 'source_c']

@app.task(name='etl.extract')
def extract(source: str) -> List[Dict]:
    """
    Extract data from a simulated source.
    In a real application, this could be a database, API, or file.
    """
    # Simulate fetching data with random delay
    import time
    time.sleep(random.uniform(0.1, 0.5))
    
    # Generate sample data
    num_records = random.randint(5, 10)
    records = []
    
    for i in range(num_records):
        record = {
            'id': f'{source}_{i}',
            'timestamp': datetime.now().isoformat(),
            'value': random.randint(1, 100),
            'source': source
        }
        records.append(record)
    
    print(f'Extracted {len(records)} records from {source}')
    return records

@app.task(name='etl.transform')
def transform(data: List[Dict]) -> List[Dict]:
    """
    Transform the extracted data.
    This example adds some computed fields and filters data.
    """
    transformed_data = []
    
    for record in data:
        # Add computed fields
        transformed_record = record.copy()
        transformed_record['value_squared'] = record['value'] ** 2
        transformed_record['is_high_value'] = record['value'] > 50
        transformed_record['processed_at'] = datetime.now().isoformat()
        
        # Only keep records with even values
        if record['value'] % 2 == 0:
            transformed_data.append(transformed_record)
    
    print(f'Transformed data: kept {len(transformed_data)} out of {len(data)} records')
    return transformed_data

@app.task(name='etl.merge_data')
def merge_data(results: List[List[Dict]]) -> List[Dict]:
    """
    Merge data from multiple sources into a single list.
    """
    merged = []
    for result in results:
        merged.extend(result)
    
    print(f'Merged {len(merged)} total records')
    return merged

@app.task(name='etl.load')
def load(data: List[Dict]) -> str:
    """
    Load the transformed data into a destination.
    In this example, we'll just save to a JSON file.
    """
    output_file = f'etl_output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f'Loaded {len(data)} records to {output_file}')
    return f'Data successfully loaded to {output_file}'

def build_etl_pipeline():
    """
    Build and execute the ETL pipeline using Celery canvas features.
    """
    # Extract data from multiple sources in parallel
    extraction_tasks = [extract.s(source) for source in DATA_SOURCES]
    extraction_group = group(extraction_tasks)
    
    # Build the pipeline:
    # 1. Extract data from all sources (group)
    # 2. Merge the results
    # 3. Transform the merged data
    # 4. Load the final results
    pipeline = (
        extraction_group |
        merge_data.s() |
        transform.s() |
        load.s()
    )
    
    return pipeline

if __name__ == '__main__':
    # Build and execute the pipeline
    pipeline = build_etl_pipeline()
    result = pipeline.apply_async()
    
    print("Pipeline started! You can track the progress using the Celery worker logs.")
    print(f"Pipeline ID: {result.id}") 