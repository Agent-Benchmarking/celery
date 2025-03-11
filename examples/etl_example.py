#!/usr/bin/env python
"""
Example of using Celery canvas features to build an ETL pipeline.
This example demonstrates how to chain and group tasks for data processing.

This ETL pipeline example shows how to use Celery's powerful canvas features
to build a data processing workflow that:
1. Extracts data from multiple sources in parallel
2. Merges the extracted data
3. Transforms the data with computed fields and filtering (using map)
4. Loads the results in chunks to prevent memory exhaustion

Key Celery Features Demonstrated:
- Parallel task execution using `group`
- Task chaining using the `|` operator
- Immutable signatures using `si()`
- Error handling with callbacks
- Result passing between tasks
- Task ID inheritance (Celery 5.4+)
- Memory-efficient processing with `chunks`
- Record-level processing with `map`

Pipeline Flow:
    [extract(source_a), extract(source_b), extract(source_c)] (in parallel)
                           ↓
                     merge_data
                           ↓
                    map(transform)
                           ↓
                    chunks(load)

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
merge the results, transform each record individually using map,
and save the results in chunks to prevent memory exhaustion.
"""

from celery import Celery, group, chunks
import json
import random
from datetime import datetime
from typing import List, Dict, Any

# Initialize Celery app
app = Celery("etl_example")
app.conf.update(
    broker_url="redis://localhost:6379/0", result_backend="redis://localhost:6379/0"
)

# Configuration
DATA_SOURCES = ["source_a", "source_b", "source_c"]
CHUNK_SIZE = 100  # Number of records to process in each chunk
HIGH_VALUE_THRESHOLD = 50


@app.task(name="etl.extract")
def extract(source: str) -> List[Dict[str, Any]]:
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
            "id": f"{source}_{i}",
            "timestamp": datetime.now().isoformat(),
            "value": random.randint(1, 100),
            "source": source,
        }
        records.append(record)

    print(f"Extracted {len(records)} records from {source}")
    return records


@app.task(name="etl.transform")
def transform(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a single record by adding computed fields.
    This task is designed to be used with map() for record-level processing.
    """
    # Add computed fields
    transformed = record.copy()
    transformed["value_squared"] = record["value"] ** 2
    transformed["is_high_value"] = record["value"] > HIGH_VALUE_THRESHOLD
    transformed["processed_at"] = datetime.now().isoformat()

    # Only return records with even values
    if record["value"] % 2 == 0:
        return transformed
    return None


@app.task(name="etl.merge_data")
def merge_data(results: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Merge data from multiple sources into a single list.
    """
    merged = []
    for result in results:
        merged.extend(result)

    print(f"Merged {len(merged)} total records")
    return merged


@app.task(name="etl.filter_none")
def filter_none(results: List[Any]) -> List[Any]:
    """
    Filter out None values from the transformed results.
    """
    filtered = [r for r in results if r is not None]
    print(f"Filtered {len(filtered)} valid records from {len(results)} total")
    return filtered


@app.task(name="etl.load_chunk")
def load_chunk(chunk: List[Dict[str, Any]], chunk_num: int) -> str:
    """
    Load a chunk of transformed data into a destination.
    In this example, we'll save each chunk to a separate JSON file.
    """
    output_file = (
        f"etl_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}_chunk_{chunk_num}.json"
    )

    with open(output_file, "w") as f:
        json.dump(chunk, f, indent=2)

    print(f"Loaded chunk {chunk_num} with {len(chunk)} records to {output_file}")
    return output_file


@app.task(name="etl.handle_error")
def handle_error(request, exc, traceback):
    """
    Error callback that logs failed task information.
    """
    error_file = f"etl_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    with open(error_file, "w") as f:
        print(f"Task {request.id} failed: {exc}\\n{traceback}", file=f)
    return f"Error logged to {error_file}"


@app.task(name="etl.transform_batch")
def transform_batch(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform a batch of records by applying the transform function to each.
    Returns only the non-None results (records with even values).
    """
    transformed = []
    for record in records:
        result = transform(record)
        if result is not None:
            transformed.append(result)
    
    print(f"Transformed batch: kept {len(transformed)} out of {len(records)} records")
    return transformed


@app.task(name="etl.chunk_processor")
def chunk_processor(records: List[Dict[str, Any]]) -> List[str]:
    """
    Process a list of records in chunks.
    Returns the list of output files created.
    """
    output_files = []
    for i, chunk_start in enumerate(range(0, len(records), CHUNK_SIZE)):
        chunk = records[chunk_start:chunk_start + CHUNK_SIZE]
        output_file = load_chunk(chunk, i)
        output_files.append(output_file)
    return output_files


def build_etl_pipeline():
    """
    Build and execute the ETL pipeline using Celery canvas features.

    The pipeline demonstrates several Celery canvas features:
    1. Group for parallel extraction
    2. Chain for sequential steps
    3. Memory-efficient chunk processing
    4. Error callbacks for failure handling
    5. Task ID inheritance for the final tasks
    """
    # Extract data from multiple sources in parallel
    extraction_tasks = [extract.s(source) for source in DATA_SOURCES]
    extraction_group = group(extraction_tasks)

    # Build the pipeline:
    # 1. Extract data from all sources (group)
    # 2. Merge the results into a single list
    # 3. Transform records in batches
    # 4. Process and load in chunks
    pipeline = (
        extraction_group
        | merge_data.s()
        | transform_batch.s()
        | chunk_processor.s()
    ).on_error(handle_error.s())  # Add error handling

    return pipeline


if __name__ == "__main__":
    """
    Expected Output:
    1. Console output will show:
       - Number of records extracted from each source
       - Total records after merging
       - Number of valid records after filtering
       - Information about each chunk saved
    
    2. Generated files:
       - Multiple JSON files named 'etl_output_YYYYMMDD_HHMMSS_chunk_N.json'
         where N is the chunk number
       - Each JSON file contains a list of records with structure:
         {
             "id": "source_X_N",
             "timestamp": "ISO-8601 timestamp",
             "value": integer,
             "source": "source_X",
             "value_squared": integer,
             "is_high_value": boolean,
             "processed_at": "ISO-8601 timestamp"
         }
       - Only records with even values are included
    
    3. In case of errors:
       - Error files named 'etl_error_YYYYMMDD_HHMMSS.log' will be created
    """
    # Build and execute the pipeline
    pipeline = build_etl_pipeline()
    result = pipeline.apply_async()
    
    print("Pipeline started! You can track the progress using the Celery worker logs.")
    print(f"Pipeline ID: {result.id}")  # This will be the ID of the final task
