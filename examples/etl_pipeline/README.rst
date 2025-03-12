======================
 ETL Pipeline Example
======================

This example demonstrates how to build ETL (Extract, Transform, Load) pipelines using Celery's Canvas feature. It shows how to use various canvas primitives (signatures, chains, groups, and chords) to create flexible data processing workflows.

Overview
========

The example implements three types of ETL pipelines:

1. **Simple Pipeline**: A linear sequence of tasks that extracts data from a file, processes it, and saves the result.
2. **Parallel Pipeline**: Extracts data from multiple files in parallel, then combines and processes the results.
3. **Complex Pipeline**: Extracts data from multiple sources (files and APIs), processes it, and loads it to multiple destinations (files and databases).

These pipelines showcase different aspects of Celery's Canvas API:

- **Signatures**: Used to represent individual tasks with their arguments
- **Chains**: Sequential execution of tasks
- **Groups**: Parallel execution of tasks
- **Chords**: Parallel tasks followed by an aggregation task

Features
========

- Extract data from files (CSV, JSON) and APIs
- Perform data cleaning, enrichment, and filtering operations
- Load data to files and databases
- Process data in parallel using groups
- Combine data from multiple sources using chords
- Create complex workflows by nesting canvas primitives

Requirements
===========

- Celery 
- Redis (for the result backend)
- RabbitMQ (for the message broker)

Getting Started
==============

1. Install a message broker (RabbitMQ or Redis):

   .. code-block:: bash

       # Install RabbitMQ (Ubuntu/Debian)
       $ sudo apt-get install rabbitmq-server

       # Start RabbitMQ
       $ sudo service rabbitmq-server start

2. Start a Celery worker:

   .. code-block:: bash

       $ cd examples/etl_pipeline
       $ celery -A tasks worker -l info

3. In a new terminal, run the example script:

   .. code-block:: bash

       $ cd examples/etl_pipeline
       $ python example.py

   This will:
   
   - Generate sample data files
   - Create and submit all three pipeline types
   - Print the task IDs for tracking

4. To run a specific pipeline type:

   .. code-block:: bash

       $ python example.py --pipeline simple
       $ python example.py --pipeline parallel
       $ python example.py --pipeline complex

5. Check the worker terminal for task progress and results.

Pipeline Details
==============

Simple Pipeline
--------------

.. code-block:: python

    chain(
        extract_from_file.s(input_file),
        clean_data.s(),
        enrich_data.s(),
        filter_data.s(min_value=min_value),
        load_to_file.s(output_file),
    )

The simple pipeline extracts data from a single file, cleans it, enriches it, filters it based on a minimum value, and then loads it to an output file.

Parallel Pipeline
---------------

.. code-block:: python

    chord(
        group(extract_from_file.s(filename) for filename in input_files),
        chain(
            clean_data.s(),
            enrich_data.s(),
            filter_data.s(min_value=min_value),
            load_to_file.s(output_file),
            summarize_etl_job.s(),
        )
    )

The parallel pipeline extracts data from multiple files simultaneously using a group, then processes and combines the results.

Complex Pipeline
--------------

.. code-block:: python

    chain(
        group(
            extract_from_file.s(input_file),
            extract_from_api.s(api_url)
        ),
        clean_data.s(),
        enrich_data.s(),
        filter_data.s(min_value=10),
        group(
            load_to_file.s(output_file),
            load_to_database.s(db_connection, table_name)
        ),
        summarize_etl_job.s()
    )

The complex pipeline extracts data from both a file and an API in parallel, processes the combined data, and then loads the results to both a file and a database in parallel.

Notes
=====

- This example uses simulated API and database operations to make it runnable without external dependencies.
- In a production environment, you would need to implement proper error handling, retries, and monitoring.
- For large datasets, you may need to process data in batches to avoid memory issues.

See Also
========

- Canvas User Guide: http://docs.celeryproject.org/en/latest/userguide/canvas.html
- Task Routing: http://docs.celeryproject.org/en/latest/userguide/routing.html
- Periodic Tasks: http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html 