==============================================
 ETL Workflows with Celery Canvas
==============================================

Introduction
============

This example demonstrates how to implement Extract, Transform, Load (ETL) workflows 
using Celery's Canvas feature. Canvas is a powerful tool in Celery that allows you 
to build complex workflows by chaining, grouping, and combining tasks in various patterns.

In this example, we demonstrate several ETL patterns using different Canvas primitives:

- Chains: Sequential execution of tasks
- Groups: Parallel execution of tasks
- Chords: Parallel execution followed by an aggregation step
- Maps: Apply the same operation to a sequence of elements

Use Cases
=========

ETL processes are commonly used for:

- Data integration between different systems
- Data migration
- Analytics and reporting pipelines
- Data synchronization
- Regular data processing jobs

Prerequisites
============

To run this example, you need:

- Python 3.8+
- Celery 5.2+
- A message broker (RabbitMQ or Redis)
- Additional packages in requirements.txt

Installation
============

1. Set up a virtual environment::

    $ python -m venv venv
    $ source venv/bin/activate

2. Install dependencies::

    $ pip install -r requirements.txt

3. Make sure your broker (RabbitMQ or Redis) is running.

Running the Example
==================

1. Start the Celery worker::

    $ cd examples/canvas-etl
    $ /home/adrin/venvs/celery/bin/python -m celery -A etl_app worker -l INFO

2. In another terminal, run the ETL pipeline examples::

    $ cd examples/canvas-etl
    $ /home/adrin/venvs/celery/bin/python etl_app.py

Understanding the Code
=====================

- ``etl_app.py``: The main application that initializes Celery and contains examples of different ETL workflows
- ``tasks.py``: Contains all the task definitions (extract, transform, load)
- ``utils.py``: Helper functions and mock data for the ETL process

ETL Patterns Demonstrated
=========================

1. **Simple Pipeline**: A sequential chain of extract -> transform -> load
2. **Parallel Extraction**: Multiple data sources extracted in parallel, then combined
3. **Data Partitioning**: Processing large datasets by partitioning and parallel processing
4. **Conditional Pipeline**: Different transformations based on data characteristics
5. **Error Handling**: Demonstrating retry and error handling in ETL pipelines

Canvas Features Used
====================

- **chain**: For sequential execution of tasks (e.g., extract -> transform -> load)
- **group**: For parallel execution of tasks (e.g., extract from multiple sources)
- **chord**: For parallel execution followed by an aggregation step
- **map**: For applying a transformation to a collection of items

Best Practices
=============

This example also demonstrates best practices for Celery workflows:

- Proper error handling and retries
- State passing between tasks
- Result handling
- Task idempotency (safe to run multiple times)
- Task granularity (not too fine, not too coarse)
- Progress tracking
