.. _hello-world:

=================
 Hello World
=================

.. contents::
    :local:

Introduction
============

This tutorial will guide you through creating a simple "Hello World" application using Celery. 
By the end of this tutorial, you'll understand the basic concepts of Celery and be able to:

- Set up a Celery application
- Define a meaningful task
- Execute the task asynchronously
- Monitor task execution

Prerequisites
=============

This tutorial assumes you already have:

- Python installed and are familiar with creating Python projects
- RabbitMQ installed and running (default port 5672)
- Basic understanding of asynchronous processing concepts

Project Structure
================

Let's create a new project directory for our Celery demo:

.. code-block:: bash

    $ mkdir celery_demo
    $ cd celery_demo

Our project will have the following structure:

.. code-block:: text

    celery_demo/
    ├── celery_app.py    # Celery application configuration
    ├── tasks.py         # Task definitions
    └── run_tasks.py     # Script to call our tasks

Creating the Celery Application
===============================

First, let's create the Celery application configuration in ``celery_app.py``:

.. code-block:: python

    from celery import Celery

    # Create a Celery instance
    app = Celery(
        'celery_demo',                      # Name of the application
        broker='amqp://guest:guest@localhost:5672//',  # RabbitMQ URL
    )

    # Optional configuration
    app.conf.update(
        task_serializer='json',             # Tasks will be serialized using JSON
        accept_content=['json'],            # Worker accepts JSON-serialized tasks
        task_track_started=True,            # Track when tasks are started
        worker_concurrency=2,               # Number of worker processes
    )

    if __name__ == '__main__':
        app.start()

Defining Tasks
=============

Now, let's create more meaningful tasks in ``tasks.py``:

.. code-block:: python

    from celery_app import app
    import time
    import random
    from datetime import datetime

    @app.task(bind=True)
    def process_data(self, data_id, complexity=1):
        """
        Process a data item with the given ID and complexity.
        
        Args:
            data_id: Identifier for the data to process
            complexity: Processing complexity factor (1-5)
        
        Returns:
            dict: Processing results with metadata
        """
        # Log when the task starts
        start_time = datetime.now()
        print(f"[{start_time}] Processing data item {data_id} (complexity: {complexity})")
        
        # Simulate varying processing time based on complexity
        processing_time = complexity * random.uniform(0.5, 1.5)
        time.sleep(processing_time)
        
        # Simulate processing steps
        steps_completed = random.randint(3, 7)
        success_rate = min(100, 100 - (random.randint(0, complexity * 5)))
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"[{end_time}] Completed processing data item {data_id} in {duration:.2f}s")
        
        # Return processing results
        return {
            'data_id': data_id,
            'processing_time': duration,
            'steps_completed': steps_completed,
            'success_rate': success_rate,
            'complexity': complexity,
            'timestamp': datetime.now().isoformat()
        }

    @app.task
    def aggregate_results(result_list):
        """
        Aggregate multiple processing results.
        
        Args:
            result_list: List of processing result dictionaries
            
        Returns:
            dict: Aggregated statistics
        """
        if not result_list:
            return {'error': 'No results to aggregate'}
            
        total_time = sum(r['processing_time'] for r in result_list)
        avg_success = sum(r['success_rate'] for r in result_list) / len(result_list)
        
        return {
            'total_items': len(result_list),
            'total_processing_time': total_time,
            'average_processing_time': total_time / len(result_list),
            'average_success_rate': avg_success,
            'timestamp': datetime.now().isoformat()
        }

Running the Worker
=================

Open a terminal and start a Celery worker:

.. code-block:: bash

    $ celery -A celery_app worker --loglevel=info

You'll see output similar to this:

.. code-block:: text

    -------------- celery@hostname v5.2.7 (dawn-chorus)
    --- ***** ----- 
    -- ******* ---- Linux-5.15.0-56-generic-x86_64-with-glibc2.35 2023-03-08 12:34:56
    - *** --- * --- 
    - ** ---------- [config]
    - ** ---------- .> app:         celery_demo:0x7f8b5d3e9880
    - ** ---------- .> transport:   amqp://guest:**@localhost:5672//
    - ** ---------- .> results:     disabled://
    - *** --- * --- .> concurrency: 2 (prefork)
    -- ******* ---- .> task events: OFF (enable -E to monitor tasks in flower)
    --- ***** ----- 
     -------------- [queues]
                    .> celery           exchange=celery(direct) key=celery
    
    [tasks]
      . tasks.aggregate_results
      . tasks.process_data

This output confirms that:
- The worker is running
- It's connected to RabbitMQ
- It has discovered our two tasks

Calling Tasks
============

Now, let's create ``run_tasks.py`` to call our tasks:

.. code-block:: python

    from tasks import process_data, aggregate_results
    from celery import group
    import random

    def main():
        print("Submitting data processing tasks...")
        
        # Create a group of tasks with different data IDs and complexity levels
        data_ids = range(1, 6)  # Process 5 data items
        tasks = []
        
        for data_id in data_ids:
            # Randomly assign complexity between 1-5
            complexity = random.randint(1, 5)
            # Submit the task asynchronously
            task = process_data.delay(data_id, complexity)
            tasks.append(task)
            print(f"Submitted task for data_id={data_id}, complexity={complexity}, task_id={task.id}")
        
        print("\nWaiting for all tasks to complete...")
        
        # Collect all results
        results = []
        for task in tasks:
            try:
                result = task.get(timeout=30)  # Wait up to 30 seconds
                results.append(result)
                print(f"Task completed: data_id={result['data_id']}, "
                      f"time={result['processing_time']:.2f}s, "
                      f"success={result['success_rate']}%")
            except Exception as e:
                print(f"Task failed: {e}")
        
        # If we have results, aggregate them
        if results:
            print("\nSubmitting aggregation task...")
            agg_task = aggregate_results.delay(results)
            agg_result = agg_task.get(timeout=10)
            
            print("\nAggregated Results:")
            print(f"Total items processed: {agg_result['total_items']}")
            print(f"Total processing time: {agg_result['total_processing_time']:.2f}s")
            print(f"Average processing time: {agg_result['average_processing_time']:.2f}s")
            print(f"Average success rate: {agg_result['average_success_rate']:.2f}%")
        
    if __name__ == "__main__":
        main()

Run the script:

.. code-block:: bash

    $ python run_tasks.py

Understanding the Workflow
=========================

Let's visualize what happens when you run the tasks:

.. mermaid::

    sequenceDiagram
        participant Client as Client Script
        participant Broker as RabbitMQ
        participant Worker as Celery Worker
        
        Client->>+Broker: 1. Send task message
        Note right of Client: process_data.delay(data_id, complexity)
        Broker-->>-Client: Acknowledge receipt
        
        Worker->>+Broker: 2. Fetch task message
        Broker-->>-Worker: Return task message
        
        Worker->>Worker: 3. Execute task
        Note right of Worker: Process data with<br/>specified complexity
        
        Client->>+Broker: 4. Check for result
        Broker-->>-Client: No result yet
        
        Worker->>+Broker: 5. Task completed
        Note right of Worker: Return processing results
        
        Client->>+Broker: 6. Check for result again
        Broker-->>-Client: Return result
        
        Note over Client,Worker: Process repeats for each data item
        
        Client->>+Broker: 7. Send aggregation task
        Broker-->>-Client: Acknowledge receipt
        
        Worker->>+Broker: 8. Fetch aggregation task
        Broker-->>-Worker: Return task message
        
        Worker->>Worker: 9. Aggregate results
        
        Worker->>+Broker: 10. Aggregation completed
        Client->>+Broker: 11. Get aggregation result
        Broker-->>-Client: Return result

Key Components
-------------

.. mermaid::

    graph TD
        A[Client Application] -->|Sends tasks| B[Message Broker<br/>RabbitMQ]
        B -->|Delivers tasks| C[Celery Workers]
        C -->|Process tasks| D[Task Execution]
        D -->|Generate results| B
        B -->|Deliver results| A
        
        style A fill:#f9f,stroke:#333,stroke-width:2px
        style B fill:#bbf,stroke:#333,stroke-width:2px
        style C fill:#bfb,stroke:#333,stroke-width:2px
        style D fill:#fbb,stroke:#333,stroke-width:2px

This architecture demonstrates:

1. **Message Passing**: Tasks are sent as messages through RabbitMQ
2. **Decoupling**: The client doesn't need to know which worker processes the task
3. **Asynchronous Processing**: The client can continue working while tasks are processed

Next Steps
==========

Now that you've created your first Celery application, you can:

- Learn about more advanced features like periodic tasks, task routing, and error handling
- Explore the :doc:`task-cookbook` for more examples
- Read the :doc:`/userguide/index` for a comprehensive guide to Celery

Troubleshooting
===============

If you encounter issues:

- Make sure RabbitMQ is running (``rabbitmqctl status``)
- Check that your worker is running and connected to RabbitMQ
- Verify that your code matches the examples exactly
- Look at the worker logs for error messages
- Ensure you're in the correct directory when running the scripts
