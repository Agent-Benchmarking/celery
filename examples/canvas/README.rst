Celery Canvas: Document Processing Pipeline
=======================================

This example demonstrates how to use Celery's Canvas feature to create a powerful document processing pipeline.

What is Canvas?
--------------

Canvas is Celery's workflow design feature that allows you to chain, group, and combine tasks in complex ways. It's perfect for ETL (Extract, Transform, Load) workflows where you need to:

- Process data from multiple sources in parallel
- Apply transformations in sequence
- Aggregate results
- Handle dependencies between tasks

Canvas provides several powerful primitives:

- **chain**: Execute tasks sequentially (A → B → C)
- **group**: Run tasks in parallel ([A, B, C])
- **chord**: Run a group of tasks followed by a callback (([A, B, C] → D))
- **map**: Apply the same task to a sequence of values
- **chunks**: Split a long list of tasks into smaller groups

Project Structure
---------------

The example is organized into multiple files:

.. code-block:: text

    examples/canvas/
    ├── README.rst
    ├── requirements.txt
    ├── run.py                     # Script to run the pipeline
    ├── workflow_ascii.txt         # ASCII art workflow diagram
    ├── config.py                  # Configuration and constants
    ├── models.py                  # Data models using Pydantic
    ├── myapp.py                   # Celery app configuration
    └── tasks/                     # Task modules
        ├── __init__.py
        ├── document.py            # Document processing tasks
        └── report.py              # Report generation tasks

This Example
-----------

This example simulates a document processing pipeline:

1. **Extract**: Collect data from multiple documents in parallel
2. **Transform**: Validate and process each document
3. **Load**: Aggregate the results and generate a final report

Workflow Diagram
---------------

.. mermaid::

    graph TD
        A[process_documents] --> B[GROUP: document.extract]
        
        B --> C1[document.extract: DOC_001]
        B --> C2[document.extract: DOC_002]
        B --> C3[document.extract: DOC_003]
        B --> C4[document.extract: DOC_004]
        B --> C5[document.extract: DOC_005]
        
        C1 --> D1[document.validate]
        C2 --> D2[document.validate]
        C3 --> D3[document.validate]
        C4 --> D4[document.validate]
        C5 --> D5[document.validate]
        
        D1 --> E1[document.process]
        D2 --> E2[document.process]
        D3 --> E3[document.process]
        D4 --> E4[document.process]
        D5 --> E5[document.process]
        
        E1 & E2 & E3 & E4 & E5 --> F[CHORD: report.aggregate]
        
        F --> G[report.format]
        
        G --> H[Final Report]
        
        classDef group fill:#f9f,stroke:#333,stroke-width:2px;
        classDef task fill:#bbf,stroke:#333,stroke-width:1px;
        classDef chord fill:#bfb,stroke:#333,stroke-width:2px;
        
        class A,B group;
        class C1,C2,C3,C4,C5,D1,D2,D3,D4,D5,E1,E2,E3,E4,E5,G task;
        class F chord;

Running the Example
------------------

1. Set Up Your Environment
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Create and activate a virtual environment
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate

    # Install dependencies
    pip install -r requirements.txt

2. Start RabbitMQ
~~~~~~~~~~~~~~~~

Using Docker (recommended):

.. code-block:: bash

    docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management

The management interface will be available at http://localhost:15672 (guest/guest)

3. Start the Celery Worker
~~~~~~~~~~~~~~~~~~~~~~~~~

In a terminal window:

.. code-block:: bash

    # Start a worker with INFO level logging
    celery -A myapp worker -l INFO

    # Or start with more workers (e.g., 4 worker processes)
    celery -A myapp worker -l INFO --concurrency=4

    # For Windows users:
    celery -A myapp worker -l INFO --pool=solo

4. Run the Pipeline
~~~~~~~~~~~~~~~~~

Option 1: Using the provided script (recommended):

.. code-block:: bash

    python run.py

Option 2: Using the Python shell:

.. code-block:: python

    >>> from myapp import app
    >>> from tasks.document import process_documents
    >>> result = process_documents.delay()
    >>> # Wait for the result
    >>> print(result.get(timeout=60))  # 60 second timeout

Option 3: Using the Celery shell:

.. code-block:: bash

    celery -A myapp shell

Then in the shell:

.. code-block:: python

    >>> from tasks.document import process_documents
    >>> result = process_documents.delay()
    >>> print(result.get())

Implementation Details
--------------------

The workflow is defined in the ``process_documents`` task in ``tasks/document.py``:

.. code-block:: python

    @app.task(name="document.process_documents")
    def process_documents():
        # Step 1: Extract data from all documents in parallel (group)
        extraction = group(
            extract_document.s(doc_id) 
            for doc_id in DOCUMENT_IDS
        )
        
        # Step 2: For each document, validate and process (chain)
        document_workflows = [
            chain(
                validate_document.s(),
                process_document.s()
            ) 
            for _ in range(len(DOCUMENT_IDS))
        ]
        
        # Step 3: Aggregate results from all documents (chord)
        aggregation = chord(
            header=document_workflows,
            body=aggregate_results.s()
        )
        
        # Step 4: Format the final report (chain)
        final_workflow = chain(
            extraction,
            aggregation,
            format_report.s()
        )
        
        # Execute the workflow
        return final_workflow.delay()

Pydantic Integration
------------------

This example demonstrates Celery's Pydantic integration for data validation:

.. code-block:: python

    from pydantic import BaseModel, Field

    class Document(BaseModel):
        id: str
        category: str
        page_count: int = Field(gt=0)
        word_count: int = Field(gt=0)
        # ...

    @app.task(name="document.process", pydantic=True)
    def process_document(validation_result: Tuple[Document, float]) -> Document:
        document, quality_score = validation_result
        # ...
        return document

Error Handling and Monitoring
----------------------------

- Tasks can be monitored through the RabbitMQ management interface
- Use ``result.ready()`` to check if the pipeline has completed
- Individual task results can be accessed through ``result.parent``
- Set up retry policies using ``@app.task(retry=True, max_retries=3)``

Requirements
-----------

- Python 3.8+
- Celery 5.5.0rc5 or later
- RabbitMQ 3.8+ (for both broker and backend)
- Pydantic 2.5.2 or later
