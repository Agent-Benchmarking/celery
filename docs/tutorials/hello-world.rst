.. _hello-world:

======================
Celery Hello World
======================

:Release: |version|
:Date: |today|

This tutorial provides a simple "Hello World" example to help beginners get started with Celery quickly. 
We'll create a minimal Celery application that sends and processes a simple greeting task.

Prerequisites
============

Before starting, make sure you have:

* Python 3.8 or newer installed
* A message broker (we'll use Redis in this example for simplicity)
* Basic familiarity with Python

Setting Up Your Environment
==========================

First, let's create a virtual environment and install the necessary packages:

.. code-block:: bash

    # Create a virtual environment
    $ python -m venv celery-hello-world
    $ cd celery-hello-world
    
    # Activate the virtual environment
    # On Windows:
    $ .\Scripts\activate
    # On macOS/Linux:
    $ source bin/activate
    
    # Install Celery and Redis
    $ pip install celery redis

Starting Redis
=============

For this tutorial, we'll use Redis as our message broker. You can run it using Docker:

.. code-block:: bash

    $ docker run -d -p 6379:6379 redis

If you don't have Docker, you can install Redis directly on your system by following the 
instructions at https://redis.io/download.

Creating Your First Celery Application
=====================================

Let's create a simple Celery application. Create a file named ``hello_world.py`` with the following content:

.. code-block:: python

    from celery import Celery
    
    # Create a Celery instance
    app = Celery('hello_world', 
                 broker='redis://localhost:6379/0',
                 backend='redis://localhost:6379/0')
    
    # Define a task
    @app.task
    def hello(name):
        return f"Hello, {name}!"
    
    # Optional: Configure some settings
    app.conf.update(
        result_expires=3600,  # Results expire after 1 hour
    )
    
    # This allows you to run the file directly
    if __name__ == '__main__':
        app.start()

Understanding the Code
=====================

Let's break down what's happening in this code:

1. We import the ``Celery`` class from the celery package.

2. We create a Celery application instance with three arguments:
   
   * ``'hello_world'`` - The name of our application
   * ``broker`` - The URL of our message broker (Redis)
   * ``backend`` - Where to store task results (also Redis)

3. We define a task called ``hello`` using the ``@app.task`` decorator. This task takes a name parameter and returns a greeting.

4. We configure some basic settings, like how long results should be stored.

Running the Celery Worker
========================

Now, let's start a Celery worker to process our tasks. Open a terminal, activate your virtual environment, and run:

.. code-block:: bash

    $ celery -A hello_world worker --loglevel=info

You should see output indicating that the worker has started and is ready to receive tasks.

Calling Tasks
============

Now, let's call our task and see the result. Open a new terminal window, activate your virtual environment, and start a Python interactive shell:

.. code-block:: bash

    $ python

In the Python shell, import your task and call it:

.. code-block:: python

    >>> from hello_world import hello
    >>> result = hello.delay('World')
    >>> result.get()
    'Hello, World!'

Congratulations! You've just created and executed your first Celery task.

Understanding What Happened
==========================

Let's understand the flow of what just happened:

1. You defined a task in your ``hello_world.py`` file.
2. You started a Celery worker that loaded your task definition.
3. In a separate Python session, you imported the task and called it with ``.delay()``.
4. The task was sent to the Redis broker.
5. The worker picked up the task from Redis and executed it.
6. The result was stored back in Redis.
7. When you called ``result.get()``, it retrieved the result from Redis.

Different Ways to Call Tasks
===========================

There are several ways to call tasks in Celery:

1. Using ``delay()``:

   .. code-block:: python
   
       >>> result = hello.delay('World')
   
   This is the simplest way to call a task.

2. Using ``apply_async()``:

   .. code-block:: python
   
       >>> result = hello.apply_async(args=['World'])
   
   This gives you more control over task execution.

3. With a countdown (delay):

   .. code-block:: python
   
       >>> result = hello.apply_async(args=['World'], countdown=10)
   
   This schedules the task to run 10 seconds in the future.

4. With an ETA (specific time):

   .. code-block:: python
   
       >>> from datetime import datetime, timedelta
       >>> eta = datetime.utcnow() + timedelta(minutes=1)
       >>> result = hello.apply_async(args=['World'], eta=eta)
   
   This schedules the task to run at a specific time.

Creating a More Complete Example
===============================

Let's expand our example to include multiple tasks. Create a new file named ``hello_app.py``:

.. code-block:: python

    from celery import Celery
    import time
    
    app = Celery('hello_app',
                 broker='redis://localhost:6379/0',
                 backend='redis://localhost:6379/0')
    
    @app.task
    def hello(name):
        return f"Hello, {name}!"
    
    @app.task
    def long_task(seconds):
        """A task that takes some time to complete"""
        time.sleep(seconds)
        return f"Task completed after {seconds} seconds"
    
    @app.task
    def process_greeting(greeting):
        """A task that processes the result of another task"""
        return f"Processed: {greeting.upper()}"
    
    if __name__ == '__main__':
        app.start()

Now you can run a worker for this application:

.. code-block:: bash

    $ celery -A hello_app worker --loglevel=info

And in another terminal, you can try chaining tasks:

.. code-block:: python

    >>> from hello_app import hello, process_greeting
    >>> from celery import chain
    
    # Chain tasks together
    >>> result = chain(hello.s('World'), process_greeting.s())()
    >>> result.get()
    'Processed: HELLO, WORLD!'

Monitoring Tasks
===============

You can monitor task execution in real-time using Flower, a web-based tool for Celery:

.. code-block:: bash

    $ pip install flower
    $ celery -A hello_app flower

Then open your browser to http://localhost:5555 to see the Flower dashboard.

Next Steps
=========

Now that you've created your first Celery application, you might want to explore:

* :ref:`first-steps` - A more detailed introduction to Celery
* :ref:`next-steps` - More advanced Celery features
* :ref:`task-cookbook` - Recipes for common task patterns

Troubleshooting
==============

If you encounter issues:

* Make sure Redis is running and accessible
* Check that your virtual environment is activated
* Verify that you're running the worker and client code in separate terminals
* Look at the worker logs for error messages

Remember that Celery tasks run asynchronously by default. If you need the result immediately, you can call ``result.get()``, but this blocks until the task completes, which defeats the purpose of asynchronous processing for long-running tasks.
