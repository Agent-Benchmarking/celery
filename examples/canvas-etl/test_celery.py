"""
Simple test script for Celery to verify it's working correctly.
"""

from celery import Celery

# Create a simple app with the memory-based broker
app = Celery('test_app')
app.conf.update(
    broker_url='memory://',
    result_backend='cache',
    cache_backend='memory',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    result_expires=3600,
)

@app.task
def add(x, y):
    """Simple task that adds two numbers."""
    return x + y

if __name__ == '__main__':
    # Test with eager execution (runs tasks immediately, no worker needed)
    app.conf.update(task_always_eager=True)
    
    # Run the task eagerly
    result = add.delay(4, 5)
    print(f"Task result: {result.get()}")
    
    print("Task completed successfully!")
