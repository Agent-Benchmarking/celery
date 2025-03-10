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
