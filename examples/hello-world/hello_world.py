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
