"""
Document Processing Pipeline - Celery App Configuration

This module configures the Celery application for the document processing pipeline.
"""

from celery import Celery

app = Celery(
    "myapp",
    broker="amqp://guest@localhost//",
    backend="redis://localhost:6379/0",
    include=["tasks.document", "tasks.report"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    enable_utc=True,
    task_track_started=True,
    pydantic=True,
)

if __name__ == "__main__":
    app.start()
