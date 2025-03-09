"""
Document Processing Pipeline - Tasks

This package contains the Celery tasks used in the document processing pipeline.
"""
from tasks.document import process_documents

__all__ = ['process_documents']
