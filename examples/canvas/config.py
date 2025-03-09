"""
Document Processing Pipeline - Configuration

This module defines configuration and constants used throughout the document processing pipeline.
"""

import uuid


def generate_document_id():
    """Generate a document ID using UUID4 in the format 'DOC_<uuid>'."""
    return f"DOC_{str(uuid.uuid4())}"


# Generate a list of sample document IDs
DOCUMENT_IDS = [generate_document_id() for _ in range(5)]

# Document categories
CATEGORIES = ["INVOICE", "CONTRACT", "REPORT", "FORM", "LETTER"]

# Document statuses
STATUSES = ["APPROVED", "PENDING", "REJECTED"]

# Processing results
RESULTS = {
    "PAYMENT_REQUIRED": "Document requires payment processing",
    "LEGAL_REVIEW": "Document needs legal review",
    "ARCHIVE": "Document should be archived",
    "STORE": "Document should be stored in the system",
    "SKIPPED": "Document processing was skipped due to validation issues",
}
