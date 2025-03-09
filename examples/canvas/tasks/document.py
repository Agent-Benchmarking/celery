"""
Document Processing Pipeline - Document Tasks

This module defines the Celery tasks for document extraction, validation, and processing.
"""

from datetime import date, timedelta
import random
from typing import List, Tuple

from celery import chain, group
from celery.utils.log import get_task_logger

from myapp import app
from config import CATEGORIES, DOCUMENT_IDS, STATUSES
from models import Document, DocumentMetadata, ValidationResult
from tasks.report import aggregate_results, format_report

# Configure logging
logger = get_task_logger(__name__)


@app.task(name="document.extract", pydantic=True)
def extract_document(doc_id: str) -> Document:
    """
    Extract data from a document.

    In a real-world scenario, this would connect to a document store.
    For this example, we generate randomized document data.

    Args:
        doc_id: The ID of the document to extract

    Returns:
        Document: The extracted document data
    """
    logger.debug(f"Starting extraction for document [{doc_id}]")

    # Enhanced category selection with weighted probabilities
    category_weights = {
        "CONTRACT": 0.15,
        "REPORT": 0.2,
        "INVOICE": 0.15,
        "EMAIL": 0.1,
        "MEMO": 0.1,
        "PROPOSAL": 0.1,
        "PRESENTATION": 0.05,
        "LEGAL": 0.05,
        "FORM": 0.05,
        "OTHER": 0.05,
    }
    category = random.choices(list(category_weights.keys()), weights=list(category_weights.values()))[0]

    # Enhanced status selection with more variation
    status_weights = {
        "PENDING": 0.2,
        "IN_PROGRESS": 0.3,
        "COMPLETED": 0.2,
        "REJECTED": 0.15,
        "ON_HOLD": 0.1,
        "ARCHIVED": 0.05,
    }
    status = random.choices(list(status_weights.keys()), weights=list(status_weights.values()))[0]

    # Dynamic page ranges based on document type
    page_ranges = {
        "CONTRACT": (15, 120),
        "REPORT": (20, 100),
        "INVOICE": (1, 5),
        "EMAIL": (1, 4),
        "MEMO": (1, 12),
        "PROPOSAL": (8, 60),
        "PRESENTATION": (8, 75),
        "LEGAL": (15, 200),
        "FORM": (1, 10),
        "OTHER": (1, 40),
    }
    min_pages, max_pages = page_ranges.get(category, (1, 30))
    page_count = random.randint(min_pages, max_pages)

    # Enhanced word count calculation
    words_per_page_ranges = {
        "CONTRACT": (300, 600),
        "REPORT": (250, 500),
        "INVOICE": (100, 300),
        "EMAIL": (150, 400),
        "MEMO": (200, 400),
        "OTHER": (200, 500),
    }
    min_words, max_words = words_per_page_ranges.get(category, (200, 500))
    words_per_page = random.randint(min_words, max_words)
    word_count = page_count * words_per_page + random.randint(-200, 200)
    word_count = max(100, word_count)

    # Generate realistic creation date within last year
    max_days_ago = 365
    random_days = random.randint(0, max_days_ago)
    created_date = date.today() - timedelta(days=random_days)

    document = Document(
        id=doc_id,
        category=category,
        page_count=page_count,
        word_count=word_count,
        status=status,
        metadata=DocumentMetadata(
            author=f"Author_{random.randint(1, 20)}",  # Increased author range
            created_date=created_date,
        ),
    )

    logger.info(
        f"📄 Extracted document [{doc_id}] | Type: {document.category} | "
        f"Pages: {document.page_count} | Status: {status}"
    )
    logger.debug(f"Document details: {document.model_dump_json(indent=2)}")

    return document


@app.task(name="document.validate", pydantic=True)
def validate_document(document: Document) -> Tuple[Document, float]:
    """
    Validate the document data and calculate a quality score.

    Args:
        document: Document data to validate

    Returns:
        Tuple[Document, float]: Validated document and quality score
    """
    doc_id = document.id
    logger.debug(f"Starting validation for document: {doc_id}")

    # Simple validation rules
    issues = []

    # These validations are redundant with Pydantic but included for demonstration
    if document.category not in CATEGORIES:
        issues.append("Unknown category")
        logger.warning(f"Document {doc_id} has unknown category: {document.category}")

    if document.status not in STATUSES:
        issues.append("Unknown status")
        logger.warning(f"Document {doc_id} has unknown status: {document.status}")

    # Calculate quality score (0-100%)
    max_issues = 2
    quality_score = 100 - (len(issues) / max_issues * 100) if max_issues > 0 else 100
    quality_score = round(quality_score, 2)

    # Add validation results to document
    document.validation = ValidationResult(quality_score=quality_score, issues=issues)

    logger.info(f"Validated document {doc_id}. Quality score: {quality_score}%")
    return document, quality_score


@app.task(name="document.process", pydantic=True)
def process_document(validation_result: Tuple[Document, float]) -> Document:
    """
    Process the validated document.

    Args:
        validation_result: Tuple of (document, quality_score)

    Returns:
        Document: The processed document
    """
    document, quality_score = validation_result
    doc_id = document.id

    logger.info(f"Processing document: {doc_id}")
    logger.debug(f"Document quality score: {quality_score}%")

    # Skip processing if quality score is too low
    if quality_score < 50:
        logger.warning(f"Document {doc_id} has low quality score ({quality_score}%). Skipping processing.")
        document.processing_result = "SKIPPED"
        return document

    # Simulate document processing
    if document.category == "INVOICE":
        document.processing_result = "PAYMENT_REQUIRED"
        logger.debug(f"Document {doc_id} is an invoice, marking for payment")
    elif document.category == "CONTRACT":
        document.processing_result = "LEGAL_REVIEW"
        logger.debug(f"Document {doc_id} is a contract, sending for legal review")
    elif document.status == "REJECTED":
        document.processing_result = "ARCHIVE"
        logger.debug(f"Document {doc_id} is rejected, archiving")
    else:
        document.processing_result = "STORE"
        logger.debug(f"Document {doc_id} processed with default action: store")

    logger.info(f"Processed document {doc_id}: {document.processing_result}")
    return document


@app.task(name="document.process_each")
def process_each(documents: List[Document]) -> List[Document]:
    """
    Process each document in the list through validation and processing.

    This task helps bridge the gap between the group of extract tasks
    and the individual validate/process tasks.

    Args:
        documents: List of documents to process

    Returns:
        List[Document]: List of processed documents
    """
    logger.info(f"Processing {len(documents)} documents individually")

    results = []
    for document in documents:
        # Process each document through validation and processing
        validated_doc, quality_score = validate_document(document)
        processed_doc = process_document((validated_doc, quality_score))
        results.append(processed_doc)

    return results


@app.task(name="document.process_documents")
def process_documents():
    """
    Main task that orchestrates the entire document processing pipeline using Canvas.

    This demonstrates how to use chains, groups, and chords to create
    a complex workflow that processes multiple documents with Pydantic validation.

    Returns:
        AsyncResult: Task result that will eventually contain the formatted report
    """
    logger.info("Starting document processing pipeline")
    logger.debug(f"Processing {len(DOCUMENT_IDS)} documents: {DOCUMENT_IDS}")

    # Step 1: Extract data from all documents in parallel (group)
    logger.debug("Creating parallel extraction tasks for documents")
    extraction = group(extract_document.s(doc_id) for doc_id in DOCUMENT_IDS)

    # Step 2: Process each document through validation and processing
    logger.debug("Setting up document processing")
    processing = chain(extraction, process_each.s())

    # Step 3: Aggregate results and format report
    logger.debug("Building final workflow chain")
    final_workflow = chain(processing, aggregate_results.s(), format_report.s())

    # Execute the workflow
    logger.info("Executing document processing workflow")
    return final_workflow.delay()  # Using delay() instead of apply_async() with no args
