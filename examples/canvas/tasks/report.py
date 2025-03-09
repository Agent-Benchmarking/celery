"""
Document Processing Pipeline - Report Tasks

This module defines the Celery tasks for report generation and formatting.
"""

from typing import List, Union, Dict

from celery.utils.log import get_task_logger

from myapp import app
from models import Document, ProcessingReport

# Configure logging
logger = get_task_logger(__name__)


@app.task(name="report.aggregate", pydantic=True)
def aggregate_results(processed_docs: List[Union[Dict, Document]]) -> ProcessingReport:
    """
    Aggregate results from all documents into a final report.

    Args:
        processed_docs: List of processed documents (can be Document objects or dictionaries)

    Returns:
        ProcessingReport: Aggregated report
    """
    # Convert dictionaries to Document objects
    documents = []
    for doc in processed_docs:
        if isinstance(doc, dict):
            documents.append(Document(**doc))
        else:
            documents.append(doc)

    logger.info(f"Aggregating results from {len(documents)} documents")
    logger.debug(f"Documents to aggregate: {[doc.id for doc in documents]}")

    # Count documents by category
    category_counts = {}
    for doc in documents:
        category = doc.category
        category_counts[category] = category_counts.get(category, 0) + 1

    # Count documents by status
    status_counts = {}
    for doc in documents:
        status = doc.status
        status_counts[status] = status_counts.get(status, 0) + 1

    # Count documents by processing result
    result_counts = {}
    for doc in documents:
        result = doc.processing_result
        if result:
            result_counts[result] = result_counts.get(result, 0) + 1

    # Calculate average quality score
    avg_quality = sum(doc.validation.quality_score if doc.validation else 0 for doc in documents) / len(documents)

    # Create summary report
    report = ProcessingReport(
        total_documents=len(documents),
        categories=category_counts,
        statuses=status_counts,
        processing_results=result_counts,
        average_quality_score=round(avg_quality, 2),
        documents=documents,
    )

    logger.info(f"Report generated for {report.total_documents} documents")
    logger.debug(f"Category breakdown: {report.categories}")

    return report


@app.task(name="report.format", pydantic=True)
def format_report(report: ProcessingReport) -> str:
    """
    Format a user-friendly summary of the document processing report.

    Args:
        report: The document processing report

    Returns:
        str: Formatted report summary
    """
    logger.info("Formatting report summary")
    logger.debug(f"Report contains {report.total_documents} documents")

    # Create a formatted summary
    summary = [
        "=" * 60,
        "DOCUMENT PROCESSING SUMMARY",
        "=" * 60,
        f"Total documents processed: {report.total_documents}",
        f"Average quality score: {report.average_quality_score}%",
        "-" * 60,
        "Documents by category:",
    ]

    # Add category breakdown
    for category, count in report.categories.items():
        summary.append(f"  • {category}: {count}")

    summary.append("-" * 60)
    summary.append("Documents by status:")

    # Add status breakdown
    for status, count in report.statuses.items():
        summary.append(f"  • {status}: {count}")

    summary.append("-" * 60)
    summary.append("Processing results:")

    # Add processing results breakdown
    for result, count in report.processing_results.items():
        summary.append(f"  • {result}: {count}")

    summary.append("-" * 60)
    summary.append("Document details:")

    # Add document-specific data
    for doc in report.documents:
        summary.append(f"  • {doc.id} ({doc.category}):")
        summary.append(f"    - Status: {doc.status}")
        if doc.validation:
            summary.append(f"    - Quality: {doc.validation.quality_score}%")
        summary.append(f"    - Result: {doc.processing_result}")

    summary.append("=" * 60)

    result = "\n".join(summary)
    logger.info("Report summary generated")
    logger.debug(f"Report summary length: {len(result)} characters")

    return result
