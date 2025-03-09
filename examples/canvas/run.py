#!/usr/bin/env python
"""
Script to run the document processing pipeline.

This script demonstrates how to use the process_documents task
to run the workflow and get the results.
"""
from tasks.document import process_documents

if __name__ == "__main__":
    print("=" * 50)
    print("Document Processing Pipeline")
    print("=" * 50)
    print("\nInitializing pipeline...")
    print("- This will extract data from multiple documents")
    print("- Process the extracted information")
    print("- Generate a final report")

    print("\nLaunching workflow...")
    # Run the workflow and get task ID
    result = process_documents.delay()
    print(f"Task ID: {result.id}")
    print("Waiting for processing to complete...")

    # Wait for the result with timeout
    summary = result.get(timeout=60)  # Wait up to 60 seconds for completion

    print("\n" + "=" * 50)
    print("Processing Results")
    print("=" * 50)
    print(f"\n{summary}")

    print("\n" + "=" * 50)
    print("Pipeline completed successfully!")
    print("=" * 50)
