"""
Document Processing Pipeline - Data Models

This module defines the Pydantic models used for data validation in the document processing pipeline.
"""

from datetime import date
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class DocumentMetadata(BaseModel):
    """Document metadata model"""

    author: str
    created_date: date


class ValidationResult(BaseModel):
    """Document validation results"""

    quality_score: float = Field(ge=0, le=100)
    issues: List[str] = []


class Document(BaseModel):
    """Main document model with all attributes"""

    id: str
    category: str
    page_count: int = Field(gt=0)
    word_count: int = Field(gt=0)
    status: str
    metadata: DocumentMetadata
    validation: Optional[ValidationResult] = None
    processing_result: Optional[str] = None


class ProcessingReport(BaseModel):
    """Final processing report model"""

    total_documents: int
    categories: Dict[str, int]
    statuses: Dict[str, int]
    processing_results: Dict[str, int]
    average_quality_score: float
    documents: List[Document]
