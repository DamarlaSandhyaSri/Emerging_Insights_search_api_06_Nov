from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class RequestModel(BaseModel):
    question: Optional[str] = Field(None, description="Text to search the documents against the OS")
    retrieval_strategy: Optional[str] = Field(None, description="Retrieveval strategy")
    # retrieval_index_name: Optional[str] = Field(None, description="OS index name")
    query_template_name: Optional[str] = Field(None, description="key identifier for query template")
    placeholder_values: Optional[Dict[str, Any]] = Field(None, description="Dictionary of placeholder values")
    # formatter: Optional[Any] = Field(None, description="Custom Object for Opensearch")
