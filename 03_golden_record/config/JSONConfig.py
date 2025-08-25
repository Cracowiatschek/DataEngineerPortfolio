from prefect.blocks.core import Block
from pydantic import Field
from typing import List, Dict, Any


class JSONConfig(Block):
    """Custom block to storage JSON config file"""
    data: List[Dict[str, Any]] = Field(..., description="List of materialized views to refresh with sources")
