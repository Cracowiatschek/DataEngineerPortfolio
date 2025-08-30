from prefect.blocks.core import Block
from pydantic import Field
from typing import Dict, Any


class CityLocation(Block):
    """Custom block to storage JSON config file"""
    object: Dict[str, Any] = Field(..., description="Dict with path to city location object")
