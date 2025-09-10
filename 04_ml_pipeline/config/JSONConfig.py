from prefect.blocks.core import Block
from pydantic import Field
from typing import Dict, Any


class JSONConfig(Block):
    """Custom block to storage JSON config file"""
    data: Dict[str, Any] = Field(..., description="Config fields for ML pipeline")
