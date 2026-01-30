from __future__ import annotations

import json
from datetime import datetime, date
from enum import Enum
from typing import Any, ClassVar, Dict

import bittensor as bt
from pydantic import Field, ConfigDict, field_validator

# Import v1 models and mapping functions to expose them at the top level
from .models.v1 import common, odds, outcomes
from .mapping import v1 as mapping_v1


def _is_numpy_type(value: Any) -> bool:
    """Check if value is a numpy scalar without requiring numpy import."""
    type_name = type(value).__module__
    return type_name == "numpy" or type_name.startswith("numpy.")


def _coerce_to_python(value: Any) -> Any:
    """Convert numpy types to native Python types for JSON serialization."""
    if _is_numpy_type(value):
        # Handle numpy scalars (float64, int64, etc.)
        if hasattr(value, "item"):
            return value.item()
        # Fallback for numpy arrays (shouldn't hit this often)
        if hasattr(value, "tolist"):
            return value.tolist()
    return value


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


class SparketSynapseType(str, Enum):
    # miner -> validator (submissions)
    ODDS_PUSH = "odds_push"
    OUTCOME_PUSH = "outcome_push"
    # miner -> validator (data requests)
    GAME_DATA_REQUEST = "game_data_request"
    # validator -> miner
    CONNECTION_INFO_PUSH = "connection_info_push"
    

class SparketSynapse(bt.Synapse):
    """
    Sparket protocol synapse for validator<->miner communication.
    
    required_hash_fields specifies which fields are included in the body hash
    for signature verification. Without this, bittensor hashes an empty string,
    causing signature mismatch errors.
    """
    
    # Pydantic v2 config: serialize datetime objects as ISO strings
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None,
            date: lambda v: v.isoformat() if v else None,
        },
        arbitrary_types_allowed=True,
    )
    
    # Include our custom fields in the body hash for signature verification
    required_hash_fields: ClassVar[tuple[str, ...]] = ("type", "payload")
    
    type: SparketSynapseType | str = Field(default=SparketSynapseType.CONNECTION_INFO_PUSH)
    payload: Any = Field(default_factory=dict)

    @field_validator('payload', mode='before')
    @classmethod
    def _convert_payload_datetimes(cls, v: Any) -> Any:
        """Convert any datetime objects in payload to ISO strings before validation."""
        return cls._coerce_json(v)

    @staticmethod
    def _coerce_json(value: Any) -> Any:
        """Coerce values for JSON serialization.
        
        Handles:
        - SparketSynapseType enums → string values
        - datetime/date objects → ISO format strings
        - numpy scalars (float64, int64, etc.) → native Python types
        - Recursive dict/list traversal
        """
        if isinstance(value, SparketSynapseType):
            return value.value
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if _is_numpy_type(value):
            return _coerce_to_python(value)
        if isinstance(value, dict):
            return {k: SparketSynapse._coerce_json(v) for k, v in value.items()}
        if isinstance(value, list):
            return [SparketSynapse._coerce_json(v) for v in value]
        return value

    def model_dump(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        data["type"] = self._coerce_json(data.get("type"))
        data["payload"] = self._coerce_json(data.get("payload"))
        return data

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        return self.model_dump(*args, **kwargs)

    def serialize(self) -> Dict[str, Any]:
        return self.model_dump()

    def deserialize(self) -> Any:
        # Return payload as-is by default; callers can pattern-match by type
        return self.payload
