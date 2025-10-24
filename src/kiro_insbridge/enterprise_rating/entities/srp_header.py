from pydantic import BaseModel, ConfigDict

from kiro_insbridge.enterprise_rating.entities.srp_request import SrpRequest


class SrpHeader(BaseModel):
    """SRP Header entity representing the export root."""

    srpheader: SrpRequest
    model_config = ConfigDict(from_attributes=True)
