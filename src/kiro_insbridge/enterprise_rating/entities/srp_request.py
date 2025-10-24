



from pydantic import BaseModel, ConfigDict

from kiro_insbridge.enterprise_rating.entities.srp_request_user import SrpRequestUser


class SrpRequest(BaseModel):
    schema: str | None = None
    prog_key: str | None = None
    build_type: str | None = None
    location: str | None = None
    carrier_id: str | None = None
    carrier_name: str | None = None
    line_id: str | None = None
    line_desc: str | None = None
    schema_id: str | None = None
    program_id: str | None = None
    program_name: str | None = None
    version_desc: str | None = None
    program_version: str | None = None
    parent_company: str | None = None
    notes: str | None = None
    date_created_split: str | None = None
    date_created: str | None = None
    model_config = ConfigDict(from_attributes=True)

class Srp(BaseModel):
    srp_header: SrpRequest
    srpuser: SrpRequestUser
    model_config = ConfigDict(extra="ignore", from_attributes=True)