

from pydantic import BaseModel, ConfigDict

from enterprise_rating.entities.srp_user import SrpUser


class SrpRequest(BaseModel):
    user : SrpUser
    prog_key: str
    build_type: str
    location: str
    carrier_id: str
    carrier_name: str
    line_id: str
    line_desc: str
    schema_id: str
    program_id: str
    program_name: str
    version_desc: str
    program_version: str
    parent_company: str
    notes: str | None = None
    date_created_split: str | None = None
    date_created: str | None = None
    model_config = ConfigDict(from_attributes=True)
