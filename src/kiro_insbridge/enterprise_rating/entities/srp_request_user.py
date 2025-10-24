"""SrpRequestUser"""

from pydantic import BaseModel, ConfigDict

class SrpRequestUser(BaseModel):
    """SRP Request User entity representing user information."""

    user_name: str | None = None
    full_name: str | None = None
    email_address: str | None = None
    model_config = ConfigDict(extra="ignore", from_attributes=True)