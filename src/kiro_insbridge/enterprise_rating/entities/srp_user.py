from pydantic import BaseModel, ConfigDict


class SrpUser(BaseModel):
    user_name: str
    full_name: str
    email_address: str
    model_config = ConfigDict(from_attributes=True)
