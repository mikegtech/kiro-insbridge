from pydantic import BaseModel, ConfigDict


class Input(BaseModel):
    """Represents a input."""

    line: str
    index: int
    data_type: str
    description: str
    category_id: str
    system_var: str
    qual_type: str
    model_config = ConfigDict(from_attributes=True)
