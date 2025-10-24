from pydantic import BaseModel


class Category(BaseModel):
    """Represents a category."""

    line: str
    index: str
    parent: str
    description: str
