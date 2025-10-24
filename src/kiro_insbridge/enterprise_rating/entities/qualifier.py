

from re import A
from typing import Any

from pydantic import BaseModel, ConfigDict


class Qualifier(BaseModel):
    """Represents one <q …/> element (a single table criteria).
    We keep an `ast` field as a list of plain dicts (not dataclass instances).
    """

    i: int
    v: str | None = None
    c: str
    t: int
    m: str
    # After decoding, we store a list of JSON‐friendly dicts here
    ast: list[Any] | None = None
    audit_context: list[Any] | None = None
    model_config = ConfigDict(from_attributes=True)
