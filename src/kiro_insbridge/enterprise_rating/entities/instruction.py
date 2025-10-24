

from typing import Any

from pydantic import BaseModel, ConfigDict


class Instruction(BaseModel):
    """Represents one <i …/> element (a single algorithm step).
    We keep an `ast` field as a list of plain dicts (not dataclass instances).
    """

    n: int
    t: int
    ins: str
    ins_tar: str | None = None
    seq_t: int | None = None
    seq_f: int | None = None

    # After decoding, we store a list of JSON‐friendly dicts here
    ast: list[Any] | None = None
    model_config = ConfigDict(from_attributes=True)
