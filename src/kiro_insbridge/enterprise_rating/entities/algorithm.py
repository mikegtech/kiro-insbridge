from pydantic import BaseModel, ConfigDict

from enterprise_rating.entities.dependency import DependencyBase
from enterprise_rating.entities.instruction import Instruction


class Algorithm(BaseModel):
    """Represents a sequence of algorithms in the program version."""

    prog_key: str  # Primary key for the program
    revision_key: str  # Revision key for the algorithm
    alg_type: str  # Type of the algorithm
    category_id: str  # Category ID associated with the algorithm
    description: str  # Description of the algorithm
    date_last_modified: str  # Date when the algorithm was last modified
    index: int  # Index of the algorithm in the sequence
    version: str  # Version of the algorithm
    program_id: str  # Program ID associated with the algorithm
    assign_filter: str | None = None  # Filter for assignment
    advanced_type: str  # Advanced type of the algorithm
    dependency_vars: list[DependencyBase] | None = None
    steps: list[Instruction] | None = None
    model_config = ConfigDict(from_attributes=True)


class AlgorithmSequence(BaseModel):
    """Represents a sequence of algorithms in the program version."""

    # algorithm: Algorithm
    sequence_number: int  # The order of the algorithm in the sequence
    universal: str  # Universal identifier for the algorithm sequence
    algorithm: Algorithm  # The algorithm associated with this sequence
    model_config = ConfigDict(from_attributes=True)
