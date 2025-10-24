from __future__ import annotations

from typing import Annotated, Literal, get_args

from pydantic import BaseModel, Field

from kiro_insbridge.enterprise_rating.entities.instruction import Instruction


class DependencyBase(BaseModel):

    alg_type: str | None = None  # Type of the algorithm
    category_id: str  # Category ID associated with the algorithm
    description: str  # Description of the algorithm
    index: int  # Index of the algorithm in the sequence
    calc_index: int | None = None  # Optional index for the algorithm
    universal: str = "0"  # Universal flag for the algorithm
    data_type: str | None = None  # Data type of the algorithm
    ib_type: str | None = None  # IB type of the algorithm
    level_id: str | None = None  # Level ID for the algorithm
    system_var: str | None = None  # System variable flag
    processed: str | None = None  # Processed flag for the algorithm
    dependency_vars: list[Dependency] | None = None
    # dependency_vars: dict[str, Dependency] | None = None
    steps: list[Instruction] | None = None

    class Config:
        extra = "ignore"  # Ignore extra fields not defined in the model
        arbitrary_types_allowed = True

    def is_calculated_variable(self) -> bool:
        """Check if this dependency is a CalculatedVariable based on its ib_type."""
        # Get all valid ib_type values for CalculatedVariable
        valid_types = get_args(CalculatedVariable.model_fields['ib_type'].annotation)
        return getattr(self, "ib_type", None) in valid_types

    def is_result_variable(self) -> bool:
        """Check if this dependency is a ResultVariable based on its ib_type."""
        # Get all valid ib_type values for CalculatedVariable
        valid_types = get_args(ResultVariable.model_fields['ib_type'].annotation)
        return getattr(self, "ib_type", None) in valid_types

    def is_table_variable(self) -> bool:
        """Check if this dependency is a TableVariable based on its ib_type."""
        # Get all valid ib_type values for CalculatedVariable
        valid_types = get_args(TableVariable.model_fields['ib_type'].annotation)
        return getattr(self, "ib_type", None) in valid_types


class CalculatedVariable(DependencyBase):
    ib_type: Literal["10", "3"]
    prog_key: str  # Primary key for the program
    revision_key: str  # Revision key for the algorithm
    program_id: str  # Program ID associated with the algorithm
    version: str  # Version of the algorithm
    date_last_modified: str  # Date when the algorithm was last modified


class TableVariable(DependencyBase):
    ib_type: Literal["6", "9"]
    prog_key: str  # Primary key for the program
    revision_key: str  # Revision key for the algorithm
    program_id: str  # Program ID associated with the algorithm
    version: str  # Version of the algorithm
    date_last_modified: str  # Date when the algorithm was last modified


class ResultVariable(DependencyBase):
    ib_type: Literal["8", "16"]


class InputVariable(DependencyBase):
    ib_type: Literal["4"]


Dependency = Annotated[
    CalculatedVariable | TableVariable | ResultVariable | InputVariable,
    Field(discriminator="ib_type")
]
