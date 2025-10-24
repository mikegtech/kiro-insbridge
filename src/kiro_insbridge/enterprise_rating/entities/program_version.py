from pydantic import BaseModel, ConfigDict

from kiro_insbridge.enterprise_rating.entities.algorithm import AlgorithmSequence
from kiro_insbridge.enterprise_rating.entities.category import Category
from kiro_insbridge.enterprise_rating.entities.input_variable import Input


class DataDictionary(BaseModel):
    """Represents a insbridge schema."""

    categories: list[Category]
    inputs: list[Input]


class ProgramVersion(BaseModel):
    """Represents a program_version."""

    # Define the model configuration
    subscriber: str
    line: str
    schema_id: str
    program_id: str
    program_name: str = ""
    version: int
    version_name: str
    primary_key: str
    global_primary_key: str
    effective_date: str
    effective_date_exact: str
    persisted: str
    date_mask: str
    culture: str
    decimal_symbol: str
    group_symbol: str
    data_dictionary: DataDictionary
    algorithm_seq: list[AlgorithmSequence]
    model_config = ConfigDict(from_attributes=True)
