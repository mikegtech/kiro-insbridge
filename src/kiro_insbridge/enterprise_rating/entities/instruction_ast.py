# enterprise_rating/entities/instruction_ast.py


from pydantic import BaseModel

from enterprise_rating.ast_decoder.ast_nodes import ASTNode


class InstructionAst(BaseModel):
    """A wrapper around the list of ASTNode objects produced by decoding one instruction.
    We also include a success flag and optional error message so that downstream services
    know whether parsing succeeded or failed.
    """

    nodes: list[ASTNode]
    decoded_ok: bool = True
    error_message: str | None = None
