# enterprise_rating/ast_decoder/decoder.py

from enterprise_rating.ast_decoder.helpers.ins_helpers import get_ins_type_def
from enterprise_rating.entities.algorithm import Algorithm
from enterprise_rating.entities.dependency import DependencyBase
from enterprise_rating.entities.program_version import ProgramVersion

from .parser import parse
from .tokenizer import tokenize


def decode_ins(
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    dep_item: DependencyBase | None = None,
    include_english: bool = False
) -> list:
    """Entrypoint: decode one instruction dict into a list of AST nodes.
    If algorithm_or_dependency or program_version is None, parsing will
    produce a best-effort AST without doing any jumps or lookups.

    Args:
      raw_ins        dict of instruction fields (keys: 'n','t','ins','ins_tar','seq_t','seq_f')
      algorithm_or_dependency  an Algorithm object or a Dependency object (or None)
      program_version a ProgramVersion object (or None)

    Returns:
      List[ASTNode]

    """
    # existing = raw_ins.get("ast")
    # if existing is not None and isinstance(existing, list) and len(existing) > 0:
        # If the AST is already present, return it directly
    #    return existing

    ins_str = raw_ins.get("ins", "") or ""

    ins_type = get_ins_type_def(raw_ins.get("t"))

    ins_target = raw_ins.get("ins_tar", "")

    tokens = tokenize(ins_str, ins_type, ins_target)
    return parse(tokens, raw_ins, algorithm_or_dependency, program_version, dep_item)
