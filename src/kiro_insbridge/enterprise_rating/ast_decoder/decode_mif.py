# enterprise_rating/ast_decoder/decode_mif.py

from typing import cast

from enterprise_rating.ast_decoder.helpers.ins_helpers import get_ins_type_def
from enterprise_rating.ast_decoder.renderer import render_node
from enterprise_rating.entities.algorithm import Algorithm
from enterprise_rating.entities.dependency import DependencyBase
from enterprise_rating.entities.program_version import ProgramVersion

from .ast_nodes import (ASTNode, CompareNode, IfNode, JumpNode,
                        MultiConditionNode, RawNode)
from .defs_legacy import MULTI_IF_SYMBOL
from .tokenizer import tokenize


def decode_mif(
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "MULTI_IF"
) -> list[ASTNode]:
    """Build exactly one IfNode whose condition is a MultiConditionNode
    containing all sub-clauses joined by OR (^) or AND (+).
    """
    ins_str = raw_ins.get("ins", "") or ""
    step = int(raw_ins.get("n", 0))
    ins_type = int(raw_ins.get("t", 0))

    # 1) Split off base (before '#') and multi (after '#')
    if MULTI_IF_SYMBOL in ins_str:
        idx = ins_str.index(MULTI_IF_SYMBOL)
        base_part  = ins_str[:idx]
        multi_body = ins_str[idx+1:]
    else:
        base_part  = ""
        multi_body = ins_str

    # 2) Decide joiner: OR if '^' present else AND if '+' present (default OR)
    if '^' in multi_body:
        split_char, joiner = '^', "OR"
    else:
        split_char, joiner = '+', "AND"

    # 3) Collect fragments: base_part + each piece of multi_body
    fragments = []
    if base_part.strip():
        fragments.append(base_part.strip())
    fragments += [frag for frag in multi_body.split(split_char) if frag.strip()]

    # 4) Parse each fragment into a CompareNode via parse_if
    compare_nodes: list[CompareNode] = []
    from .parser import parse_if  # avoid circular
    for frag in fragments:
        # tokenize & build a mini‐raw for parse_if
        raw = raw_ins.copy()
        raw["ins"] = frag.strip()
        tokens = tokenize(raw["ins"], get_ins_type_def(raw["t"]), None)
        nodes = parse_if(tokens, raw, algorithm_or_dependency, program_version)
        # parse_if always returns one IfNode with condition=CompareNode
        if_node = cast(IfNode, nodes[0])
        if nodes:
            cond = if_node.condition
            if isinstance(cond, CompareNode):
                cond.cond_op = joiner
                compare_nodes.append(cond)

    # 5) Build the MultiConditionNode
    multi_cond = MultiConditionNode(
        step=step,
        ins_type=ins_type,
        template_id= template_id,
        step_type=ins_type,
        conditions=compare_nodes,
        joiner=joiner,
    )

    # 6) Build the top‐level IfNode with jump branches
    true_t, false_t = raw_ins.get("seq_t"), raw_ins.get("seq_f")
    true_branch = []
    false_branch = []
    if true_t is not None and int(true_t) > 0:
        true_branch  = [JumpNode(step=step, ins_type=ins_type,
                                 step_type=ins_type,
                                 template_id="JUMP",
                                 target=int(true_t))]
    if false_t is not None and int(false_t) > 0:
        false_branch = [JumpNode(step=step, ins_type=ins_type,
                                 step_type=ins_type,
                                 template_id="JUMP",
                                 target=int(false_t))]

    if_node = IfNode(
        step=step,
        ins_type=ins_type,
        step_type=ins_type,
        template_id = template_id,
        condition=multi_cond,
        true_branch=true_branch,
        false_branch=false_branch,
    )

    if_node.english = render_node(if_node)

    return [if_node]


def decode_mif_old(
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = ""
) -> list[ASTNode]:
    """Decode any instruction whose 'ins' string contains '#' (multi-IF marker),
    or '^' (OR), or '+' (AND).  Each sub-clause is still in the form "|VAR|OP|VALUE|",
    so we do NOT strip away the '|'—instead, parse_if will split on pipes.

    If decode_ins(...) raises, return a single RawNode containing the exception text.
    """
    from .decoder import decode_ins

    combined_nodes: list[ASTNode] = []
    ins_str = raw_ins.get("ins", "") or ""

    # 1) If '#' present, split into base_part (before '#') and multi_body (after '#').
    if MULTI_IF_SYMBOL in ins_str:
        idx_hash = ins_str.index(MULTI_IF_SYMBOL)
        base_part = ins_str[:idx_hash]
        multi_body = ins_str[idx_hash + 1:]
    else:
        base_part = ""
        multi_body = ins_str

    # 2) If there's a nonempty base_part, parse it first as a standalone IF node
    trimmed_base = base_part.strip()
    if trimmed_base:
        sub_raw = raw_ins.copy()
        sub_raw["ins"] = trimmed_base
        try:
            combined_nodes.extend(decode_ins(sub_raw, algorithm_or_dependency, program_version))
        except Exception as e:
            step = int(raw_ins.get("n", 0))
            tval = raw_ins.get("t")
            try:
                ins_type_val = int(tval)
            except:
                ins_type_val = None
            combined_nodes.append(
                RawNode(step=step, ins_type=ins_type_val, template_id=template_id, raw="", value=f"ERROR: {e}")
            )

    # 3) Now split multi_body on '^' or '+' (in the order they appear).  We do NOT remove the pipes.
    fragments: list[str] = []
    i = 0
    length = len(multi_body)

    while i < length:
        idx_caret = multi_body.find("^", i)
        idx_plus = multi_body.find("+", i)

        # Helper to pick the earliest non-(-1) index
        def earliest(a: int, b: int) -> int:
            if a == -1: return b
            if b == -1: return a
            return a if a < b else b

        split_idx = earliest(idx_caret, idx_plus)

        if split_idx == -1:
            # No more '^' or '+'.  The rest is one final fragment.
            fragments.append(multi_body[i:])
            break
        else:
            fragments.append(multi_body[i:split_idx])
            i = split_idx + 1

    # 4) For each fragment (still in the form "|VAR|OP|VALUE|"), call decode_ins(...)
    for fragment in fragments:
        sub_raw = raw_ins.copy()
        sub_raw["ins"] = fragment.strip()
        try:
            combined_nodes.extend(decode_ins(sub_raw, algorithm_or_dependency, program_version))
        except Exception as e:
            step = int(raw_ins.get("n", 0))
            tval = raw_ins.get("t")
            try:
                ins_type_val = int(tval)
            except:
                ins_type_val = None
            combined_nodes.append(
                RawNode(step=step, ins_type=ins_type_val, raw="", value=f"ERROR: {e}")
            )

    return combined_nodes
