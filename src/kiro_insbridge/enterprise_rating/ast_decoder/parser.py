# noqa: F401, F841, ARG001, E241
# flake8: noqa: E501
# pylint: disable=unused-import, unused-variable, unused-argument, missing-module-docstring
from collections.abc import Callable

from kiro_insbridge.enterprise_rating.ast_decoder.defs import MULTI_IF_SYMBOL
from kiro_insbridge.enterprise_rating.ast_decoder.renderer import render_node
from kiro_insbridge.enterprise_rating.entities.algorithm import Algorithm
from kiro_insbridge.enterprise_rating.entities.dependency import DependencyBase
from kiro_insbridge.enterprise_rating.entities.program_version import ProgramVersion

from .ast_nodes import (ArithmeticNode, AssignmentNode, ASTNode, CompareNode,
                        FunctionNode, IfNode, JumpNode, RawNode, TypeCheckNode)
from .decode_mif import decode_mif
from .defs import InsType
from .helpers.ins_helpers import get_ins_type_def
from .helpers.var_lookup import get_target_var_desc, get_var_desc
from .tokenizer import Token


# ──────────────────────────────────────────────────────────────────────────────
def parse(
    tokens: list[Token],
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    dep_item: DependencyBase | None = None,
) -> list[ASTNode]:
    """Main parser dispatcher: inspects InsType and directs to the appropriate subparser.
    Supports algorithm_or_dependency=None or program_version=None by skipping any lookups/jumps.

    Args:
      tokens: list of Token objects (from tokenize(raw_ins['ins'])).
      raw_ins: dict of instruction fields ('n','t','ins','ins_tar','seq_t','seq_f', etc.).
      algorithm_or_dependency: an Algorithm object or a Dependency object (or None).
      program_version: a ProgramVersion object (or None).

    Returns:
      List[ASTNode]: The parsed AST nodes for the instruction.

    """
    ins_type = get_ins_type_def(raw_ins.get("t"))  # type: InsType

    step = int(raw_ins.get("n", 0))
    ins_str = raw_ins.get("ins", "") or ""

    # 3) InsType dispatch map
    # noqa: E241
    dispatch_map: dict[InsType, tuple[Callable, str]] = {
        InsType.DEF_INS_TYPE_ARITHEMETIC: (parse_arithmetic, "ASSIGNMENT"),      # 0
        InsType.DEF_INS_TYPE_NUMERIC_IF: (parse_if, "IF_COMPARE"),               # 1
        InsType.DEF_INS_TYPE_CALL: (parse_call, "FUNCTION_CALL"),                # 2
        InsType.SORT: (parse_sort, "FUNCTION_CALL"),                             # 3
        InsType.DEF_INS_TYPE_MASK: (parse_mask, "MASK"),                         # 4
        InsType.SET_STRING: (parse_set_string, "ASSIGNMENT"),                    # 5
        InsType.EMPTY: (parse_empty, "EMPTY"),                                   # 6
        InsType.INS_STR_CONCAT: (parse_string_addition, "STRING_CONCAT"),        # 86
        InsType.DATE_DIFF_DAYS: (parse_date_diff, "DATE_DIFF"),                  # 57
        InsType.DATE_DIFF_MONTHS: (parse_date_diff, "DATE_DIFF"),                # 58
        InsType.DATE_DIFF_YEARS: (parse_date_diff, "DATE_DIFF"),                 # 59
        InsType.INS_DATE_ADDITION: (parse_date_addition, "DATE_DIFF"),           # 126
        InsType.INS_MATH_FUNC_EXP: (parse_function, "FUNCTION_CALL"),            # 127
        InsType.INS_MATH_FUNC_LOG: (parse_function, "FUNCTION_CALL"),            # 128
        InsType.INS_MATH_FUNC_LOG10: (parse_function, "FUNCTION_CALL"),          # 129
        InsType.INS_MATH_FUNC_EXPE: (parse_function, "FUNCTION_CALL"),           # 130
        InsType.INS_MATH_FUNC_SQRT: (parse_function, "FUNCTION_CALL"),           # 133
        InsType.INS_TRIG_FUNC_COS: (parse_function, "FUNCTION_CALL"),            # 138
        InsType.INS_TRIG_FUNC_SIN: (parse_function, "FUNCTION_CALL"),            # 142
        InsType.INS_TRIG_FUNC_TAN: (parse_function, "FUNCTION_CALL"),            # 146
        InsType.INS_TRIG_FUNC_COSH: (parse_function, "FUNCTION_CALL"),           # 139
        InsType.INS_TRIG_FUNC_SINH: (parse_function, "FUNCTION_CALL"),           # 143
        InsType.INS_TRIG_FUNC_TANH: (parse_function, "FUNCTION_CALL"),           # 147
        InsType.INS_QUERY_DATA_SOURCE: (parse_data_source, "QUERY_DATA_SOURCE"), # 200
        InsType.INS_RANK_CATEGORY_INSTANCE: (parse_rank_flag, "RANK_FLAG"),      # 94
        InsType.INS_RANK_CATEGORY_AVAILABLE: (parse_rank_flag, "RANK_ACROSS_CATEGORY_ALL_AVAILABLE_ALT"), # 93
        InsType.DEF_INS_TYPE_SET_UNDERWRITING_TO_FAIL: (parse_empty, "SET_UNDERWRITING_TO_FAIL"), # 254
        InsType.INS_IS_DATE: (parse_type_check, "IS_DATE"),                      # 95
        InsType.INS_IS_NUMERIC: (parse_type_check, "IS_NUMERIC"),                # 98
        InsType.INS_IS_ALPHA: (parse_type_check, "IS_ALPHA"),                    # 99
        # ...continue for all needed types...
    }

    parser_func_tuple = dispatch_map.get(ins_type) if ins_type is not None else None
    if parser_func_tuple is not None:
        parser_func, template_id = parser_func_tuple

        # 1) If there's a '#' anywhere, jump to decode_mif
        if MULTI_IF_SYMBOL in ins_str or "^" in ins_str or "+" in ins_str:
            return decode_mif(raw_ins, algorithm_or_dependency, program_version, template_id)

        # 2) Otherwise, if this is a plain IF, call parse_if
        if ins_type == InsType.DEF_INS_TYPE_NUMERIC_IF:
            return parse_if(tokens, raw_ins, algorithm_or_dependency, program_version, template_id)

        # Some parsers need raw_ins + algorithm_or_dependency + program_version
        if parser_func in (parse_if, parse_if_date, parse_data_source, parse_type_check):
            return parser_func(tokens, raw_ins, algorithm_or_dependency, program_version, template_id)  # type: ignore

        ins_target = get_target_var_desc(raw_ins.get("ins_tar", ""), dep_item)

        ast_nodes = parser_func(tokens, step, ins_type, ins_target, algorithm_or_dependency, program_version, template_id)  # type: ignore

        if isinstance(ast_nodes, list) and ast_nodes and isinstance(ast_nodes[0], AssignmentNode):
            ast_node = ast_nodes[0]

            ast_node.next_true = [
                JumpNode(step=step, ins_type=ins_type, template_id="JUMP", target=raw_ins.get("seq_t"))
            ]

            ast_node.next_false = [
                JumpNode(step=step, ins_type=ins_type, template_id="JUMP", target=raw_ins.get("seq_f"))
            ]

            ast_node.english = render_node(ast_node)  # Render the English description

        return ast_nodes

    # 4) Fallback: if no InsType matched, produce a RawNode
    desc = get_var_desc(ins_str, None, algorithm_or_dependency, program_version)
    return [RawNode(step=step, ins_type=ins_type, raw=ins_str, value=desc)]


# ──────────────────────────────────────────────────────────────────────────────
def parse_rank_flag(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Generic parser for any InsType whose name begins with 'RANK' or 'FLAG'.
    If algorithm_or_dependency or program_version is None, we skip get_var_desc lookups.
    """
    action_text = ins_type.name.replace("_", " ").title()

    vars_expanded = []
    for t in tokens:
        if (
            program_version is not None
            and algorithm_or_dependency is not None
            and t.type == "WORD"
            and ("GI_" in t.value or "GC_" in t.value)
        ):
            vars_expanded.append(get_var_desc(t.value, t.type, algorithm_or_dependency, program_version))
        else:
            vars_expanded.append(t.value)

    if vars_expanded:
        action_text += ": " + ", ".join(vars_expanded)

    desc = get_var_desc(action_text, None, algorithm_or_dependency, program_version)
    return [RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=action_text, value=desc)]


# ──────────────────────────────────────────────────────────────────────────────
def parse_sort(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Stub for Sort instruction."""
    text = "Sort: " + " ".join(t.value for t in tokens)
    return [RawNode(step=step, ins_type=ins_type, template_id=template_id, raw="", value=text)]


# ──────────────────────────────────────────────────────────────────────────────
def parse_mask(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Stub for Mask instruction."""
    text = "Mask: " + " ".join(t.value for t in tokens)

    return [RawNode(step=step, ins_type=ins_type, template_id=template_id, raw="", value=text)]


# ──────────────────────────────────────────────────────────────────────────────
def parse_empty(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Empty instruction (no-op)."""
    return []


# ──────────────────────────────────────────────────────────────────────────────
def parse_set_string(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Set String: usually of the form VAR [literal].
    Produces an AssignmentNode where expr is a RawNode(string literal).
    """
    # 1) Build the concat expression
    target_val = get_var_desc(ins_target, None, algorithm_or_dependency, program_version)
    args = [RawNode(step, ins_type, raw=t.value,
                value=target_val if t.type == "TARGET" else get_var_desc(t.value, t.type, algorithm_or_dependency, program_version),type=t.type)
            for t in tokens]

    concat = FunctionNode(
        step=step, ins_type=ins_type,
        name="SetString",
        args=args,
    )

    assign = AssignmentNode(
        step=step,
        ins_type=ins_type,
        var=ins_target,
        expr=concat,
        target=None,
        template_id=template_id,
        next_true=None,
        next_false=None,
    )

    return [assign]


# ──────────────────────────────────────────────────────────────────────────────
def parse_string_addition(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse String Addition instructions (InsType.STRING_ADDITION)."""
    target_val = get_var_desc(ins_target, None, algorithm_or_dependency, program_version)
    args = [RawNode(step, ins_type, raw=t.value,
                value=target_val if t.type == "TARGET" else get_var_desc(t.value, t.type, algorithm_or_dependency, program_version),type=t.type)
            for t in tokens]

    concat = FunctionNode(
        step=step, ins_type=ins_type,
        name="StringAddition",
        args=args,
    )

    assign = AssignmentNode(
        step=step,
        ins_type=ins_type,
        var=ins_target,         # ins_tar goes here
        expr=concat,            # the FunctionNode becomes the expression
        target=target_val,
        template_id=template_id,
        next_true=None,
        next_false=None,
    )


    return [assign]


# ──────────────────────────────────────────────────────────────────────────────
def parse_date_diff(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse DATE_DIFF_* into a FunctionNode; all verbalization happens in renderer.py."""
    # 1) pull out the raw token values
    if len(tokens) >= 2:
        left_val, right_val = tokens[0].value, tokens[1].value
    else:
        left_val = tokens[0].value if tokens else ""
        right_val = ""

    # 2) look up their “nice” descriptions, but do *not* build any English sentence here
    left_desc = get_var_desc(left_val, None, None)
    right_desc = get_var_desc(right_val, None, None)

    left_node = RawNode(step=step, ins_type=ins_type, raw=left_val, value=left_desc)
    right_node = RawNode(step=step, ins_type=ins_type, raw=right_val, value=right_desc)

    # 3) Emit only the structural AST
    func_node = FunctionNode(
        step=step,
        ins_type=ins_type,
        name="DateDifference",  # or ins_type.name
        template_id=template_id,
        args=[left_node, right_node],
    )
    func_node.english = render_node(func_node)  # Render the English description
    return [func_node]


# ──────────────────────────────────────────────────────────────────────────────
def parse_date_addition(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse Date Addition instructions: [base_date_or_var, offset_spec].
    Builds a FunctionNode named "Date Addition" with English = "Add offset to date".
    """
    if len(tokens) >= 2:
        date_val = tokens[0].value
        offset_val = tokens[1].value
    else:
        date_val = tokens[0].value if tokens else ""
        offset_val = ""

    date_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=date_val, value=date_val)
    offset_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=offset_val, value=offset_val)

    func_node = FunctionNode(
        step=step,
        ins_type=ins_type,
        name="DateAddition",
        template_id=template_id,
        args=[date_node, offset_node],
    )
    func_node.english = render_node(func_node)  # Render the English description
    return [func_node]


# ──────────────────────────────────────────────────────────────────────────────
def parse_if_date(
    tokens: list[Token],
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse IF_DATE instructions (InsType.IF_DATE).
    Builds a CompareNode inside an IfNode, then wires true/false branches via seq_t/seq_f
    if algorithm_or_dependency is provided.
    """
    step = int(raw_ins.get("n", 0))
    ins_type = InsType(int(raw_ins.get("t", 0)))

    left_val = tokens[0].value if len(tokens) > 0 else ""
    op_val = tokens[1].value if len(tokens) > 1 else ""
    right_val = tokens[2].value if len(tokens) > 2 else ""

    left_desc = get_var_desc(left_val, None, algorithm_or_dependency, program_version)
    left_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=left_val, value=left_desc)

    right_desc = get_var_desc(left_val, None, algorithm_or_dependency, program_version)
    right_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=right_val, value=right_desc)

    condition = CompareNode(
        step=step,
        ins_type=ins_type,
        left=left_node,
        template_id=template_id,
        operator=op_val,
        right=right_node,
    )
    condition.english = render_node(condition)  # Render the English description

    # Next‐step pointers
    seq_t = raw_ins.get("seq_t")
    seq_f = raw_ins.get("seq_f")

    next_true = int(seq_t) if seq_t is not None else None
    next_false = int(seq_f) if seq_f is not None else None

    node = IfNode(
        step=step, ins_type=ins_type, template_id=template_id, condition=condition, true_branch=[], false_branch=[]
    )
    node.english = render_node(node)  # Render the English description

    # True branch
    if next_true is not None:
        node.true_branch = [
            JumpNode(step=step, ins_type=ins_type, template_id="JUMP", target=next_true)
        ]

        for branch_node in node.true_branch:
            branch_node.english = render_node(branch_node)

    # False branch
    if next_false is not None:
        node.false_branch = [
            JumpNode(step=step, ins_type=ins_type, template_id="JUMP", target=next_false)
        ]

        for branch_node in node.false_branch:
            branch_node.english = render_node(branch_node)

    return [node]


# ──────────────────────────────────────────────────────────────────────────────
def parse_if(
    tokens: list[Token],
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse a single‐clause IF of the form "|VAR|OP|VALUE|" (e.g. "|GR_5370|=|{}|").
    We assume callers (decode_mif or parse) never strip the pipes before we run this.
    """
    step = int(raw_ins.get("n", 0))
    ins_type = InsType(int(raw_ins.get("t", 0)))
    ins_str = raw_ins.get("ins", "") or ""  # e.g. "|~GI_494|<>|GC_691|"

    # 1) Split on '|' – we expect ["", VAR, OP, VALUE, ""]
    parts = ins_str.split("|")
    if len(parts) >= 4:
        left_val = parts[1]
        operator = get_var_desc(parts[2])
        right_val = parts[3]
    elif len(parts) == 3:
        left_val = parts[0]
        operator = get_var_desc(parts[1])
        right_val = parts[2]
    else:
        # If unexpected format, capture entire string as left_val
        left_val = ins_str
        operator = ""
        right_val = ""

    desc_left = get_var_desc(left_val, None, algorithm_or_dependency, program_version)
    left_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=left_val, value=desc_left)
    desc_right = get_var_desc(right_val, None, algorithm_or_dependency, program_version)
    right_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=right_val, value=desc_right)

    condition = CompareNode(
        step=step, ins_type=ins_type, left=left_node, operator=operator, right=right_node, english=""
    )

    node = IfNode(
        step=step, ins_type=ins_type, template_id=template_id, condition=condition, true_branch=[], false_branch=[]
    )

    # 2) Wire up true/false branches via seq_t / seq_f if available
    seq_t = raw_ins.get("seq_t")
    seq_f = raw_ins.get("seq_f")

    next_true = int(seq_t) if seq_t is not None else None
    next_false = int(seq_f) if seq_f is not None else None

    # True branch
    if next_true is not None:
        node.true_branch = [
            JumpNode(step=step, ins_type=ins_type, template_id="JUMP", target=next_true)
        ]

    # False branch
    if next_false is not None:
        node.false_branch = [
            JumpNode(step=step, ins_type=ins_type, template_id="JUMP", target=next_false)
        ]

    if template_id is not None and template_id != "":
        node.english = render_node(node)

    return [node]


# ──────────────────────────────────────────────────────────────────────────────
def parse_arithmetic(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse arithmetic expressions into an ArithmeticNode.
    Expected format: VAR operator VAR [round_spec], where round_spec might be '!RN', etc.
    """
    round_spec = None
    if tokens and tokens[-1].value.startswith("!"):
        round_token = tokens[-1].value
        round_spec = round_token[1:]
        tokens = tokens[:-1]

    if len(tokens) >= 3:
        left_val = tokens[0].value
        operator = tokens[1].value
        right_val = tokens[2].value

        left_desc = get_var_desc(left_val, None, algorithm_or_dependency, program_version)
        left_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=left_val, value=left_desc)

        right_desc = get_var_desc(right_val, None, algorithm_or_dependency, program_version)
        right_node = RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=right_val, value=right_desc)

        node = ArithmeticNode(
            step=step,
            ins_type=ins_type,
            left=left_node,
            operator=operator,
            template_id=template_id,
            right=right_node,
            round_spec=round_spec
        )
        node.english = render_node(node)

        return [node]

    # Fallback
    return [
        RawNode(step=step, ins_type=ins_type, template_id=template_id, raw="", value=" ".join(t.value for t in tokens))
    ]


# ──────────────────────────────────────────────────────────────────────────────
def parse_function(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Generic parser for math & trigonometry functions.
    Handles single‐arg (SQRT, LOG, etc.) and two‐arg (POWER, etc.) + optional rounding.
    """
    round_spec = None
    if tokens and tokens[-1].value.startswith("!"):
        round_token = tokens[-1].value
        round_spec = round_token[1:]
        tokens = tokens[:-1]

    args = [RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=t.value, value=t.value) for t in tokens]

    friendly_map = {
        "POWER": "Power",
        "LOG": "Natural Log",
        "LOG10": "Log Base 10",
        "EXP": "Exponential",
        "SQRT": "Square Root",
        "COS": "Cosine",
        "SIN": "Sine",
        "TAN": "Tangent",
        "COSH": "Hyperbolic Cosine",
        "SINH": "Hyperbolic Sine",
        "TANH": "Hyperbolic Tangent",
    }
    display_name = friendly_map.get(ins_type.name, ins_type.name.title())

    node = FunctionNode(
        step=step, ins_type=ins_type, template_id=template_id, name=display_name, args=args, round_spec=round_spec
    )
    node.english = render_node(node)  # Render the English description

    return [node]


# ──────────────────────────────────────────────────────────────────────────────
def parse_call(
    tokens: list[Token],
    step: int,
    ins_type: InsType,
    ins_target: str,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse a function call."""
    node = FunctionNode(
        step=step,
        ins_type=ins_type,
        name="CallOut",
        args=[RawNode(step, ins_type, raw=t.value,
            value=getattr(t, "description", ""), type=t.type)
        for t in tokens
        ],
    )

    node.english = render_node(node)  # Render the English description
    return [node]


# ──────────────────────────────────────────────────────────────────────────────
def parse_data_source(
    tokens: list[Token],
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse DataSource instructions (InsType.DATA_SOURCE).
    If program_version is None, we skip lookups and return a placeholder.
    """
    step = int(raw_ins.get("n", 0))
    ins_type = InsType(int(raw_ins.get("t", 0)))

    node = FunctionNode(
        step=step,
        ins_type=ins_type,
        name="DataSource",
        args=[
            RawNode(step=step, ins_type=ins_type, template_id=template_id, raw=tok.value, value=tok.value)
            for tok in tokens
        ],
    )

    node.english = render_node(node)  # Render the English description
    return [node]


def parse_type_check(
    tokens: list[Token],
    raw_ins: dict,
    algorithm_or_dependency: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
    template_id: str = "",
) -> list[ASTNode]:
    """Parse IS_DATE (95), IS_NUMERIC (98), or IS_ALPHA (99):
    Build an IfNode whose condition is a TypeCheckNode, and branches are JumpNodes.
    """
    step = int(raw_ins.get("n", 0))
    ins_type = InsType(int(raw_ins.get("t", 0)))

    # 1) extract the single variable token
    if tokens and tokens[0].value.startswith("~") and len(tokens) > 1:
        left_raw = tokens[1].value
    else:
        left_raw = tokens[0].value if tokens else ""

    left_desc = get_var_desc(left_raw, None, algorithm_or_dependency, program_version)
    left_node = RawNode(
        step=step,
        ins_type=ins_type,
        template_id=template_id,
        step_type=ins_type,
        raw=left_raw,
        value=left_desc,
    )

    # 2) map ins_type to a human‐readable check_type
    check_map = {
        InsType.IS_DATE:    "date",
        InsType.IS_NUMERIC: "numeric",
        InsType.IS_ALPHA:   "alpha",
    }
    check_type = check_map.get(ins_type, ins_type.name.lower())

    # 3) build the unary condition node
    cond_node = TypeCheckNode(
        step=step,
        ins_type=ins_type,
        template_id=template_id,
        step_type=ins_type,
        left=left_node,
        check_type=check_type,
    )

    # 4) wire up true/false branches via seq_t / seq_f
    seq_t = raw_ins.get("seq_t")
    seq_f = raw_ins.get("seq_f")
    true_branch = []
    false_branch = []
    if seq_t is not None and int(seq_t) > 0:
        true_branch = [
            JumpNode(step=step, ins_type=ins_type, template_id="JUMP", step_type=ins_type,
                     target=int(seq_t))
        ]
    if seq_f is not None and int(seq_f) > 0:
        false_branch = [
            JumpNode(step=step, ins_type=ins_type, template_id="JUMP", step_type=ins_type,
                     target=int(seq_f))
        ]

    # 5) combine into a single IfNode
    node = IfNode(
        step=step,
        ins_type=ins_type,
        template_id="TYPE_CHECK",
        step_type=ins_type,
        condition=cond_node,
        true_branch=true_branch,
        false_branch=false_branch,
    )

    return [node]
