from typing import NamedTuple

from enterprise_rating.ast_decoder.defs import InsType


class ParseResult(NamedTuple):
    variable: str          # e.g. "GI_573"
    next_op_phrase: str    # "plus", "minus", …
    round_var: str         # "NR" or the actual rounding var
    next_op_symbol: str    # raw operator: + - * / …
    round_var_token: str   # raw token: RP, RN, Rnn…
    variable_start: int    # index where the variable began
    next_ptr: int          # position the caller should continue from


# ——————————————————————————————————————————————————————————————————————————
# Configuration: replace these with your actual INS_SUM / INS_PRODUCT string values
# ——————————————————————————————————————————————————————————————————————————
SUM_PRODUCT_INS_TYPES: dict[InsType, str] = {
    # e.g. InsType.SUM: "SUM",
    #       InsType.PRODUCT: "PRODUCT"
}


def find_next_var(equation: str, ptr: int, ins_type: InsType) -> ParseResult:
    if equation is None:
        raise ValueError("`equation` must not be None")
    length = len(equation)
    if ptr >= length:
        return _done(ptr)

    # ──── pre-flight ───────────────────────────────────────────
    ptr = _skip_leading_whitespace(ptr, equation, ins_type)
    if ptr >= length:
        return _done(ptr)

    # ──── variable ─────────────────────────────────────────────
    var_start = ptr
    inside_brackets = False

    while ptr < length:
        c = equation[ptr]
        # break on operator if we're not inside {...} or [...]
        if (
            not inside_brackets
            and _is_operator(c, ins_type)
            and ptr + 1 < length
            and not _is_operator(equation[ptr + 1], ins_type)
        ):
            # skip the next operator if it's a two-char operator
            break
        # track bracketed names like {FOO} or [BAR]
        if c in ('{', '['):
            # If the next char is a closing bracket, exit bracket mode and advance ptr
            if ptr + 1 < length:
                if equation[ptr + 1] not in ('}', ']'):
                    inside_brackets = True
            else:
                ptr += 1
        elif inside_brackets and c in ('}', ']'):
            inside_brackets = False
        ptr += 1

    variable = equation[var_start:ptr]
    if not variable:
        return _done(ptr)

    # ──── operator ────────────────────────────────────────────
    op_symbol = ""
    op_phrase = ""
    if ptr < length:
        op_symbol = equation[ptr]
        op_phrase = _operator_to_phrase(op_symbol, variable)
        ptr += 1

    # ──── rounding ────────────────────────────────────────────
    round_var = "NR"
    round_var_token = ""
    tail = equation[ptr:]
    # two-char tokens
    if tail.startswith(("RP", "RM")):
        round_var_token = tail[:2]
        ptr += 2
    elif tail.startswith("RN"):
        round_var_token = "RN"
        ptr += 2
    # longer R… tokens (but not RV…)
    elif tail.startswith("R") and not tail.startswith("RV"):
        ptr += 1  # skip 'R'
        start = ptr
        while ptr < length and equation[ptr].isalnum():
            ptr += 1
        round_var = equation[start:ptr]
        round_var_token = "R" + round_var

    return ParseResult(
        variable=variable,
        next_op_phrase=op_phrase,
        round_var=round_var,
        next_op_symbol=op_symbol,
        round_var_token=round_var_token,
        variable_start=var_start,
        next_ptr=ptr
    )


def _done(ptr: int) -> ParseResult:
    return ParseResult("", "", "NR", "", "", ptr, ptr)


def _skip_leading_whitespace(ptr: int, eq: str, ins_type: InsType) -> int:
    # spaces are significant for ins_type == "5"
    if ins_type == "5":
        return ptr
    length = len(eq)
    while ptr < length and eq[ptr].isspace():
        ptr += 1
    return ptr


def _is_operator(c: str, ins_type: InsType) -> bool:
    if c in {"+", "-", "*", "/", "!", "|", "@", "^", "#"}:
        return True
    # allow '!' as operator for certain ins_types
    return (ins_type in SUM_PRODUCT_INS_TYPES) and (c == "!")


def _operator_to_phrase(op: str, variable: str) -> str:
    return {
        "+": "plus",
        "-": "" if variable == "GI_" else "minus",
        "*": "multiplied by",
        "/": "divided by",
        "@": "bitwise AND",
        "^": "bitwise OR",
        "=": "equals"
    }.get(op, "")
