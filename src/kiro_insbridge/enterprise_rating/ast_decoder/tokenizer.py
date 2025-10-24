# noqa: D100, E501
# flake8: noqa: F401

import re
from collections.abc import Callable
from typing import NamedTuple

from kiro_insbridge.enterprise_rating.ast_decoder.helpers.parse_result import find_next_var
from kiro_insbridge.enterprise_rating.ast_decoder.helpers.var_lookup import get_var_desc

from .defs import InsType


class Token(NamedTuple):
    type: str
    value: str
    description: str | None = None

def tokenize_default(raw: str) -> list[str]:
    return [raw] if raw else []


def tokenize_pipe(raw: str) -> list[str]:
    return raw.split('|') if raw else []


def tokenize_plus(raw: str) -> list[str]:
    return raw.split('+') if raw else []


def tokenize_pipe_first(raw: str) -> list[str]:
    if not raw:
        return []
    idx = raw.find('|')
    return [raw] if idx < 0 else [raw[:idx], raw[idx+1:]]


def tokenize_tilde_pipe(raw: str) -> list[str]:
    if not raw:
        return []
    core = raw.split('~', 1)[1] if '~' in raw else raw
    return core.split('|')


def tokenize_rank_usage_set(raw: str) -> list[str]:
    if not raw:
        return []
    if '~' in raw:
        core = raw.split('~', 1)[1]
    else:
        idx = raw.find('|')
        core = raw[idx+1:] if idx >= 0 else ''
    return core.split('|')

def tokenize_multi_if(raw: str) -> list[str]:
    if not raw:
        return []
    m = re.search(r'[\^+]', raw)
    base_raw = raw[:m.start()] if m else raw
    tail = raw[m.start():] if m else ''
    base = base_raw.split('~', 1)[1] if '~' in base_raw else base_raw
    segments = [base]
    i = 0
    while i < len(tail):
        op = tail[i]
        if op not in ('^', '+'):
            i += 1
            continue
        j = i + 1
        while j < len(tail) and tail[j] not in ('^', '+'):
            j += 1
        raw_seg = tail[i+1:j]
        seg = raw_seg.split('~', 1)[1] if '~' in raw_seg else raw_seg
        segments.append(f"{op}{seg}")
        i = j
    return segments

def tokenize_all(raw: str) -> list[Token]:
    """Break raw instruction string into tokens: operators, vars, literals.
    Operators: | ^ + = > < ! ~ { } [ ]
    Variables and numbers are WORD tokens.
    """
    if not raw:
        return []
    # Regex to capture delimiters and words
    pattern = r"(\||\^|\+|>=|<=|=|>|<|!R2|!RN|!RS|!|~|\{|\}|\[|\])"
    parts = [p for p in re.split(pattern, raw) if p and not p.isspace()]
    tokens = []
    for part in parts:
        if part in {'|','^','+','>=','<=','=','>','<','!R2','!RN','!RS','!','~','{','}','[',']'}:
            tokens.append(Token(type='OP', value=get_var_desc(part)))
        else:
            tokens.append(Token(type='WORD', value=part))
    return tokens

def tokenize_scan(raw: str, ins_type: InsType, ins_target: str | None) -> list[Token]:
    tokens = []

    if ins_target is not None:
        tokens.append(Token(type='TARGET', value=ins_target))
        tokens.append(Token(type='OP', value="=", description="[equals]"))

    ptr = 0
    while ptr < len(raw):
        pr = find_next_var(raw, ptr, ins_type)
        if pr is None:
            break

        ptr = pr.next_ptr

        tokens.append(Token(type='VAR', value=pr.variable))

        if len(pr.round_var_token) > 0 or pr.round_var != "NR":
            round_token = pr.round_var_token
            match round_token:
                case _ if round_token.startswith("RP"):
                    round_desc = f"Round Up {round_token[2:]}"
                case _ if round_token.startswith("RM"):
                    round_desc = f"Truncate {round_token[2:]}"
                case "RN":
                    round_desc = "No Round"
                case "RS":
                    round_desc = ""
                    round_token = ""
                case _:
                    round_desc = f"Round {pr.round_var}"

            tokens.append(Token(type='ROUND', value=round_token, description=round_desc))

        if pr.next_op_phrase is not None and pr.next_op_symbol != '!':
            tokens.append(Token(type='OP', value=pr.next_op_symbol, description=pr.next_op_phrase))

    return tokens
# -----------------------------------------------------------------------------
# Dispatch map with descriptions
# -----------------------------------------------------------------------------

dispatch_map: dict[InsType, tuple[Callable, str]] = {
    InsType.DEF_INS_TYPE_CALL:                (tokenize_scan, "PIPE_DELIMITED"),
    InsType.DEF_INS_TYPE_MASK:                (tokenize_pipe_first, "FIRST_PIPE_DELIMITED"),

    InsType.DEF_INS_TYPE_NUMERIC_IF:          (tokenize_multi_if, "IF"),
    InsType.IF_ALL_ALL:                       (tokenize_multi_if, "IF"),
    InsType.IF_NO_ALL:                        (tokenize_multi_if, "IF"),
    InsType.IF_ANY_ALL:                       (tokenize_multi_if, "IF"),
    InsType.IF_DATE:                          (tokenize_multi_if, "IF"),
    InsType.IF_ALL_CURRENT_PATH:              (tokenize_multi_if, "IF"),
    InsType.IF_NO_CURRENT_PATH:               (tokenize_multi_if, "IF"),
    InsType.IF_ANY_CURRENT_PATH:              (tokenize_multi_if, "IF"),

    InsType.INS_IS_ALPHA:                     (tokenize_tilde_pipe, "TILDE_PIPE_SPLIT"),
    InsType.INS_IS_DATE:                      (tokenize_tilde_pipe, "TILDE_PIPE_SPLIT"),
    InsType.INS_IS_NUMERIC:                   (tokenize_tilde_pipe, "TILDE_PIPE_SPLIT"),

    InsType.INS_SUM:                          (tokenize_plus, "PLUS_DELIMITED"),
    InsType.INS_SUM_CURRENT_PATH:             (tokenize_plus, "PLUS_DELIMITED"),

    InsType.INS_STR_CONCAT:                   (tokenize_scan, "DEFAULT"),
    InsType.SET_STRING:                       (tokenize_scan, "DEFAULT"),

    InsType.DATE_DIFF_DAYS:                   (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.DATE_DIFF_MONTHS:                 (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.DATE_DIFF_YEARS:                  (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_DATE_ADDITION:                (tokenize_pipe, "PIPE_DELIMITED"),

    InsType.INS_GET_CATEGORY_ITEM:            (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_SET_CATEGORY_ITEM:            (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_GET_RANKED_CATEGORY_ITEM:     (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_SET_RANKED_CATEGORY_ITEM:     (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_GET_CATEGORY_ITEM_AVAILABLE:  (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_SET_CATEGORY_ITEM_AVAILABLE:  (tokenize_pipe, "PIPE_DELIMITED"),

    InsType.INS_CNT_CATEGORY_AVAILABLE:       (tokenize_default, "DEFAULT"),
    InsType.INS_CNT_CATEGORY_INSTANCE:        (tokenize_default, "DEFAULT"),
    InsType.INS_RANK_CATEGORY_AVAILABLE:      (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_RANK_CATEGORY_INSTANCE:       (tokenize_pipe, "PIPE_DELIMITED"),

    InsType.INS_FLAG_ALL_BY_USAGE_SET:        (tokenize_rank_usage_set, "RANK_USAGE_PIPE"),
    InsType.INS_RANK_ALL_BY_USAGE_SET_COND_ASC:(tokenize_rank_usage_set, "RANK_USAGE_PIPE"),
    InsType.INS_RANK_ALL_BY_USAGE_SET_COND_DES:(tokenize_rank_usage_set, "RANK_USAGE_PIPE"),

    InsType.INS_MATH_FUNC_EXP:                (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_LOG:                (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_LOG10:              (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_EXPE:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_RAND:               (tokenize_default, "DEFAULT"),
    InsType.INS_MATH_FUNC_FACT:               (tokenize_default, "DEFAULT"),
    InsType.INS_MATH_FUNC_SQRT:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_CEIL:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_FLOOR:              (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_MATH_FUNC_EVEN:               (tokenize_default, "DEFAULT"),
    InsType.INS_MATH_FUNC_ODD:                (tokenize_default, "DEFAULT"),

    InsType.INS_TRIG_FUNC_COS:                (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_COSH:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_ACOS:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_ACOSH:              (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_SIN:                (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_SINH:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_ASIN:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_ASINH:              (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_TAN:                (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_TANH:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_ATAN:               (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_ATANH:              (tokenize_pipe, "PIPE_DELIMITERED"),
    InsType.INS_TRIG_FUNC_DEG:                (tokenize_pipe, "PIPE_DELIMITED"),
    InsType.INS_TRIG_FUNC_RAD:                (tokenize_pipe, "PIPE_DELIMITED"),

    InsType.INS_ASSOCIATE_HRV_VALUE_TO_HRD_VALUE:(tokenize_default, "DEFAULT"),
    InsType.INS_QUERY_DATA_SOURCE:            (tokenize_pipe, "PIPE_DELIMITED"),
}

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def tokenize(raw_ins: str, ins_type: InsType, ins_target: str | None) -> list[Token]:
    """Tokenize the raw instruction string based on its type.

    Returns a list of string segments according to the dispatch_map rules.
    """
    func_tuple = dispatch_map.get(ins_type, (tokenize_default, "DEFAULT")) if ins_type is not None else None

    if func_tuple is not None:
        func, _ = func_tuple
        if func is tokenize_multi_if:
            return tokenize_all(raw_ins)
        if func is tokenize_scan:
            return tokenize_scan(raw_ins, ins_type, ins_target)
        return func(raw_ins)

    return []

__all__ = ['InsType', 'tokenize', 'dispatch_map']
