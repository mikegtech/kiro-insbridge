# -----------------------------------------------------------------------------
# Instruction types (using actual numeric IDs from DefsInc)
# -----------------------------------------------------------------------------
# noqa: F401
# flake8: noqa: E221
from enum import Enum

VAR_PREFIXES = {
    "LS",  # "Results of Step <ID>"
    "PL",  # "Program Lookup Variables"
    "GL",  # "Global Lookup Variables"
    "GI",  # "Global Input Variables"        ← special: use data_dictionary
    "GR",  # "Global Result Variables"
    "PR",  # "Global Result Variables" (alias to GR)
    "PC",  # "Program Calculated Variables"
    "GC",  # "Global Calculated Variables"
    "PP",  # "Program Policy Variables"
    "GP",  # "Global Calculated Variables (type 1)"
    "IG",  # "Instructions Groups (local)"
    "LX",  # "System Variable"
    "IX",  # "System Variable (alias)"
    "PQ",  # "Local Data Source Variables"
    "GQ",  # "Global Data Source Variables"
    # (Your proc also had LX/IX for SYSTEM_VARS; PQ, GQ for data sources.)
}

class InsType(Enum):  # noqa: D101
    """Instruction types for legacy AST decoder."""

    UNKNOWN = -1  # Invalid Type

    DEF_INS_TYPE_ARITHEMETIC       = 0
    DEF_INS_TYPE_NUMERIC_IF        = 1

    DEF_INS_TYPE_CALL              = 2
    SORT = 3  # Sort
    DEF_INS_TYPE_MASK              = 4
    SET_STRING = 5  # Set String
    EMPTY = 6  # Empty

    # Conditional IFs
    IF_ALL_ALL                     = 50
    IF_NO_ALL                      = 51
    IF_ANY_ALL                     = 52
    IF_ALL_CURRENT_PATH            = 53
    IF_NO_CURRENT_PATH             = 54
    IF_ANY_CURRENT_PATH            = 55
    IF_DATE                        = 56

    # Date operations
    DATE_DIFF_DAYS                 = 57
    DATE_DIFF_MONTHS               = 58
    DATE_DIFF_YEARS                = 59
    INS_DATE_ADDITION              = 126

    # Summation and concatenation
    INS_SUM                        = 60
    INS_SUM_CURRENT_PATH           = 87
    INS_STR_CONCAT                 = 86
    STRING_ADDITION              = 86

    # Simple value operations
    INS_ABS                        = 84
    INS_STRING_LENGTH              = 85
    INS_CNT_CATEGORY_AVAILABLE     = 89
    INS_CNT_CATEGORY_INSTANCE      = 90

    # Category/Ranks
    INS_GET_CATEGORY_ITEM                  = 120
    INS_SET_CATEGORY_ITEM                  = 121
    INS_GET_RANKED_CATEGORY_ITEM           = 122
    INS_SET_RANKED_CATEGORY_ITEM           = 123
    INS_GET_CATEGORY_ITEM_AVAILABLE        = 124
    INS_SET_CATEGORY_ITEM_AVAILABLE        = 125
    INS_RANK_CATEGORY_AVAILABLE            = 93
    INS_RANK_CATEGORY_INSTANCE             = 94

    # Usage-based ranks and flags
    INS_FLAG_ALL_BY_USAGE_SET        = 113
    INS_RANK_ALL_BY_USAGE_SET_COND_ASC  = 118
    INS_RANK_ALL_BY_USAGE_SET_COND_DES  = 119

    # Math functions
    INS_MATH_FUNC_EXP               = 127
    INS_MATH_FUNC_LOG               = 128
    INS_MATH_FUNC_LOG10             = 129
    INS_MATH_FUNC_EXPE              = 130
    INS_MATH_FUNC_RAND              = 131
    INS_MATH_FUNC_FACT              = 132
    INS_MATH_FUNC_SQRT              = 133
    INS_MATH_FUNC_CEIL              = 134
    INS_MATH_FUNC_FLOOR             = 135
    INS_MATH_FUNC_EVEN              = 136
    INS_MATH_FUNC_ODD               = 137

    # Trigonometric functions
    INS_TRIG_FUNC_COS               = 138
    INS_TRIG_FUNC_COSH              = 139
    INS_TRIG_FUNC_ACOS              = 140
    INS_TRIG_FUNC_ACOSH             = 141
    INS_TRIG_FUNC_SIN               = 142
    INS_TRIG_FUNC_SINH              = 143
    INS_TRIG_FUNC_ASIN              = 144
    INS_TRIG_FUNC_ASINH             = 145
    INS_TRIG_FUNC_TAN               = 146
    INS_TRIG_FUNC_TANH              = 147
    INS_TRIG_FUNC_ATAN              = 148
    INS_TRIG_FUNC_ATANH             = 149
    INS_TRIG_FUNC_DEG               = 150
    INS_TRIG_FUNC_RAD               = 151

    # Type checks
    INS_IS_ALPHA           = 99
    INS_IS_DATE            = 95
    INS_IS_NUMERIC         = 98

    # Special operations
    INS_ASSOCIATE_HRV_VALUE_TO_HRD_VALUE  = 110
    INS_QUERY_DATA_SOURCE                = 200

    DEF_INS_TYPE_SET_UNDERWRITING_TO_FAIL = 254

class JumpIndexInstruction(Enum):
    JUMP_ALWAYS = -2
    JUMP_ON_TRUE = -1
    JUMP_ON_FALSE = 0


MULTI_IF_SYMBOL = "#"


def split_var_token(token: str) -> tuple[str, int, int | None]:
    """Given something like "PC_456.2" or "~GI_123" or "DGR_4740", return:
      (prefix, var_id, sub_id)
    - Strips any leading "~" or "D" first.
    - Splits on "_" to get prefix (first two chars) and the rest.
    - If there's a dot, everything after it is sub_id.
    """
    # 1) Remove leading "~" or "D" if present
    if token.startswith("~") or token.startswith("D"):
        token = token[1:]

    # 2) Ensure it has form "XX_<something>"
    if "_" not in token or len(token) < 3:
        raise ValueError(f"Cannot parse variable token '{token}'")

    prefix = token[:2]
    rest = token[3:]  # skip "XX_"
    # If there's a dot, split out sub_id
    if "." in rest:
        main_id_str, sub_id_str = rest.split(".", 1)
        if not main_id_str.isdigit() or not sub_id_str.isdigit():
            raise ValueError(f"Bad numeric IDs in '{token}'")
        return prefix, int(main_id_str), int(sub_id_str)
    else:
        if not rest.isdigit():
            raise ValueError(f"Bad numeric ID in '{token}'")
        return prefix, int(rest), None

