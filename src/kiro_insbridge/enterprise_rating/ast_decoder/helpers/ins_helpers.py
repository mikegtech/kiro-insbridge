from kiro_insbridge.enterprise_rating.ast_decoder.defs import InsType


def decode_filter_rule(filter_rule: str, dependency_var_writer, dependency_list) -> None:
    """Stub for DecodeFilterRule: splits filter rules by '-' and writes
    dependency variables to writer.
    """
    if not filter_rule:
        return
    parts = filter_rule.split('-')
    if len(parts) != 4:
        return
    prefix = 'GC_' if parts[1] == '0' else 'PC_'
    var_key = f"{prefix}{parts[2]}"
    # TODO: write var_key to dependency_var_writer and update dependency_list
    pass


def get_operator_english(oper: str) -> str:
    """Stub for GetOperatorEnglish: maps symbol to English phrase.
    """
    mapping = {
        '=': 'equals',
        '>': 'greater than',
        '<': 'less than',
        '<=': 'less than or equal to',
        '>=': 'greater than or equal to',
        '!=': 'not equal to',
        '<>': 'not equal to',
        '@': 'bitwise AND',
        '^': 'bitwise OR'
    }
    return mapping.get(oper, oper)


def get_round_english(round_spec: str) -> str:
    """Stub for GetRoundEnglish: describes rounding spec in English.
    """
    if not round_spec:
        return ''
    # Round up (RP)
    if round_spec.startswith("RP"):
        places = round_spec[2:] if len(round_spec) >= 3 else "0"
        return f"Round Up {places} place(s)"
    # Truncate (RM)
    if round_spec.startswith("RM"):
        places = round_spec[2:] if len(round_spec) >= 3 else "0"
        return f"Truncate {places} place(s)"
    # No Round (RN)
    if round_spec.startswith("RN"):
        return "No Round"
    # Skip NR and RS prefixes
    if round_spec.startswith("NR") or round_spec.startswith("RS"):
        return round_spec
    # Default rounding
    # e.g. "R2" -> Round to 2 place(s)
    if round_spec.startswith("R"):
        places = round_spec[1:] if len(round_spec) == 2 else round_spec[1:]
        return f"Round to {places} place(s)"
    return round_spec


def get_next_step_english(next_step: str, current_ins_number: int) -> str:
    """Stub for GetNextStepEnglish: translates jump targets to human text.
    """
    if next_step == str(-2):
        return 'EXIT_LOOP'
    if next_step == str(-1):
        return 'DONE'
    if next_step.lower() == str(1):
        return 'Return True'
    if next_step == str(0):
        return f'Step {current_ins_number + 1}'
    return f'Step {next_step}'


def get_ins_type_def(ins_type: str | None) -> InsType:
    """Safely decode instruction type from string to InsType enum."""
    if ins_type is None:
        return InsType.UNKNOWN

    try:
        ins_type_def = InsType(int(ins_type))
    except (ValueError, TypeError):
        ins_type_def = InsType.UNKNOWN

    return ins_type_def
