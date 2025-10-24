def find_next_var(
    str_ptr: int,
    equation: str,
    ins_type: str
) -> tuple[str, str, str, str, str, int]:
    """Scan `equation` starting at index `str_ptr` and extract:
    - next_var:    the substring (variable or literal) up until a delimiter
    - next_op:     the English form of the delimiter/operator found after the var
    - round_var:   a flag ("NR") or emptiness if a rounding specifier is parsed
    - next_op_obj: the raw operator character (e.g. "+", "-", "*", "@", etc.)
    - round_var_obj: the raw rounding token (e.g. "RN", "RP2", "RM1", etc.)
    - next_var_ptr: the index where this variable began (same as input str_ptr)
    """
    # Initialize “output” variables
    next_op     = ""
    round_var   = ""
    next_op_obj = ""
    round_var_obj = ""
    next_var_ptr = str_ptr

    builder2 = ""   # will accumulate the characters of the next variable
    length = 1      # how many chars we’ve consumed so far
    eq_len = len(equation)

    # Attempt to read the first character at str_ptr into builder1
    try:
        builder1 = equation[str_ptr]
    except IndexError:
        builder1 = ""

    # If the instruction type is "5" (Set String), then an empty builder1 means “DONE”
    if ins_type == "5":
        if not builder1:
            return "", next_op, round_var, next_op_obj, round_var_obj, next_var_ptr
    else:
        # Otherwise, if builder1 is whitespace or empty, we’re done
        if not builder1.strip():
            return "", next_op, round_var, next_op_obj, round_var_obj, next_var_ptr

    num = 0  # tracks when we are inside “{…}” or “[…]”

    # Loop: keep consuming characters until we hit a delimiter (unless inside braces/brackets)
    while True:
        delim_hit = (
            builder1 in ("!", "+", "*", "/", "|", "@", "^")
            or builder1 == ""
            or builder1 is None
        )

        # The C# also excluded “#” for some instruction types; we haven’t encountered “#” logic here, so skip that.
        # If not inside a brace‐block (num != 1) and delim_hit is True, break out.
        if delim_hit and num == 0:
            break

        # Check for entering a brace “{” or bracket “[”
        if builder1 in ("{", "["):
            num = 1

        # The C# code had a special minus‐sign check:
        #   if (builder1 == "-" && num == 0 && builder2 != "GI_" && previous char not in “{[”)
        # We replicate that logic:
        if (
            builder1 == "-"
            and num == 0
            and builder2 != "GI_"
            and str_ptr - 1 >= 0
            and equation[str_ptr - 1] not in ("{", "[")
        ):
            # If that minus is a “leading minus” for a variable like “-5” or “-GI_…”,
            # then don’t treat it as a delimiter; just consume it as part of the variable.
            pass
        else:
            # If we are inside a brace/block and just hit a closing “}” or “]”, turn off num:
            if num == 1 and builder1 in ("}", "]"):
                num = 0

            # Append the character to builder2
            builder2 = equation[next_var_ptr : next_var_ptr + length]
            str_ptr += 1
            length += 1

            # If there are still characters left, read the next char into builder1
            if str_ptr < eq_len:
                builder1 = equation[str_ptr]
            else:
                # Reached end of string; break
                break
            continue

        # If we arrive here, it means delim_hit was True but we *were* inside braces or we didn't enter the above minus‐logic
        break

    # At this point, builder2 is the substring from next_var_ptr up to (but not including) the delimiter
    next_var = builder2

    # builder1 is now the delimiter/next‐operator character (or empty string if at end)
    next_op_obj = builder1

    # Translate raw operator to English
    if builder1 == "+":
        next_op = "plus"
    elif builder1 == "-":
        # In C# they only say “minus” if builder2 != "GI_"
        if builder2 != "GI_":
            next_op = "minus"
    elif builder1 == "*":
        next_op = "multiplied by"
    elif builder1 == "/":
        next_op = "divided by"
    elif builder1 == "@":
        next_op = "bitwise AND"
    elif builder1 == "^":
        next_op = "bitwise OR"
    elif builder1 == "=":
        next_op = "equals"
    else:
        next_op = ""

    # Advance past that operator character
    str_ptr += 1

    # Attempt to see if there’s a rounding suffix immediately following
    try:
        builder1 = equation[str_ptr]
    except IndexError:
        builder1 = ""
        round_var = "NR"

    try:
        if builder1:
            two = equation[str_ptr : str_ptr + 2]

            # If the next two chars are “RP” or “RM” → round up or round down
            if two in ("RP", "RM"):
                round_var = "NR"
                # If the remainder of the string is exactly “RP” or “RM”, consume only 2 chars
                if equation[str_ptr:] == two:
                    round_var_obj = two
                    str_ptr += 2
                else:
                    # Otherwise, consume 3 chars, e.g. “RP2”, “RM1”
                    round_var_obj = equation[str_ptr : str_ptr + 3]
                    str_ptr += 3

            # If the next two chars are “RN” → No Round
            elif equation[str_ptr : str_ptr + 2] == "RN":
                round_var_obj = "RN"
                round_var = "NR"
                str_ptr += 2

            # If builder1 is “R” but the next two chars are not “RV” → some other R‐prefix
            elif builder1 == "R" and equation[str_ptr : str_ptr + 2] != "RV":
                str_ptr += 1
                round_var = equation[str_ptr:]
                round_var_obj = equation[str_ptr - 1 :]
                str_ptr += len(equation[str_ptr:])

            else:
                round_var = "NR"
    except Exception:
        # If anything goes wrong here, default to “NR” (no rounding)
        round_var = "NR"

    return next_var, next_op, round_var, next_op_obj, round_var_obj, next_var_ptr
