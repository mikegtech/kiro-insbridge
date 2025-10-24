# enterprise_rating/ast_decoder/helpers/var_lookup.py

from kiro_insbridge.enterprise_rating.ast_decoder.defs import split_var_token
from kiro_insbridge.enterprise_rating.entities.algorithm import Algorithm
from kiro_insbridge.enterprise_rating.entities.dependency import DependencyBase
from kiro_insbridge.enterprise_rating.entities.program_version import ProgramVersion


def get_target_var_desc(target_var: str,dep: Algorithm | DependencyBase | None = None) -> str:
    """Return a human-readable description for 'target_var', using these rules:
    1) If target_var is one of the simple operator tokens ("=", ">", "<", "<=", ">=", "!=", "<>", "@", "^"),
       return the corresponding English phrase.
    2) If it begins with '{' or '[', strip punctuation and return the inner text.
    3) Otherwise, strip any leading "~" or "D", then parse prefix and ID/subID.
    4) If prefix == "GI", lookup in program_version.global_input_vars.
    5) Otherwise, look in these places (in order):
         • For LS: “Results of Step <ID>” (step is var_id).
         • For PL / GL: lookup in LookupVarExt / LookupVar (line-level).
         • For GR / PR: lookup in program_version.global_result_vars
         • For PC: lookup in algorithm_or_dependency.calculated_vars  (program lockdown)
         • For GC: lookup in global calculated vars (use line_id=0 for those)
         • For PP: lookup in program's calculated_vars (policy group)
         • For GP: similar to GC but only where .type == 1
         • For IG: use instructions_groups directly
         • For LX / IX: lookup in system_vars
         • For PQ: lookup in LookupVarExt as a local DS variable
         • For GQ: lookup in LookupVarExt but where prog_id=0
    6) If still not found, return target_var (or “-- Undefined Variable --”).
    """
    if(isinstance(dep, Algorithm)):
        return target_var

    try:
        prefix, var_id, sub_id = split_var_token(target_var)
    except ValueError:
        # Could not parse token → just return it
        return target_var

    # === 1) Handle operator tokens immediately ===

    if isinstance(dep, DependencyBase) and prefix in {"PC", "GC", "PP", "GP"} and dep.calc_index == var_id:
        # If the dependency has a description, return it
        return dep.description or target_var

    return target_var


def get_var_desc(
    target_var: str,
    token_type: str | None = None,
    deps: list[Algorithm | DependencyBase] | None = None,
    program_version: ProgramVersion | None = None,
) -> str:
    """Return a human‐readable description for 'target_var', using these rules:
    1) If target_var is one of the simple operator tokens ("=", ">", "<", "<=", ">=", "!=", "<>", "@", "^"),
       return the corresponding English phrase.
    2) If it begins with '{' or '[', strip punctuation and return the inner text.
    3) Otherwise, strip any leading "~" or "D", then parse prefix and ID/subID.
    4) If prefix == "GI", lookup in program_version.global_input_vars.
    5) Otherwise, look in these places (in order):
         • For LS: “Results of Step <ID>” (step is var_id).
         • For PL / GL: lookup in LookupVarExt / LookupVar (line-level).
         • For GR / PR: lookup in program_version.global_result_vars
         • For PC: lookup in algorithm_or_dependency.calculated_vars  (program lockdown)
         • For GC: lookup in global calculated vars (use line_id=0 for those)
         • For PP: lookup in program's calculated_vars (policy group)
         • For GP: similar to GC but only where .type == 1
         • For IG: use instructions_groups directly
         • For LX / IX: lookup in system_vars
         • For PQ: lookup in LookupVarExt as a local DS variable
         • For GQ: lookup in LookupVarExt but where prog_id=0
    6) If still not found, return target_var (or “-- Undefined Variable --”).
    """
    # === 1) Handle operator tokens immediately ===
    op_map = {
        "=": "[equals]",
        ">": "[greater than]",
        "<": "[less than]",
        "<=": "[less than or equal to]",
        ">=": "[greater than or equal to]",
        "!=": "[not equal to]",
        "<>": "[not equal to]",
        "@": "[bitwise AND]",
        "^": "[bitwise OR]",
    }
    if target_var in op_map:
        return op_map[target_var]

    # === 2) If it’s a literal in {…} or […], return inner text ===
    if target_var.startswith("{") or target_var.startswith("["):
        stripped = target_var.strip()
        # drop the leading/trailing brace or bracket
        inner = stripped[1:-1].strip()
        return inner or "NULL"

    # === 3) Parse prefix, var_id, and optional sub_id ===
    try:
        prefix, var_id, sub_id = split_var_token(target_var)
    except ValueError:
        # Could not parse token → just return it
        return target_var

    # === 4) If prefix == "GI", look in global_input_vars ===
    # 5j) LX / IX → System Variables (table: SystemVar)
    if prefix in {"GI", "LX", "IX"} and program_version is not None:
        # program_version.global_input_vars is assumed to be a list of InputVariable Pydantic models
        # each has fields: id (int), line_id, schema_id, var_desc, data_type, assign_type, etc.

        # Find the matching input variable
        for iv in program_version.data_dictionary.inputs:
            if iv.index == var_id and iv.line == program_version.line:
                return iv.description or target_var
        return target_var

    # 5a) LS → “Results of Step <var_id>”
    if prefix == "LS":
        return f"Results of Step {var_id}"

    if deps is not None:
        # 5b) PL → Program Lookup Vars (table: LookupVarExt filtered by prog_id and line_id)
        if prefix in {"PL", "GL", "PQ", "GQ"}:
            for dep in deps:  # Pydantic list of LookupVarExt
                if isinstance(dep, DependencyBase) and dep.is_table_variable() and dep.index == var_id:
                    return getattr(dep, "description", target_var) or target_var
            return target_var

        # 5d) GR / PR → Global Result Vars
        if prefix in {"GR", "PR"}:
            for dep in deps:  # Pydantic list of LookupVarExt
                if isinstance(dep, DependencyBase) and dep.is_result_variable() and dep.index == var_id:
                    return getattr(dep, "description", target_var) or target_var
            return target_var

        # 5e) PC → Program Calculated Vars (join to instructions-groups for description)
        if prefix in {"PC", "GC", "PP", "GP"}:
            for dep in deps:  # Pydantic list of LookupVarExt
                if isinstance(dep, DependencyBase) and dep.is_calculated_variable() and dep.calc_index == var_id:
                    return getattr(dep, "description", target_var) or target_var

            return target_var

    # 5m) If we fall through to here, prefix is unknown or not handled.
    return target_var
