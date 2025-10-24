from pathlib import Path

import yaml
from jinja2 import Template

from enterprise_rating.ast_decoder.ast_nodes import (ArithmeticNode,
                                                     AssignmentNode,
                                                     FunctionNode, IfNode,
                                                     JumpNode)

# Load once at module import
with open(Path(__file__).parent / "templates.yml", encoding="utf-8") as f:
   _cfg = yaml.safe_load(f)

# Load and compile all templates at import time
# _cfg = yaml.safe_load(Path(__file__).parent / "templates.yml")
TEMPLATES = {
    tpl_id: Template(tpl_text)
    for tpl_id, tpl_text in _cfg["templates"].items()
}
STEP_TYPES = _cfg["step_types"]



def render_node_old(node):
    """Turn a single AST node into a polished English sentence,
    prefixed with the human-readable step-type label.
    """
    # 1) look up the step-type
    step_label = STEP_TYPES.get(str(node.ins_type), f"Type {node.ins_type}")

    # 2) pick the right AST template
    tpl = TEMPLATES.get(node.template_id, "{english}")

    # 3) fill in fields common to all node shapes
    filled = tpl.format(
        left=getattr(node.condition.left, "raw", ""),
        op=getattr(node.condition, "operator", ""),
        right=getattr(node.condition.right, "raw", ""),
        cond_op=node.condition.cond_op if hasattr(node.condition, "cond_op") else "",
        args=", ".join(arg.raw for arg in getattr(node, "args", [])),
        target=getattr(node, "target", ""),
        english=getattr(node, "english", ""),
    )

    return f"**{step_label}**: {filled}"


def render_node(node) -> str:
    """Render any ASTNode according to its template_id.
    Special-case nodes without a `condition` (like JumpNode).
    If anything goes wrong, capture the exception text as node.english.
    """
    try:
        tpl: Template | None = TEMPLATES.get(node.template_id)
        # if no template, fall back to pre-set .english
        if tpl is None:
            return getattr(node, "english", f"No Template found: {node.template_id}") or "What?"

        # 1) JumpNode: only {{ target }}
        if isinstance(node, JumpNode):
            return tpl.render(target=node.target).strip()

        # ANY IfNode, whether single‐ or multi‐clause:
        if isinstance(node, IfNode):
            # grab either the multi‐list or fall back to single
            cond = node.condition
            if cond is not None and hasattr(cond, "conditions"):
                clauses = cond.conditions
            else:
                clauses = [cond] if cond is not None else []

            return tpl.render(
                conditions=[
                    {
                        "left":  c.left.value,
                        "op":    c.operator,
                        "right": c.right.value,
                    }
                    for c in clauses
                ],
                joiner=getattr(cond, "joiner", ""),
                true_target=(
                    node.true_branch[0].target
                    if node.true_branch and isinstance(node.true_branch[0], JumpNode)
                    else None
                ),
                false_target=(
                    node.false_branch[0].target
                    if node.false_branch and isinstance(node.false_branch[0], JumpNode)
                    else None
                )
            )


        # 3) ArithmeticNode: {{ left }}, {{ operator }}, {{ right }}, {{ round_spec }}
        if isinstance(node, ArithmeticNode):
            ctx = {
                "left":       node.left.raw,
                "operator":   node.operator,
                "right":      node.right.raw,
                "round_spec": node.round_spec or "",
            }
            return tpl.render(**ctx)

        # 4) FunctionNode: {{ name }}, {{ args }}, {{ round_spec }}
        if isinstance(node, FunctionNode):
            ctx = {
                "name":       node.name,
                "args":       ", ".join(arg.raw for arg in node.args),
                "round_spec": getattr(node, "round_spec", "") or "",
            }
            return tpl.render(**ctx)

        if isinstance(node, AssignmentNode):
            # 4a) AssignmentNode: {{ var }}, {{ expr }}, {{ target }}
            ctx = {
                "name":       "Arithmetic",
                "args":       ", ".join(arg.value for arg in node.expr.args),
                "round_spec": getattr(node, "round_spec", "") or "",
                "next_true" : str(node.next_true and node.next_true[0].target),
                "next_false": str(node.next_false and node.next_false[0].target),
            }
            return tpl.render(**ctx)


        # 5) Fallback to any .english on the node
        return getattr(node, "english", "") or ""

    except Exception as e:
        # On error, store and return the exception text
        err = str(e)
        node.english = err
        return err



def render_node_new(node):
    tpl = TEMPLATES.get(node.template_id)
    if tpl:
        if isinstance(node, JumpNode):
            return tpl.format(target=node.target)
        if isinstance(node, IfNode):
            return tpl.format(
                left=node.condition.left.raw,
                op=node.condition.operator,
                right=node.condition.right.raw
            )
        if isinstance(node, ArithmeticNode):
            # fill in rounding if present:
            round_fmt = ""
            if getattr(node, "round_spec", None):
                round_fmt = f" (round to {node.round_spec} places)"
            # choose template or fallback
            if tpl:
                text = tpl.format(
                    left=node.left.raw,
                    operator=node.operator,
                    right=node.right.raw,
                    rounding=round_fmt
                )
    # fall back to node.english if someone pre-populated it
    return node.english or ""
