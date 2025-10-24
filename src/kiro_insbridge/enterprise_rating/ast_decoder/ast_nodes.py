# enterprise_rating/ast_decoder/ast_nodes.py

from __future__ import annotations

from dataclasses import dataclass, field

from enterprise_rating.ast_decoder.defs import InsType


@dataclass
class ASTNode:
    """Common fields for _all_ AST nodes.
    - step       : the step number (n)
    - ins_type   : the numeric INS_TYPE code
    - english    : optional fallback English snippet
    - template_id: key into templates.yml (e.g. "IF_COMPARE")
    - step_type  : copy of ins_type (so you can re-label it later)
    """

    step: int
    ins_type: InsType
    english: str = field(default="",   kw_only=True)
    template_id: str = field(default="",   kw_only=True)
    step_type: InsType | None = field(default=None,   kw_only=True)


@dataclass
class RawNode(ASTNode):
    """A simple leaf node carrying a single value (literal or variable)."""

    raw: str
    value: str
    type: str | None = None

@dataclass
class CompareNode(ASTNode):
    """Represents a binary comparison: left ∘ right (e.g. GI_84 > GC_47)."""

    left: RawNode
    operator: str
    right: RawNode
    cond_op: str | None = None


@dataclass
class Step:
    number: int
    nodes: list[ASTNode]
    render_flag: bool = False
    # internal cache
    _english: str | None = field(default=None, init=False, repr=False)

    @property
    def english(self) -> str:
        # don’t render unless the flag is True
        if not self.render_flag:
            return ""
        # cache the concatenation
        if self._english is None:
            parts = [n.english for n in self.nodes if getattr(n, "english", "")]
            self._english = " ".join(parts)
        return self._english

@dataclass
class IfNode(ASTNode):
    """An IF node with a CompareNode condition and two branches."""

    true_branch: list[ASTNode]  = field(default_factory=list)
    false_branch: list[ASTNode] = field(default_factory=list)
    condition: CompareNode | MultiConditionNode | TypeCheckNode | None = field(default=None,   kw_only=True)


@dataclass
class MultiConditionNode(ASTNode):
    """Holds multiple CompareNode conditions joined by a single operator (“OR”/^ or “AND”/+).
    """

    conditions: list[CompareNode]
    joiner: str  # "OR" or "AND"
    cond_op: str | None = None


@dataclass
class ArithmeticNode(ASTNode):
    """Represents an arithmetic computation: left ∘ right [round_spec]."""

    left: RawNode
    operator: str
    right: RawNode
    round_spec:    str | None = None
    round_english: str | None = None


@dataclass
class FunctionNode(ASTNode):
    """A generic function or call (e.g. string concat, date-diff, data-source)."""

    name: str
    args: list[RawNode]
    round_spec: str | None = None


@dataclass
class AssignmentNode(ASTNode):
    """Represents a SET_STRING or similar:
    var := expr
    """

    var:    str
    expr:   ASTNode
    target: str | None = None
    next_true:  list[ASTNode] | None = None
    next_false: list[ASTNode] | None = None

@dataclass
class JumpNode(ASTNode):
    """Represents a jump to another step in the program.
    - target_step: the step number to jump to
    - target_ins_type: the INS_TYPE of the target instruction
    """

    target: int | None = None

@dataclass
class TypeCheckNode(ASTNode):
    """Represents a unary type‐check (date / numeric / alpha) on a single variable.
    """

    left: RawNode
    check_type: str    # e.g. "date", "numeric", "alpha"
