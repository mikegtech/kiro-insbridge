import logging
import os
from dataclasses import asdict
from pathlib import Path

import xmltodict

from enterprise_rating.ast_decoder.ast_nodes import (
    ArithmeticNode,
    AssignmentNode,
    CompareNode,
    FunctionNode,
    IfNode,
    RawNode,
)
from enterprise_rating.ast_decoder.decoder import decode_ins  # noqa: F401
from enterprise_rating.entities.dependency import CalculatedVariable, DependencyBase
from enterprise_rating.entities.program_version import ProgramVersion  # wherever you defined your Pydantic models

logger = logging.getLogger(__name__)


class ProgramVersionRepository:  # noqa: D101
    _NO_ARG = object()

    env_xml = os.environ.get("PROGRAM_VERSION_XML")
    if env_xml is None:
        raise RuntimeError(
            "Environment variable 'PROGRAM_VERSION_XML' is not set."
        )
    XML_FILE = Path(env_xml)

    # Define attribute maps per entity
    ATTRIBUTE_MAPS = {
        "ProgramVersion": {
            "@sub": "subscriber",
            "@line": "line",
            "@schema": "schema_id",
            "@prog": "program_id",
            "@ver": "version",
            "@verName": "version_name",
            "@pk": "primary_key",
            "@gpk": "global_primary_key",
            "@ed": "effective_date",
            "@ed_exact": "effective_date_exact",
            "@persisted": "persisted",
            "@date_mask": "date_mask",
            "@culture": "culture",
            "@decimal_symbol": "decimal_symbol",
            "@group_symbol": "group_symbol",
            "schema": "data_dictionary",
            "seq": "algorithm_seq",
            # Add more attribute mappings specific to ProgramVersion
        },
        "DataDictionary": {
            "schema": "data_dictionary",
            # Example: "@id": "schema_id",
            # Add attribute mappings specific to DataDictionary if needed
        },
        "Category": {"@l": "line", "@i": "index", "@p": "parent", "@d": "description"},
        "Input": {
            "@l": "line",
            "@i": "index",
            "@dt": "data_type",
            "@d": "description",
            "@qt": "qual_type",
            "@c": "category_id",
            "@sys": "system_var",
        },
        "AlgorithmSequence": {
            "seq": "algorithm_seq",
            "@n": "sequence_number",
            "@u": "universal",
            # Example: "@id": "schema_id",
            # Add attribute mappings specific to DataDictionary if needed
        },
        "Algorithm": {
            "item": "algorithm",
            "@pk": "prog_key",
            "@rk": "revision_key",
            "@alg": "alg_type",
            "@qt": "qual_type",
            "@cat": "category_id",
            "@d": "description",
            "@dlm": "date_last_modified",
            "@i": "index",
            "@v": "version",
            "@p": "program_id",
            "@assign_fltr": "assign_filter",
            "@adv_type": "advanced_type",
            "d": "dependency_vars",
            "i": "steps",
        },
        "DependencyBase": {
            "@pk": "prog_key",
            "@rk": "revision_key",
            "@i": "index",
            "@v": "version",
            "@cid": "calc_index",
            "@d": "description",
            "@alg": "alg_type",
            "@cat": "category_id",
            "@p": "program_id",
            "@dt": "data_type",
            "@t": "ib_type",
            "@dlm": "date_last_modified",
            "@u": "universal",
            "@sys": "system_var",
            "@processed": "processed",
            "@level": "level_id",
            "d": "dependency_vars",
            "i": "steps",
            # Add more attribute mappings specific to DependencyBase if needed
        },
        "Instruction": {
            "i": "steps",
            "@n": "n",  # step number
            "@t": "t",  # instruction type code
            "@ins": "ins",  # raw instruction string
            "@ins_tar": "ins_tar",  # instruction target (optional)
            "@seq_t": "seq_t",  # index of next-if-true (optional)
            "@seq_f": "seq_f",  # index of next-if-false (optional)
            "ast": "ast",  # AST translation of the instruction
        }
        # Add more entities and their attribute maps as needed
    }

    @staticmethod
    def _entity_aware_postprocessor(path, key, value=_NO_ARG):
        # only process if there's something to process

        # Unwrap {"item": {...}} at any level
        if isinstance(value, dict) and set(value.keys()) == {"item"}:
            value = value["item"]

        attr_map = {}
        # Determine the entity type based on the path
        if path and isinstance(path[-1], tuple):
            parent = path[-1][0]
            if parent == "export":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("ProgramVersion", {})
            elif parent == "schema":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("DataDictionary", {})
            elif parent == "categories":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("Category", {})
            elif parent == "inputs":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("Input", {})
            elif parent == "seq":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("AlgorithmSequence", {})
            elif parent == "item":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("Algorithm", {})
            elif parent == "d":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("DependencyBase", {})
            elif parent == "i":
                attr_map = ProgramVersionRepository.ATTRIBUTE_MAPS.get("Instruction", {})

        mapped_key = attr_map.get(key, key)

        # Flatten categories and inputs, and map their children
        if mapped_key == "categories" and isinstance(value, dict) and "c" in value:
            value = value["c"]
            # Map each category dict's keys
            if isinstance(value, list):
                value = [
                    {ProgramVersionRepository.ATTRIBUTE_MAPS["Category"].get(k, k): v for k, v in item.items()}
                    for item in value
                ]
            elif isinstance(value, dict):
                value = [{ProgramVersionRepository.ATTRIBUTE_MAPS["Category"].get(k, k): v for k, v in value.items()}]

        if mapped_key == "inputs" and isinstance(value, dict) and "iv" in value:
            value = value["iv"]
            # Map each input dict's keys
            if isinstance(value, list):
                value = [
                    {ProgramVersionRepository.ATTRIBUTE_MAPS["Input"].get(k, k): v for k, v in item.items()}
                    for item in value
                ]
            elif isinstance(value, dict):
                value = [{ProgramVersionRepository.ATTRIBUTE_MAPS["Input"].get(k, k): v for k, v in value.items()}]

        # Flatten algorithm_seq and map their children
        if mapped_key == "algorithm" and isinstance(value, dict) and "item" in value:
            value = value["item"]
            # Map each category dict's keys
            if isinstance(value, list):
                value = [
                    {ProgramVersionRepository.ATTRIBUTE_MAPS["Algorithm"].get(k, k): v for k, v in item.items()}
                    for item in value
                ]
            elif isinstance(value, dict):
                value = [{ProgramVersionRepository.ATTRIBUTE_MAPS["Algorithm"].get(k, k): v for k, v in value.items()}]

        # Flatten algorithm_seq and map their children
        if mapped_key == "dependency_vars" and value is not ProgramVersionRepository._NO_ARG:
            if isinstance(value, dict):
                # Single dependency
                value = {ProgramVersionRepository.ATTRIBUTE_MAPS["DependencyBase"].get(k, k): v for k, v in value.items()}
            elif isinstance(value, list):
                # If only one, maybe unwrap
                if len(value) == 1:
                    value = {ProgramVersionRepository.ATTRIBUTE_MAPS["DependencyBase"].get(k, k): v for k, v in value[0].items()}
                else:
                    # If more than one, keep as list
                    value = [
                        {ProgramVersionRepository.ATTRIBUTE_MAPS["DependencyBase"].get(k, k): v for k, v in item.items()}
                        for item in value
                    ]

            ProgramVersionRepository._current_dependencies = value  # <-- Store dependencies

        # Flatten algorithm_seq and map their children
        if mapped_key == "steps":
            # value is already the steps list/dict
            if isinstance(value, dict):
                # Single dependency
                value = {ProgramVersionRepository.ATTRIBUTE_MAPS["Instruction"].get(k, k): v for k, v in value.items()}
            elif isinstance(value, list):
                # If only one, maybe unwrap
                if len(value) == 1:
                    value = {ProgramVersionRepository.ATTRIBUTE_MAPS["Instruction"].get(k, k): v for k, v in value[0].items()}
                else:
                    # If more than one, keep as list
                    value = [
                        {ProgramVersionRepository.ATTRIBUTE_MAPS["Instruction"].get(k, k): v for k, v in item.items()}
                        for item in value
                    ]

            value["ast"] = None

        return mapped_key, value

    @staticmethod
    def _node_to_dict(obj) -> dict | list | str | int | None:
        """Recursively convert an ASTNode (or list of ASTNode) into a plain python dict/list.
        If obj is an ASTNode subclass, we convert its __dict__ but recurse on any nested ASTNode or list.
        Otherwise, return obj as-is (e.g. str, int).
        """
        # 1) If it’s exactly None, return None
        if obj is None:
            return None

        # 2) If it’s a list, convert each element
        if isinstance(obj, list):
            return [ProgramVersionRepository._node_to_dict(item) for item in obj]

        # 3) If it’s one of our ASTNode subclasses, convert it
        if isinstance(obj, (RawNode, CompareNode, IfNode, ArithmeticNode, FunctionNode, AssignmentNode)):
            result = {}
            for key, val in obj.__dict__.items():
                result[key] = ProgramVersionRepository._node_to_dict(val)
            return result

        # 4) Otherwise (primitives: str, int, etc.), return raw
        return obj

    @staticmethod
    def process_all_instructions(progver: ProgramVersion):

        # 1) Iterate over every AlgorithmSequence → every Algorithm
        for alg_seq in progver.algorithm_seq:
            algorithm = alg_seq.algorithm

            # 1.a) Process steps that live directly on this Algorithm
            # 1.b) Process every DependencyBase under this Algorithm (including nested CalculatedVariable chains)
            dependency_vars = getattr(algorithm, "dependency_vars", []) or []

            main_steps = getattr(algorithm, "steps", []) or []
            for instr in main_steps:
                # At this point, instr must be an Instruction model (not a dict).
                # Its ast field was defined as: ast: list[Any]|None = None
                if instr.ast is None:
                    try:
                        # Produce a plain dict to hand into decode_ins(...)
                        raw_dict = instr.model_dump()
                        nodes = decode_ins(raw_dict, dependency_vars, progver)
                        # Store back as list of dicts (as your Instruction.ast is a list[Any])
                        instr.ast = [asdict(n) for n in nodes] if nodes else []
                        # instr.ast = [
                        #    ProgramVersionRepository._node_to_dict(n) for n in nodes
                        # ]
                    except Exception as e:
                        error_node = RawNode(
                            step=int(raw_dict.get("n", 0)),
                            ins_type=int(raw_dict.get("t", 0)) if raw_dict.get("t") is not None else None,
                            raw="",
                            value=f"Repository ERROR: {e}"
                        )
                        instr.ast = [ProgramVersionRepository._node_to_dict(error_node)]

            queue: list[DependencyBase] = []
            for dep in dependency_vars:
                queue.append(dep)

            while queue:
                cur_dep = queue.pop(0)
                dep_vars = getattr(cur_dep, "dependency_vars", []) or []

                # Process this dependency’s own steps (each should be an Instruction model)
                dep_steps = getattr(cur_dep, "steps", []) or []
                for instr in dep_steps:
                    if instr.ast is None:
                        try:
                            raw_dict = instr.model_dump()
                            nodes = decode_ins(raw_dict, dep_vars, progver, cur_dep)
                            instr.ast = [asdict(n) for n in nodes] if nodes else []

                        except Exception:
                            instr.ast = []

                # If this dependency is a CalculatedVariable, enqueue its nested dependency_vars
                nested = getattr(cur_dep, "dependency_vars", []) or []
                for nd in nested:
                    if isinstance(nd, CalculatedVariable):
                        queue.append(nd)


    @staticmethod
    def get_program_version(lob: str, progId: str, progVer: str) -> ProgramVersion | None:
        with open(ProgramVersionRepository.XML_FILE, encoding="utf-8") as f:
            doc = xmltodict.parse(
                f.read(), postprocessor=ProgramVersionRepository._entity_aware_postprocessor, force_list=("seq", "dependency_vars", "steps", "i", "ast")
            )

        progver_data = doc.get("export", {})

        if progver_data is None:
            return None

        progver = ProgramVersion.model_validate(progver_data)
        ProgramVersionRepository.process_all_instructions(progver)

        # Extract the relevant data for the ProgramVersion entity
        # Let Pydantic coerce types, apply defaults, and validate
        return progver
