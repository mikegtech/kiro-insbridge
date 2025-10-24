
import xmltodict

from enterprise_rating.entities.srp_header import SrpHeader


class SrpHeaderRepository:
    """Repository for handling SRP Header XML data."""

    _NO_ARG = object()
    # Define attribute maps per entity
    ATTRIBUTE_MAPS = {
        "SrpHeader": {
            "idn_user": "user",
            "@pk": "prog_key",
            "@build_type": "build_type",
            "@location": "location",
            "@carrier_id": "carrier_id",
            "@carrier_name": "carrier_name",
            "@line_id": "line_id",
            "@line_desc": "line_desc",
            "@schema_id": "schema_id",
            "@program_id": "program_id",
            "@program_name": "program_name",
            "@version_desc": "version_desc",
            "@program_version": "program_version",
            "@parent_company": "parent_company",
            "@notes": "notes",
        }
    }

    @staticmethod
    def _entity_aware_postprocessor(path, key, value=_NO_ARG):

        attr_map = {}
        # Determine the entity type based on the path
        if path and isinstance(path[-1], tuple):
            parent = path[-1][0]
            if parent == "idn_user":
                attr_map = SrpHeaderRepository().ATTRIBUTE_MAPS.get("SrpHeader", {})
            elif parent == "module_request":
                attr_map = SrpHeaderRepository.ATTRIBUTE_MAPS.get("SrpHeader", {})

        mapped_key = attr_map.get(key, key)

        return mapped_key, value


    @staticmethod
    def get_srp_header(xml_file: str) -> SrpHeader | None:
        with open(xml_file, encoding="utf-8") as f:
            doc = xmltodict.parse(
                f.read(), postprocessor=SrpHeaderRepository()._entity_aware_postprocessor, force_list=("idn_user", "module_request")
            )

        srp_header_data = doc.get("export", {})

        if srp_header_data is None:
            return None

        srp_header = SrpHeader.model_validate(srp_header_data)

        return srp_header
