import shutil
import zipfile
from pathlib import Path

import xmltodict

try:
    import pyzipper

    _HAS_PYZIPPER = True
except ImportError:
    _HAS_PYZIPPER = False

from kiro_insbridge.enterprise_rating.config import get_config
from kiro_insbridge.enterprise_rating.entities.srp_request import Srp, SrpRequest
from kiro_insbridge.enterprise_rating.entities.srp_request_user import SrpRequestUser


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
        },
        "Srp":{
            "param": "srpuser",
            "module_request": "srp_header",
        },
        "SrpRequestUser": {
            "param": "srpuser",
            "@user_name": "user_name",
            "@fullname": "full_name",
            "@email_address": "email_address",
        },
    }

    @staticmethod
    def _srp_request_header_to_export_header(req: Srp) -> dict:
        """Convert Srp entity to export header dictionary."""
        header_export = {
            "export" : {
                "template": {
                    "header": {
                        "@l": req.srp_header.line_id,
                        "@schema": req.srp_header.schema_id,
                        "@product": req.srp_header.program_id,
                        "@version": req.srp_header.program_version,
                        "@date_created": req.srp_header.date_created,
                        "@carrier_id": req.srp_header.carrier_id, 
                    }
                },  
            }
        }

        return header_export
    
    @staticmethod
    def _entity_aware_postprocessor(path, key, value=_NO_ARG):
        if isinstance(value, dict) and set(value.keys()) == {"item"}:
            value = value["item"]
        
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
    def move_files_flat(source_dir: Path, dest_dir: Path, overwrite: bool = False) -> None:
        """Move all files from source directory to destination directory (flat, no subdirs).

        Args:
            source_dir: Source directory path
            dest_dir: Destination directory path
            overwrite: Whether to overwrite existing files
        """
        if not source_dir.exists() or not source_dir.is_dir():
            return

        dest_dir.mkdir(parents=True, exist_ok=True)

        for item in source_dir.iterdir():
            if item.is_file():
                dest_file = dest_dir / item.name
                if overwrite or not dest_file.exists():
                    shutil.move(str(item), str(dest_file))

    @staticmethod
    def zip_directory_universal(source_dir: Path, zip_path: Path, password: str | None = None) -> None:
        """Zip a directory with optional password protection.

        Args:
            source_dir: Directory to zip
            zip_path: Output zip file path
            password: Optional password for encryption
        """
        pwd = password.encode("utf-8") if password else None

        if _HAS_PYZIPPER and pwd:
            with pyzipper.AESZipFile(
                str(zip_path), "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES
            ) as zf:
                zf.setpassword(pwd)
                for file_path in source_dir.rglob("*"):
                    if file_path.is_file():
                        zf.write(file_path, file_path.relative_to(source_dir))
        else:
            with zipfile.ZipFile(str(zip_path), "w", zipfile.ZIP_DEFLATED) as zf:
                if pwd:
                    zf.setpassword(pwd)
                for file_path in source_dir.rglob("*"):
                    if file_path.is_file():
                        zf.write(file_path, file_path.relative_to(source_dir))

    @staticmethod
    def get_srp_header(xml_file: str) -> Srp | None:
        print(f"Reading SRP Header from XML file: {xml_file}")

        config = get_config()

        with open(xml_file, encoding="utf-8") as f:
            doc = xmltodict.parse(
                f.read(),
                force_list=("idn_user",),
                postprocessor=SrpHeaderRepository._entity_aware_postprocessor,
            )

        # Get the root element (env)
        env_data = doc.get("env")

        if env_data is None:
            return None

        # Extract user info from param.idn_user
        param_data = env_data.get("param", {})
        idn_user_list = param_data.get("idn_user", [])
        idn_user_data = idn_user_list[0] if isinstance(idn_user_list, list) and idn_user_list else {}

        # Extract header info from module_request
        module_request_data = env_data.get("module_request", {})

        # Build SrpRequestUser
        srp_user = SrpRequestUser(
            user_name=idn_user_data.get("@user_name", ""),
            full_name=idn_user_data.get("@fullname", ""),
            email_address=idn_user_data.get("@email_address", ""),
        )

        # Build SrpRequest from module_request
        srp_header = SrpRequest(
            schema=module_request_data.get("@schema"),
            prog_key=module_request_data.get("pk", ""),
            build_type=module_request_data.get("build_type", ""),
            location=module_request_data.get("location", ""),
            carrier_id=module_request_data.get("carrier_id", ""),
            carrier_name=module_request_data.get("carrier_name", ""),
            line_id=module_request_data.get("line_id", ""),
            line_desc=module_request_data.get("line_desc", ""),
            schema_id=module_request_data.get("schema_id", ""),
            program_id=module_request_data.get("program_id", ""),
            program_name=module_request_data.get("program_name", ""),
            version_desc=module_request_data.get("version_desc", ""),
            program_version=module_request_data.get("program_version", ""),
            parent_company=module_request_data.get("parent_company", ""),
            notes=module_request_data.get("notes"),
            date_created=module_request_data.get("date_created"),
        )

        # Build Srp wrapper
        srp = Srp(srp_header=srp_header, srpuser=srp_user)

        tree = SrpHeaderRepository()._srp_request_header_to_export_header(srp)
        xml_str = xmltodict.unparse(
            tree,
            pretty=True,
            indent="    ",
            full_document=True,
        )

        dest = (
            Path(xml_file).parent
            / "export"
            / str(srp.srp_header.line_desc)
            / str(srp.srp_header.program_name)
            / str(srp.srp_header.date_created).replace(" ", "_").replace("/", "_").replace(":", "_")
            / "header.xml"
        )

        dest.parent.mkdir(parents=True, exist_ok=True)

        SrpHeaderRepository().move_files_flat(Path(xml_file).parent, dest.parent, overwrite=True)
        SrpHeaderRepository().move_files_flat(Path(xml_file).parent / "rtd", dest.parent, overwrite=True)
        SrpHeaderRepository().move_files_flat(Path(xml_file).parent / "rto", dest.parent, overwrite=True)

        dest.write_text(xml_str, encoding="utf-8")

        SrpHeaderRepository().zip_directory_universal(
            dest.parent,
            dest.parent.with_suffix(".srtp"),
            config.ingest.zip_password if config.ingest else None,
        )

        try:
            shutil.rmtree(dest.parent)
        except OSError:
            pass

        return srp
