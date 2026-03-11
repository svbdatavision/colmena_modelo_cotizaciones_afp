import os
import csv
import re
import unicodedata
from typing import Optional

from pdfminer.high_level import extract_text
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser

from logging_utils import get_logger, log_exception
from pipeline_config import PipelineConfig
from pdf_storage import resolve_pdf_path


def extract_meta(input_value):
    try:
        return input_value.decode()
    except Exception:
        return ""


config = {
    "modelo": {
        "nombre": "A.F.P. Modelo S.A.",
        "codver": "Folio.*:\\s+([a-z\\d]+)",
        "metadata": {
            "Creator": "Crystal Reports",
            "Producer": "Powered By Crystal",
        },
        "download": "https://api-kong-preprod.afpmodelo.net/mwd/wsAFPHerramientas/wmValidarCertificados",
    },
    "habitat": {
        "nombre": "AFP Habitat",
        "codver": "([a-z\\d]{8}-(?:[a-z\\d]{4}-){3}[a-z\\d]{12})",
        "metadata": {
            "Creator": "JasperReports",
            "Producer": "iText1.3.1",
        },
        "download": "https://www.afphabitat.cl/wp-admin/admin-ajax.php?action=ajax_call&funcion=getValidaCertificado",
    },
    "cuprum": {
        "nombre": "AFP CUPRUM S.A.",
        "codver": "FOLIO\\s+N.?\\s+CU(\\d+)",
        "metadata": {
            "Creator": "",
            "Producer": "null",
        },
        "download": "https://www.cuprum.cl/wwwPublico/ValidaCertificados/Inicio.aspx",
    },
    "capital": {
        "nombre": "AFP CAPITAL S.A.",
        "codver": "certificacion:\\s+([a-z|\\d|-]+)",
        "metadata": {
            "Creator": "PDFsharp",
            "Producer": "PDFsharp",
        },
        "download": "https://www.afpcapital.cl/Empleador/Paginas/Validador-de-Certificados.aspx?IDList=10",
    },
    "provida": {
        "nombre": "AFP ProVida S.A.",
        "codver": "certificado:\\s+([\\d|\\.]+)",
        "metadata": {
            "Creator": "",
            "Producer": "iText",
        },
        "download": "https://w3.provida.cl/validador/descarga.ashx?Id={nro_cert}-{rut}",
    },
    "planvital": {
        "nombre": "AFP PlanVital S.A.",
        "codver": "Folio\\s+([\\d-]+)",
        "metadata": {
            "Creator": "Telerik Reporting",
            "Producer": "Telerik Reporting",
        },
        "download": "https://api2.planvital.cl/public/certificates/validate-certificate?certificateId={nro_cert}&rut={rut}&ipClient=181.43.34.60",
    },
    "uno": {
        "nombre": "AFP UNO",
        "codver": "Certificacion\\s+N.?:\\s+([a-z|\\d]+)",
        "metadata": {
            "Creator": "Crystal Reports",
            "Producer": "Powered By Crystal",
        },
        "download": "https://www.uno.cl/api/afiliado-certificado/validar",
    },
}


HEADERS = [
    "doc_idn",
    "link",
    "periodo_produccion",
    "fecha_ingreso",
    "metadata_creator",
    "metadata_producer",
    "metadata_creadate",
    "metadata_moddate",
    "es_metadata",
    "afp",
    "es_cert_cot",
    "codver",
    "rut",
]


def run(config_runtime: Optional[PipelineConfig] = None) -> str:
    config_runtime = config_runtime or PipelineConfig()
    config_runtime.ensure_directories()
    logger = get_logger(__name__, config_runtime.log_file_path, with_doc_id=True)

    output = []
    with_pdf = 0
    missing_pdf = 0
    with open(config_runtime.input_csv_path, "r", encoding="utf-8") as handler:
        reader = csv.DictReader(handler, delimiter=";", escapechar="\\")
        for row in reader:
            file_name = row["doc_idn"]
            out = row
            file_path = resolve_pdf_path(config_runtime, file_name, "original")
            file_exists = os.path.exists(file_path)

            if file_exists:
                with_pdf += 1
                try:
                    with open(file_path, "rb") as fp:
                        parser = PDFParser(fp)
                        doc = PDFDocument(parser)
                        meta = doc.info[0]
                        out["metadata_creator"] = extract_meta(meta.get("Creator", ""))
                        out["metadata_producer"] = extract_meta(meta.get("Producer", ""))
                        out["metadata_creadate"] = extract_meta(meta.get("CreationDate", ""))
                        out["metadata_moddate"] = extract_meta(meta.get("ModDate", ""))
                except Exception as err:
                    log_exception(logger, err, doc_idn=file_name)
            else:
                missing_pdf += 1
                log_exception(
                    logger,
                    FileNotFoundError(f"original pdf not found: {file_path}"),
                    doc_idn=file_name,
                )

            if file_exists:
                try:
                    txt = (
                        unicodedata.normalize("NFKD", extract_text(file_path))
                        .encode("ascii", "ignore")
                        .decode("utf-8")
                    )
                except Exception:
                    txt = ""
            else:
                txt = ""

            try:
                afp = re.search("\\n+A\\.?F\\.?P\\.? +(\\w+) ?(S\\.A\\.)?", txt, re.IGNORECASE)
                if afp:
                    out["afp"] = afp[1].lower()
                else:
                    out["afp"] = "habitat" if re.search("afphabitat\\.cl", txt, re.IGNORECASE) else ""
            except Exception:
                pass

            try:
                out["es_metadata"] = (
                    True
                    if re.search(
                        config[out["afp"]]["metadata"]["Producer"],
                        out["metadata_producer"],
                        re.IGNORECASE,
                    )
                    else False
                )
            except Exception:
                pass

            out["es_cert_cot"] = (
                True
                if re.search("certificado(?:\\s+\\w+){,2}\\s+cotizaciones", txt, re.IGNORECASE)
                else False
            )

            try:
                out["rut"] = re.search("\\d{1,2}(?:\\.\\d{3}){2}-[\\dkK]", txt)[0]
            except Exception:
                pass

            try:
                out["codver"] = re.search(config[out["afp"]]["codver"], txt, re.IGNORECASE)[1]
            except Exception:
                pass

            output.append(out)

    with open(config_runtime.output_csv_path, "w", encoding="utf-8") as handler:
        writer = csv.DictWriter(
            handler,
            fieldnames=HEADERS,
            delimiter=";",
            escapechar="\\",
            quotechar='"',
        )
        writer.writeheader()
        writer.writerows(output)

    print(f"3_parse: OK (with_pdf={with_pdf}, missing_pdf={missing_pdf})")
    return config_runtime.output_csv_path


if __name__ == "__main__":
    run()
