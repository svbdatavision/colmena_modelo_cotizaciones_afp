from urllib.parse import urlparse


DOC_SOURCE_BASE_URL = "http://10.0.15.58:8080/notifEnvioMailRest/public/documento"
DOC_SOURCE_ENV_KEY = "AFP_DOC_SOURCE_BASE_URL"


def normalize_doc_link(original_link: str, base_url: str = DOC_SOURCE_BASE_URL) -> str:
    if not original_link:
        return original_link

    if "notifEnvioMailRest/public/documento/" not in original_link:
        return original_link

    parsed = urlparse(original_link)
    path = (parsed.path or "").strip("/")
    doc_idn = path.split("/")[-1] if path else ""
    if not doc_idn:
        return original_link

    return f"{base_url.rstrip('/')}/{doc_idn}"


def normalize_source_link(original_link: str, base_url: str = DOC_SOURCE_BASE_URL) -> str:
    return normalize_doc_link(original_link=original_link, base_url=base_url)
