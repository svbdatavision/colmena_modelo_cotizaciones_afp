import base64
import csv
import glob
import os
import re
import time
from typing import Optional
from urllib.parse import urljoin

import requests
from pypasser import reCaptchaV3
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from logging_utils import get_logger, log_exception
from pipeline_config import PipelineConfig
from pdf_storage import build_pdf_path, ensure_pdf_parent


MODELO_API_URL = os.getenv(
    "AFP_MODELO_API_URL",
    "https://api-kong.afpmodelo.net/mwd/wsAFPHerramientas/wmValidarCertificados",
)
MODELO_API_KEY = os.getenv("AFP_MODELO_API_KEY", "OaRrn6BPCURmbyo20HeKKR4qXqsHC42p")
CUPRUM_VALIDAR_REGEX = re.compile(r"Validar\.aspx\?ID=[^'\"&\s]+", re.IGNORECASE)


def create_driver(driver_path: str, download_path: str, headless: bool = True):
    options = Options()
    options.add_argument("--verbose")
    options.add_argument("--no-sandbox")
    if headless:
        options.add_argument("--headless")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--dns-prefetch-disable")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("window-size=1200x800")
    options.add_argument("log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
        },
    )
    if driver_path and os.path.exists(driver_path):
        service = Service(driver_path)
        driver = Chrome(service=service, options=options)
    else:
        driver = Chrome(options=options)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});",
            },
        )
    except Exception:
        pass
    driver.set_page_load_timeout(30)
    return driver


def _request_with_retry(
    method: str,
    url: str,
    timeout_seconds: int,
    retries: int,
    **kwargs,
) -> requests.Response:
    last_error: Optional[Exception] = None
    for _ in range(retries + 1):
        try:
            response = requests.request(method, url, timeout=timeout_seconds, **kwargs)
            return response
        except Exception as err:
            last_error = err
            time.sleep(1)
    raise last_error if last_error else Exception("request error")


def _is_pdf_content(content_type: str, content: bytes) -> bool:
    content_type = (content_type or "").lower()
    return "application/pdf" in content_type or (content or b"").startswith(b"%PDF")


def _write_pdf(output_pdf_path: str, content: bytes) -> None:
    with open(output_pdf_path, "wb") as handler:
        handler.write(content)


def _clean_rut(rut: str) -> str:
    return re.sub(r"[^0-9kK]", "", rut or "").upper()


def _extract_modelo_payload(payload: dict) -> tuple[str, str]:
    result = payload.get("wmValidarCertificadosResponse", {}).get("wmValidarCertificadosResult", {})
    table = result.get("diffgram", {}).get("NewDataSet", {}).get("TABLA_VALIDAR_CERTIFICADOS")
    if isinstance(table, list) and table:
        table = table[0]
    if not isinstance(table, dict):
        return "", ""
    return str(table.get("MESSAGE", "")), str(table.get("URL_DOCUMENTO", ""))


def _find_visible_text_inputs(driver, min_count: int = 2, timeout_seconds: int = 15):
    def _probe(_driver):
        fields = []
        for el in _driver.find_elements(By.CSS_SELECTOR, "input"):
            if not el.is_displayed():
                continue
            input_type = (el.get_attribute("type") or "text").lower()
            if input_type in {"text", "search", "tel", ""}:
                fields.append(el)
        return fields if len(fields) >= min_count else False

    return WebDriverWait(driver, timeout_seconds).until(_probe)


def _find_modelo_download_url(driver) -> str:
    selectors = [
        "a.download-btn",
        "a[download]",
        "a[href*='.pdf']",
        "a[href*='certificado']",
    ]
    for selector in selectors:
        for anchor in driver.find_elements(By.CSS_SELECTOR, selector):
            href = anchor.get_attribute("href") or ""
            if anchor.is_displayed() and href:
                return href
    return ""


def _extract_cuprum_validation_url(driver) -> str:
    current_url = driver.current_url or ""
    if CUPRUM_VALIDAR_REGEX.search(current_url):
        return current_url
    match = CUPRUM_VALIDAR_REGEX.search(driver.page_source or "")
    if not match:
        return ""
    return urljoin(config["cuprum"]["url"], match.group(0))


def _wait_for_downloaded_pdf(download_temp: str, previous_files: set[str], timeout_seconds: int = 12) -> str:
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        current_files = set(glob.glob(f"{download_temp}/*"))
        new_files = [path for path in current_files if path not in previous_files]
        for path in sorted(new_files, key=os.path.getctime, reverse=True):
            if path.endswith(".crdownload"):
                continue
            try:
                with open(path, "rb") as handler:
                    if handler.read(5).startswith(b"%PDF"):
                        return path
            except Exception:
                continue
        time.sleep(0.4)
    return ""


config = dict(
    modelo=dict(url="https://nueva.afpmodelo.cl/empleadores/herramientas-empleadores/validar-certificados"),
    cuprum=dict(
        url="https://www.cuprum.cl/wwwPublico/ValidaCertificados/Inicio.aspx",
        url_dwn="https://www.cuprum.cl/wwwPublico/ValidaCertificados/Validar.aspx?ID=",
    ),
    habitat=dict(
        url="https://www.afphabitat.cl/wp-admin/admin-ajax.php?action=ajax_call&funcion=getValidaCertificado",
        domain="https://www.afphabitat.cl",
        headers={
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "es-ES,es;q=0.5",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-ch-ua": '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
            "x-requested-with": "XMLHttpRequest",
        },
    ),
    provida=dict(
        url="https://w3.provida.cl/validador/descarga.ashx",
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "es-ES,es;q=0.9,en;q=0.8,es-CL;q=0.7",
            "sec-ch-ua": '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "sec-gpc": "1",
            "upgrade-insecure-requests": "1",
        },
    ),
    uno=dict(
        url="https://www.uno.cl/api/afiliado-certificado/validar",
        headers={
            "accept": "application/json, text/plain, */*",
            "accept-language": "es-ES,es;q=0.8",
            "content-type": "application/json;charset=UTF-8",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
        },
    ),
    planvital=dict(
        url="https://api2.planvital.cl/public/certificates/validate-certificate",
        url_captcha="https://www.google.com/recaptcha/enterprise/anchor?ar=1&k=6LdLsLcZAAAAABa5_AM2INGgCz6uszjY6EkzTBMT&co=aHR0cHM6Ly93d3cucGxhbnZpdGFsLmNsOjQ0Mw..&hl=es&v=rz4DvU-cY2JYCwHSTck0_qm-&size=invisible&cb=hx6152fusotd",
        headers={
            "accept": "application/json, text/plain, */*",
            "accept-language": "es-ES,es;q=0.9",
            "sec-ch-ua": '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "sec-gpc": "1",
            "Referer": "https://www.planvital.cl/",
        },
    ),
    capital=dict(
        url="https://www.afpcapital.cl/Empleador/Paginas/Validador-de-Certificados.aspx?IDList=10",
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "es-ES,es;q=0.8",
            "cache-control": "max-age=0",
            "content-type": "application/x-www-form-urlencoded",
            "sec-ch-ua": '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "sec-gpc": "1",
            "upgrade-insecure-requests": "1",
        },
    ),
)


def download_pdf(
    id: str = "",
    rut: str = "",
    codver: str = "",
    afp: str = "",
    driver=None,
    output_pdf_path: str = "",
    download_temp: str = "",
    timeout_seconds: int = 30,
    retries: int = 2,
):
    if not output_pdf_path:
        raise Exception("output_pdf_path is required")
    ensure_pdf_parent(output_pdf_path)

    if afp == "modelo":
        cleaned_rut = _clean_rut(rut).rjust(15, "0")
        modelo_error_message = ""
        if cleaned_rut and codver:
            req = _request_with_retry(
                "POST",
                MODELO_API_URL,
                timeout_seconds=timeout_seconds,
                retries=retries,
                json={"idPersona": cleaned_rut, "FolioCertificado": codver},
                headers={
                    "accept": "*/*",
                    "content-type": "application/json",
                    "x-api-key": MODELO_API_KEY,
                },
            )
            if req.status_code == 200:
                message, url = _extract_modelo_payload(req.json())
                modelo_error_message = message
                if url:
                    download_req = _request_with_retry(
                        "GET",
                        url,
                        timeout_seconds=timeout_seconds,
                        retries=retries,
                    )
                    if download_req.status_code == 200 and _is_pdf_content(
                        download_req.headers.get("content-type", ""),
                        download_req.content,
                    ):
                        _write_pdf(output_pdf_path, download_req.content)
                        return
                if message and "OPERACION EXITOSA" in message.upper():
                    raise Exception("Modelo validation returned success without downloadable PDF")

        if driver is None:
            raise Exception(f"modelo validation failed: {modelo_error_message or 'driver not initialized'}")

        driver.get(config[afp].get("url", ""))
        inputs = _find_visible_text_inputs(driver, min_count=2, timeout_seconds=15)
        inputs[0].clear()
        inputs[0].send_keys(rut)
        inputs[1].clear()
        inputs[1].send_keys(codver)
        candidates = [
            btn
            for btn in driver.find_elements(By.CSS_SELECTOR, "button, a.ant-btn")
            if btn.is_displayed()
        ]
        target_btn = None
        for btn in candidates:
            text = (btn.text or "").strip().lower()
            btn_id = (btn.get_attribute("id") or "").strip()
            if "validar certificado" in text or btn_id.startswith("B-"):
                target_btn = btn
                break
        if target_btn is None:
            raise Exception("Modelo validation button not found")
        driver.execute_script("arguments[0].click();", target_btn)
        url = WebDriverWait(driver, 15).until(lambda d: _find_modelo_download_url(d) or False)
        req = _request_with_retry("GET", url, timeout_seconds=timeout_seconds, retries=retries)
        if req.status_code == 200 and _is_pdf_content(req.headers.get("content-type", ""), req.content):
            _write_pdf(output_pdf_path, req.content)
            return
        raise Exception("No result")

    if afp == "cuprum":
        if driver is None:
            raise Exception("driver not initialized")
        driver.get(config[afp].get("url", ""))
        folio_clean = re.sub(r"^CU", "", codver or "", flags=re.IGNORECASE)
        rut_input = driver.find_element(By.ID, "txtRUT")
        folio_input = driver.find_element(By.ID, "intFolio")
        rut_input.clear()
        folio_input.clear()
        rut_input.send_keys(rut)
        folio_input.send_keys(folio_clean)

        previous_files = set(glob.glob(f"{download_temp}/*"))
        driver.execute_script(
            """
            if (typeof ValidacionForm === 'function') {
                ValidacionForm();
            } else {
                var btn = document.getElementById('btnaceptar');
                if (btn) { btn.click(); }
            }
            """
        )

        download_candidate = _wait_for_downloaded_pdf(
            download_temp=download_temp,
            previous_files=previous_files,
            timeout_seconds=min(15, timeout_seconds),
        )
        if download_candidate:
            os.rename(download_candidate, output_pdf_path)
            return

        not_found_markers = [
            "No Se han encontrado datos para rut",
            "No Se han encontrado datos para folio",
            "Ingrese su RUT",
            "Ingrese Nro. Folio",
        ]
        page_source = driver.page_source or ""
        if any(marker in page_source for marker in not_found_markers):
            raise Exception("Cuprum certificate not found for provided rut/folio")

        cookies = driver.get_cookies()
        sess = requests.Session()
        for cookie in cookies:
            sess.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path"),
            )
        try:
            user_agent = driver.execute_script("return navigator.userAgent;")
        except Exception:
            user_agent = "Mozilla/5.0"

        target_url = _extract_cuprum_validation_url(driver) or config[afp].get("url_dwn", "")
        candidates = []
        if target_url:
            candidates.append(target_url)
        if config[afp].get("url_dwn", "") and folio_clean:
            candidates.append(config[afp].get("url_dwn", "") + folio_clean)
            candidates.append(config[afp].get("url_dwn", "") + "CU" + folio_clean)

        seen = set()
        for candidate_url in candidates:
            if not candidate_url or candidate_url in seen:
                continue
            seen.add(candidate_url)
            req = sess.get(
                candidate_url,
                timeout=timeout_seconds,
                allow_redirects=True,
                headers={
                    "User-Agent": user_agent,
                    "Referer": config[afp].get("url", ""),
                },
            )
            if req.status_code == 200 and _is_pdf_content(req.headers.get("content-type", ""), req.content):
                _write_pdf(output_pdf_path, req.content)
                return

        download_candidate = _wait_for_downloaded_pdf(
            download_temp=download_temp,
            previous_files=previous_files,
            timeout_seconds=min(8, timeout_seconds),
        )
        if download_candidate:
            os.rename(download_candidate, output_pdf_path)
            return

        raise Exception("No result")

    if afp == "habitat":
        req = _request_with_retry(
            "POST",
            config[afp].get("url", ""),
            timeout_seconds=timeout_seconds,
            retries=retries,
            data={"folio": codver},
            headers=config[afp].get("headers", {}),
        )
        if req.status_code == 200:
            resp = req.json()
            if resp.get("resultado", "") == 0:
                time.sleep(3)
                req = _request_with_retry(
                    "GET",
                    config[afp].get("domain", "") + resp.get("mensaje", ""),
                    timeout_seconds=timeout_seconds,
                    retries=retries,
                )
                if req.status_code == 200:
                    with open(output_pdf_path, "wb") as handler:
                        handler.write(req.content)
                    return
        raise Exception("No result")

    if afp == "provida":
        reqid = re.sub("\\.", "", codver) + "-" + re.sub("[\\.-]", "", rut)
        req = _request_with_retry(
            "GET",
            config[afp].get("url", ""),
            timeout_seconds=timeout_seconds,
            retries=retries,
            params={"Id": reqid},
            headers=config[afp].get("headers", {}),
        )
        if req.status_code == 200:
            if req.content:
                with open(output_pdf_path, "wb") as handler:
                    handler.write(req.content)
                return
        raise Exception("No result")

    if afp == "uno":
        reqid = re.sub("[\\.-]", "", rut)
        req = _request_with_retry(
            "POST",
            config[afp].get("url", ""),
            timeout_seconds=timeout_seconds,
            retries=retries,
            json={"payload": {"idPersona": reqid, "FolioCertificado": codver}},
            headers=config[afp].get("headers", {}),
        )
        if req.status_code == 200:
            resp = req.json()
            if resp.get("codigo", "") == "0":
                with open(output_pdf_path, "wb") as handler:
                    handler.write(base64.b64decode(resp.get("data", {}).get("bytes", "").encode()))
                return
        raise Exception("No result")

    if afp == "capital":
        if driver is None:
            raise Exception("driver not initialized")
        driver.get(config[afp].get("url", ""))
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado1").send_keys(
            re.match("([A-Z0-9]{5})-", codver)[1]
        )
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado2").send_keys(
            re.match("([A-Z0-9]{5}-){1}([A-Z0-9]{5})", codver)[2]
        )
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado3").send_keys(
            re.match("([A-Z0-9]{5}-){2}([A-Z0-9]{5})", codver)[2]
        )
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado4").send_keys(
            re.match("([A-Z0-9]{5}-){3}([A-Z0-9]{5})", codver)[2]
        )
        driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtDigito").send_keys(
            re.match("([A-Z0-9]{5}-){4}([0-9]{1})", codver)[2]
        )
        btn = driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnValida")
        driver.execute_script("arguments[0].click();", btn)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnDescargaPdf"))
        )
        btn = driver.find_element(By.ID, "ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnDescargaPdf")
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
        os.rename(max(glob.glob(f"{download_temp}/*"), key=os.path.getctime), output_pdf_path)
        return

    if afp == "planvital":
        captcha_resp = reCaptchaV3(config[afp].get("url_captcha", ""))
        req = _request_with_retry(
            "GET",
            config[afp].get("url", ""),
            timeout_seconds=timeout_seconds,
            retries=retries,
            params={"certificateId": re.sub("[-]", "", codver), "rut": re.sub("[-\\.]", "", rut)},
            headers={**config[afp].get("headers", {}), "Recaptcha-Token": captcha_resp},
            verify=False,
        )
        time.sleep(1)
        req = _request_with_retry(
            "GET",
            config[afp].get("url", ""),
            timeout_seconds=timeout_seconds,
            retries=retries,
            params={"certificateId": re.sub("[-]", "", codver), "rut": re.sub("[-\\.]", "", rut)},
            headers={**config[afp].get("headers", {}), "Recaptcha-Token": captcha_resp},
            verify=False,
        )
        if req.status_code == 200:
            resp = req.json()
            if resp.get("valid", ""):
                with open(output_pdf_path, "wb") as handler:
                    handler.write(base64.b64decode(resp.get("data", {}).encode()))
                return
        raise Exception("No result")


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
    "res_afp",
]


def run(config_runtime: Optional[PipelineConfig] = None) -> str:
    config_runtime = config_runtime or PipelineConfig()
    config_runtime.ensure_directories()
    logger = get_logger(__name__, config_runtime.log_file_path, with_doc_id=True)

    output = []
    driver = None
    try:
        with open(config_runtime.output_csv_path, "r", encoding="utf-8") as handler:
            reader = csv.DictReader(handler, delimiter=";", escapechar="\\", quotechar='"')
            for row in reader:
                out = row
                if row["es_cert_cot"] == "True" and row["afp"] and row["rut"] and row["codver"]:
                    try:
                        if row["afp"] in {"cuprum", "capital"} and driver is None:
                            driver = create_driver(
                                driver_path=config_runtime.chromedriver_path,
                                download_path=config_runtime.temp_dir,
                                headless=config_runtime.selenium_headless,
                            )
                        download_pdf(
                            id=row["doc_idn"],
                            rut=row["rut"],
                            codver=row["codver"],
                            afp=row["afp"],
                            driver=driver,
                            output_pdf_path=build_pdf_path(
                                config_runtime,
                                row["doc_idn"],
                                "validacion",
                            ),
                            download_temp=config_runtime.temp_dir,
                            timeout_seconds=config_runtime.request_timeout_seconds,
                            retries=config_runtime.request_retries,
                        )
                        out["res_afp"] = "ok"
                    except Exception as err:
                        out["res_afp"] = "error"
                        log_exception(logger, err, doc_idn=row["doc_idn"])
                output.append(out)
    finally:
        if driver is not None:
            driver.quit()

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

    print("4_afp: OK")
    return config_runtime.output_csv_path


if __name__ == "__main__":
    run()
