import base64
import csv
import glob
import os
import re
import time
from typing import Optional

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


def create_driver(driver_path: str, download_path: str, headless: bool = True):
    options = Options()
    options.add_argument("--verbose")
    options.add_argument("enable-automation")
    options.add_argument("--no-sandbox")
    if headless:
        options.add_argument("--headless")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--dns-prefetch-disable")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("window-size=1200x800")
    options.add_argument("log-level=3")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
        },
    )
    service = Service(driver_path)
    driver = Chrome(service=service, options=options)
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
    pdfs_dir: str = "",
    download_temp: str = "",
    timeout_seconds: int = 30,
    retries: int = 2,
):
    if afp == "modelo":
        if driver is None:
            raise Exception("driver not initialized")
        driver.get(config[afp].get("url", ""))
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div[1]/div[2]/input').send_keys(rut)
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div[2]/div[2]/input').send_keys(codver)
        btn = driver.find_element(By.XPATH, '//*[@id="B-000020"]')
        driver.execute_script("arguments[0].click();", btn)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div/div/a'))
        )
        url = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div/div/a').get_attribute("href")
        req = _request_with_retry("GET", url, timeout_seconds=timeout_seconds, retries=retries)
        if req.status_code == 200:
            with open(f"{pdfs_dir}/_{id}.pdf", "wb") as handler:
                handler.write(req.content)
            return
        raise Exception("No result")

    if afp == "cuprum":
        if driver is None:
            raise Exception("driver not initialized")
        driver.get(config[afp].get("url", ""))
        driver.find_element(By.ID, "txtRUT").send_keys(rut)
        driver.find_element(By.ID, "intFolio").send_keys(re.sub("CU", "", codver))
        driver.find_element(By.ID, "btnaceptar").click()
        cookies = driver.get_cookies()
        sess = requests.Session()
        for cookie in cookies:
            sess.cookies.set(cookie["name"], cookie["value"])
        req = sess.get(config[afp].get("url_dwn", ""), timeout=timeout_seconds)
        if req.status_code == 200:
            with open(f"{pdfs_dir}/_{id}.pdf", "wb") as handler:
                handler.write(req.content)
            try:
                os.remove(max(glob.glob(f"{download_temp}/*"), key=os.path.getctime))
            except Exception:
                pass
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
                    with open(f"{pdfs_dir}/_{id}.pdf", "wb") as handler:
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
                with open(f"{pdfs_dir}/_{id}.pdf", "wb") as handler:
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
                with open(f"{pdfs_dir}/_{id}.pdf", "wb") as handler:
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
        os.rename(max(glob.glob(f"{download_temp}/*"), key=os.path.getctime), f"{pdfs_dir}/_{id}.pdf")
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
                with open(f"{pdfs_dir}/_{id}.pdf", "wb") as handler:
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
                        if row["afp"] in {"modelo", "cuprum", "capital"} and driver is None:
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
                            pdfs_dir=config_runtime.pdfs_dir,
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
