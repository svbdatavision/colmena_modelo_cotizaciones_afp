import glob
import os
import csv
import re
import base64
import time
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from pypasser import reCaptchaV3
import requests
import logging

logging.basicConfig(filename='logs/errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(module)s doc_idn:%(doc_idn)s %(message)s')
logger = logging.getLogger(__name__)

#### cambiar
#driverpath = '/home/jp/Documents/Moebius/Colmena/colmena-cotizaciones/chromedriver' 
#downloadpath = '/home/jp/Documents/Moebius/Colmena/colmena-cotizaciones/pdfs'

driverpath = '/home/certafp/proyecto_cotizaciones/chromedriver/chromedriver' 
downloadpath = '/home/certafp/proyecto_cotizaciones/pdfs'
downloadtemp = '/home/certafp/proyecto_cotizaciones/tmp'

def create_driver(driver=driverpath, download=downloadpath):
    options = Options()
    options.add_argument('--verbose')
    options.add_argument('enable-automation')
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument('--ignore-certificate-errors')
    #options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--dns-prefetch-disable')
    options.add_argument('--remote-debugging-port=9222')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-gpu')
    options.add_argument('window-size=1200x800')
    options.add_argument('log-level=3')
    #options.add_argument('user-data-dir=/home/certafp/proyecto_cotizaciones/dir')
    options.add_experimental_option('prefs', {
        'download.default_directory': download,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True
    })

    service = Service(driver)
    driver = Chrome(service=service,options=options)
    driver.set_page_load_timeout(30)
    return driver

config = dict(
    modelo = dict(
        url = 'https://nueva.afpmodelo.cl/empleadores/herramientas-empleadores/validar-certificados'
    ),
    cuprum = dict(
        url = 'https://www.cuprum.cl/wwwPublico/ValidaCertificados/Inicio.aspx',
        url_dwn = 'https://www.cuprum.cl/wwwPublico/ValidaCertificados/Validar.aspx?ID='
    ),
    habitat = dict(
        url = 'https://www.afphabitat.cl/wp-admin/admin-ajax.php?action=ajax_call&funcion=getValidaCertificado',
        domain = 'https://www.afphabitat.cl',
        headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'es-ES,es;q=0.5',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'x-requested-with': 'XMLHttpRequest'
    }
    ),
    provida = dict(
        url = 'https://w3.provida.cl/validador/descarga.ashx',
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'es-ES,es;q=0.9,en;q=0.8,es-CL;q=0.7',
            'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
        }
    ),
    uno = dict(
        url = 'https://www.uno.cl/api/afiliado-certificado/validar',
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'es-ES,es;q=0.8',
            'content-type': 'application/json;charset=UTF-8',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1'
        }
    ),
    planvital = dict(
        url = 'https://api2.planvital.cl/public/certificates/validate-certificate',
        url_captcha = 'https://www.google.com/recaptcha/enterprise/anchor?ar=1&k=6LdLsLcZAAAAABa5_AM2INGgCz6uszjY6EkzTBMT&co=aHR0cHM6Ly93d3cucGxhbnZpdGFsLmNsOjQ0Mw..&hl=es&v=rz4DvU-cY2JYCwHSTck0_qm-&size=invisible&cb=hx6152fusotd',
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'es-ES,es;q=0.9',
            'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'sec-gpc': '1',
            'Referer': 'https://www.planvital.cl/'
        }
    ),
    capital = dict(
        url = 'https://www.afpcapital.cl/Empleador/Paginas/Validador-de-Certificados.aspx?IDList=10',
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'es-ES,es;q=0.8',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1'
        }
    )
)

def download_pdf(id='',rut='',codver='',afp='',driver=None):
    if afp == 'modelo':
        driver.get(config[afp].get('url',''))
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div[1]/div[2]/input').send_keys(rut)
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div[2]/div[2]/input').send_keys(codver)
        btn = driver.find_element(By.XPATH, '//*[@id="B-000020"]')
        driver.execute_script("arguments[0].click();", btn)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div/div/a')))
        url = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/main/div/div/div/div[3]/div/div/a').get_attribute('href')
        req = requests.get(url)
        if req.status_code == 200:
            with open(f'./pdfs/_{id}.pdf', 'wb') as f:
                f.write(req.content)
            return
        raise Exception('No result')        
    
    if afp == 'cuprum':
        driver.get(config[afp].get('url',''))
        driver.find_element(By.ID, 'txtRUT').send_keys(rut)
        driver.find_element(By.ID, 'intFolio').send_keys(re.sub('CU','',codver))
        driver.find_element(By.ID, 'btnaceptar').click()
        cookies = driver.get_cookies()
        sess = requests.Session()
        for cookie in cookies:
            sess.cookies.set(cookie['name'], cookie['value'])
        req = sess.get(config[afp].get('url_dwn',''))
        if req.status_code == 200:
            with open(f'./pdfs/_{id}.pdf', 'wb') as f:
                f.write(req.content)
            try:
                os.remove(max(glob.glob(f'{downloadtemp}/*'), key=os.path.getctime))
            except:
                pass
            return
        raise Exception('No result')
        

    if afp == 'habitat':
        req = requests.post(config[afp].get('url',''), data={'folio':codver}, headers=config[afp].get('headers',{}))
        if req.status_code == 200:
            resp = req.json()
            if resp.get('resultado','') == 0:
                time.sleep(3)
                req = requests.get(config[afp].get('domain','')+resp.get('mensaje',''))
                if req.status_code == 200:
                    with open(f'./pdfs/_{id}.pdf', 'wb') as f:
                        f.write(req.content)
                    return
        raise Exception('No result')

    if afp == 'provida':
        reqid = re.sub('\.','',codver)+'-'+re.sub('[\.-]','',rut)
        req = requests.get(config[afp].get('url',''),params={'Id':reqid}, headers=config[afp].get('headers',{}))
        if req.status_code == 200:
            if req.content:
                with open(f'./pdfs/_{id}.pdf', 'wb') as f:
                    f.write(req.content)
                return
        raise Exception('No result')
    
    if afp == 'uno':
        reqid = re.sub('[\.-]','',rut)
        req = requests.post(config[afp].get('url',''), json={'payload':{'idPersona':reqid,'FolioCertificado':codver}}, headers=config[afp].get('headers',{}))
        if req.status_code == 200:
            resp = req.json()
            if resp.get('codigo','') == '0':
                with open(f'./pdfs/_{id}.pdf', 'wb') as f:
                    f.write(base64.b64decode(resp.get('data',{}).get('bytes','').encode()))
                return
        raise Exception('No result')

    if afp == 'capital':
        driver.get(config[afp].get('url',''))
        driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado1').send_keys(re.match('([A-Z0-9]{5})-',codver)[1])
        driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado2').send_keys(re.match('([A-Z0-9]{5}-){1}([A-Z0-9]{5})',codver)[2])
        driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado3').send_keys(re.match('([A-Z0-9]{5}-){2}([A-Z0-9]{5})',codver)[2])
        driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtCertificado4').send_keys(re.match('([A-Z0-9]{5}-){3}([A-Z0-9]{5})',codver)[2])
        driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_txtDigito').send_keys(re.match('([A-Z0-9]{5}-){4}([0-9]{1})',codver)[2])
        btn = driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnValida')
        driver.execute_script("arguments[0].click();", btn)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnDescargaPdf')))
        btn = driver.find_element(By.ID, 'ctl00_ctl57_g_5e11d149_fe88_43a9_ba53_891df882a3f3_btnDescargaPdf')
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
        os.rename(max(glob.glob(f'{downloadtemp}/*'), key=os.path.getctime), f'{downloadpath}/_{id}.pdf')
        return
        raise Exception('No result')

    if afp == 'planvital':
        captcha_resp = reCaptchaV3(config[afp].get('url_captcha',''))
        req = requests.get(config[afp].get('url',''),params={'certificateId':re.sub('[-]','',codver), 'rut': re.sub('[-\.]','',rut)}, headers={**config[afp].get('headers',{}),'Recaptcha-Token':captcha_resp}, verify=False)
        time.sleep(1)
        req = requests.get(config[afp].get('url',''),params={'certificateId':re.sub('[-]','',codver), 'rut': re.sub('[-\.]','',rut)}, headers={**config[afp].get('headers',{}),'Recaptcha-Token':captcha_resp}, verify=False)
        if req.status_code == 200:
            resp = req.json()
            if resp.get('valid',''):
                with open(f'./pdfs/_{id}.pdf', 'wb') as f:
                    f.write(base64.b64decode(resp.get('data',{}).encode()))
                return
        raise Exception('No result')
        

output = []
with open('output/certificados.csv','r') as f:
    reader = csv.DictReader(f, delimiter=";", escapechar='\\', quotechar='"')
    try:
        driver = create_driver(download=downloadtemp)
        for row in reader:
            out = row
            if row['es_cert_cot']=='True' and row['afp'] and row['rut'] and row['codver']:
                try:
                    download_pdf(id=row['doc_idn'],rut=row['rut'],codver=row['codver'],afp=row['afp'],driver=driver)
                    out['res_afp'] = 'ok'
                except Exception as err:
                    out['res_afp'] = 'error'
                    logger.error(err, extra=dict(doc_idn=row['doc_idn']))
            output.append(out)
    except Exception as e:
        raise e
    finally:
        driver.quit()

headers = ["doc_idn","link","periodo_produccion","fecha_ingreso","metadata_creator","metadata_producer","metadata_creadate","metadata_moddate","es_metadata","afp","es_cert_cot","codver","rut","res_afp"]
with open('output/certificados.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=headers, delimiter=';', escapechar='\\', quotechar='"')
    writer.writeheader()
    writer.writerows(output)

print('4_afp: OK')