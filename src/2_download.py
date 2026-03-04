import csv
import requests
import time
import logging

logging.basicConfig(filename='logs/errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(module)s doc_idn:%(doc_idn)s %(message)s')
logger = logging.getLogger(__name__)

with open('input/certificados.csv','r') as f:
    reader = csv.DictReader(f, delimiter=";", escapechar='\\')
    for row in reader:
        id_ = row['doc_idn']
        link = row['link']
        try:
            response = requests.get(link)
            if response.status_code == 200:
                with open(f'pdfs/{id_}.pdf','wb') as f:
                    f.write(response.content)
            else:
                raise(f'status code {response.status_code}')
        except Exception as err:
            logger.error(err, extra=dict(doc_idn=id_))
        time.sleep(1)
    print('2_download: OK')
