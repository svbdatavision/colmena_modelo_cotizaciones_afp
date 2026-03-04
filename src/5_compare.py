import csv
import json
from pdfminer.high_level import extract_text
import unicodedata
import difflib
import logging

logging.basicConfig(filename='logs/errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(module)s doc_idn:%(doc_idn)s %(message)s')
logger = logging.getLogger(__name__)

def diff(x,y):
    differ = difflib.Differ()
    difs = list(differ.compare(x.splitlines(),y.splitlines()))
    res = []
    for d in difs:
        if d.startswith('-'):
            res.append(d)
        elif d.startswith('+'):
            res.append(d)
    return ' | '.join(res)

output = []
with open('output/certificados.csv','r') as f:
    reader = csv.DictReader(f, delimiter=";", escapechar='\\', quotechar='"')
    for row in reader:
        out = row
        if row['res_afp']=='ok':
            try:
                txt_afp = unicodedata.normalize('NFKD',extract_text(f"./pdfs/_{row['doc_idn']}.pdf")).encode('ascii', 'ignore').decode('utf-8')
                txt_ori = unicodedata.normalize('NFKD',extract_text(f"./pdfs/{row['doc_idn']}.pdf")).encode('ascii', 'ignore').decode('utf-8')
                if txt_afp != txt_ori:
                    dif = diff(txt_afp, txt_ori)
                else:
                    dif = ''
                out['es_dif'] = txt_afp != txt_ori
                out['res_dif'] = dif
            except Exception as err:
                logger.error(err, extra=dict(doc_idn=row['doc_idn']))
        output.append(out)


headers = ["doc_idn","link","periodo_produccion","fecha_ingreso","metadata_creator","metadata_producer","metadata_creadate","metadata_moddate","es_metadata","afp","es_cert_cot","codver","rut","res_afp","es_dif","res_dif"]
with open('output/certificados.csv', 'w', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=headers, delimiter=';', escapechar='\\', quotechar='"')
    writer.writeheader()
    writer.writerows(output)

print('5_compare: OK')