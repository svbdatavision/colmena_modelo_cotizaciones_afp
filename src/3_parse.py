import os
import re
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.high_level import extract_text
import unicodedata
import csv
import logging

logging.basicConfig(filename='logs/errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(module)s doc_idn:%(doc_idn)s %(message)s')
logger = logging.getLogger(__name__)

def extract_meta(input):
    try:
        return input.decode()
    except:
        return ''

config = {
    'modelo': {
        'nombre': 'A.F.P. Modelo S.A.',
        'codver': 'Folio.*:\s+([a-z\d]+)',
        'metadata': {
            'Creator': 'Crystal Reports',
            'Producer':'Powered By Crystal'
        },
        'download': 'https://api-kong-preprod.afpmodelo.net/mwd/wsAFPHerramientas/wmValidarCertificados'
    },
    'habitat': {
        'nombre': 'AFP Habitat',
        'codver': '([a-z\d]{8}-(?:[a-z\d]{4}-){3}[a-z\d]{12})',
        'metadata': {
            'Creator': 'JasperReports',
            'Producer':'iText1.3.1'
        },
        'download': 'https://www.afphabitat.cl/wp-admin/admin-ajax.php?action=ajax_call&funcion=getValidaCertificado'
    },
    'cuprum': {
        'nombre': 'AFP CUPRUM S.A.',
        'codver': 'FOLIO\s+N.?\s+CU(\d+)',
        'metadata':{
            'Creator':'',
            'Producer':'null'
            },
        'download': 'https://www.cuprum.cl/wwwPublico/ValidaCertificados/Inicio.aspx'
    },
    'capital': {
        'nombre': 'AFP CAPITAL S.A.',
        'codver': 'certificacion:\s+([a-z|\d|-]+)',
        'metadata': {
            'Creator': 'PDFsharp',
            'Producer':'PDFsharp'
        },
        'download': 'https://www.afpcapital.cl/Empleador/Paginas/Validador-de-Certificados.aspx?IDList=10'
    },
    'provida': {
        'nombre': 'AFP ProVida S.A.',
        'codver': 'certificado:\s+([\d|\.]+)',
        'metadata': {
            'Creator':'',
            'Producer':'iText'
        },
        'download': 'https://w3.provida.cl/validador/descarga.ashx?Id={nro_cert}-{rut}'
    },
    'planvital': {
        'nombre': 'AFP PlanVital S.A.',
        'codver': 'Folio\s+([\d-]+)',
        'metadata': {
            'Creator':'Telerik Reporting',
            'Producer':'Telerik Reporting'
            },
        'download': 'https://api2.planvital.cl/public/certificates/validate-certificate?certificateId={nro_cert}&rut={rut}&ipClient=181.43.34.60'
        },
    'uno': {
        'nombre': 'AFP UNO',
        'codver': 'Certificacion\s+N.?:\s+([a-z|\d]+)',
        'metadata': {
            'Creator':'Crystal Reports',
            'Producer':'Powered By Crystal'
            },
            'download': 'https://www.uno.cl/api/afiliado-certificado/validar'
        }
    }



output = []
with open('input/certificados.csv','r') as f:
    reader = csv.DictReader(f, delimiter=";", escapechar='\\')
    for row in reader:
        file = row['doc_idn']
        out = row
        filePath = f'./pdfs/{file}.pdf'

        try:
            fp = open(filePath, 'rb')
            parser = PDFParser(fp)
            doc = PDFDocument(parser)
            meta = doc.info[0]
            out['metadata_creator'] = extract_meta(meta.get('Creator',''))
            out['metadata_producer'] = extract_meta(meta.get('Producer',''))
            out['metadata_creadate'] = extract_meta(meta.get('CreationDate',''))
            out['metadata_moddate'] = extract_meta(meta.get('ModDate',''))
        except Exception as err:
            logger.error(err, extra=dict(doc_idn=file))
        
        try:
            txt = unicodedata.normalize('NFKD',extract_text(filePath)).encode('ascii', 'ignore').decode('utf-8')
        except:
            txt = ''

        try:    
            afp = re.search('\n+A\.?F\.?P\.? +(\w+) ?(S\.A\.)?', txt, re.IGNORECASE) # identifica afp.
            if afp:
                out['afp'] = afp[1].lower()
            else:
                out['afp'] = 'habitat' if re.search('afphabitat\.cl', txt, re.IGNORECASE) else ''
        except:
            pass

        try:
            out['es_metadata'] = True if re.search(config[out['afp']]['metadata']['Producer'], out['metadata_producer'], re.IGNORECASE) else False
        except:
            pass

        out['es_cert_cot'] = True if re.search('certificado(?:\s+\w+){,2}\s+cotizaciones', txt, re.IGNORECASE) else False # Si es certificado de cotizaciones
        
        try:
            out['rut'] = re.search('\d{1,2}(?:\.\d{3}){2}-[\dkK]',txt)[0] # extrae rut
        except:
            pass

        try:
            out['codver'] = re.search(config[out['afp']]['codver'], txt, re.IGNORECASE)[1]
        except:
            pass

        output.append(out)

headers = ["doc_idn","link","periodo_produccion","fecha_ingreso","metadata_creator","metadata_producer","metadata_creadate","metadata_moddate","es_metadata","afp","es_cert_cot","codver","rut"]
with open('output/certificados.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=headers, delimiter=';', escapechar='\\', quotechar='"')
    writer.writeheader()
    writer.writerows(output)

print('3_parse: OK')