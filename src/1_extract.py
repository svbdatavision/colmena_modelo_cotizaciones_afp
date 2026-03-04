from sqlalchemy import create_engine
from dotenv import load_dotenv
from _conn import connection
import os
import datetime
import logging

logging.basicConfig(filename='logs/errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(module)s %(message)s')
logger = logging.getLogger(__name__)

date_from = datetime.date.today() - datetime.timedelta(days=30)

query = f"""
select DOC_IDN,LINK,PERIODO_PRODUCCION,FECHA_INGRESO
from "OPX"."P_DDV_OPX"."AFP_CERTIFICADOS" 
where cast(FECHA_INGRESO as date) >= '{date_from.strftime('%Y-%m-%d')}' and 
DOC_IDN not in (select DOC_IDN from "OPX"."P_DDV_OPX"."AFP_CERTIFICADOS_OUTPUT")
order by doc_idn
limit 240
"""

headers = "doc_idn;link;periodo_produccion;fecha_ingreso\n"

try:
    ctx = connection()
    ctx.cursor().execute('USE WAREHOUSE P_OPX')
    results = ctx.cursor().execute(query).fetchall()
    out = list()
    for row in results:
        r = list()
        for res in row:
            if isinstance(res, datetime.datetime):
                aux = res.strftime('%Y-%m-%d')
                r.append(aux)
            else:
                r.append(str(res))
        out.append(';'.join(r)+"\n")
    with open('input/certificados.csv', 'w') as f:
        f.write(headers)
        f.writelines(out)
    print('1_extract: OK')
except Exception as err:
    logger.error(err)
finally:
    ctx.close()
