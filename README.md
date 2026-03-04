# modelo_cotizaciones_afp - Databricks Azure Lift & Shift

Migracion de `modelo_cotizaciones_afp` desde GCP + Snowflake a Databricks Azure, preservando la logica funcional del pipeline:

`1_extract -> 2_download -> 3_parse -> 4_afp -> 5_compare -> 6_upload`

## 1) Estructura final

```text
/notebooks/
  modelo_cotizaciones_afp_main.py

/src/
  1_extract.py        # wrapper compatible
  2_download.py       # wrapper compatible
  3_parse.py          # wrapper compatible
  4_afp.py            # wrapper compatible
  5_compare.py        # wrapper compatible
  6_upload.py         # wrapper compatible
  extract.py          # Spark SQL extract
  download.py         # descarga PDFs
  parse.py            # parseo PDF (regex intactas)
  afp.py              # validacion AFP (requests/selenium)
  compare.py          # comparacion PDF original vs AFP
  upload.py           # persistencia Spark SQL
  pipeline.py         # orquestador secuencial
  pipeline_config.py  # config runtime y rutas
  logging_utils.py
  secrets.py

/scripts/
  databricks_init_selenium.sh
```

## 2) Storage recomendado (produccion)

**Opcion implementada y recomendada:** ADLS Gen2 via **Unity Catalog Volume** (externo).

- Ruta recomendada: `/Volumes/<catalog>/<schema>/<volume>/modelo_cotizaciones_afp`
- Variable runtime: `AFP_STORAGE_BASE_PATH`

Motivo:
- Governance y permisos UC
- No depende de DBFS root
- Persistencia compartida entre jobs/notebooks

> `pipeline_config.py` usa por defecto `/Volumes/main/default/modelo_cotizaciones_afp` cuando detecta Databricks runtime.

## 3) Manejo de secretos

No hay credenciales hardcodeadas.

- `src/_conn.py` ya no contiene credenciales Snowflake (conexion removida).
- `src/secrets.py` permite leer secretos desde:
  1. Databricks Secret Scope (`dbutils.secrets.get`)
  2. Variables de entorno

Ejemplo de uso para ADLS directo (opcional):
- `configure_adls_oauth(...)` en `src/secrets.py`

## 4) Migracion SQL (Snowflake -> Spark SQL)

### Extract (step 1)
- Antes: `SELECT` Snowflake sobre `OPX.P_DDV_OPX.AFP_CERTIFICADOS`
- Ahora: mismo `SELECT` en Spark SQL (`src/extract.py`)
  - mantiene filtro por fecha
  - mantiene exclusiones por `DOC_IDN` ya cargado
  - mantiene `ORDER BY` y `LIMIT 240`

### Upload (step 6)
- Antes: inserts fila a fila en Snowflake
- Ahora: DataFrame Spark + `insertInto` tabla Spark SQL (`src/upload.py`)
  - mantiene transformaciones (`RUT_L11`, bools, limpieza metadata dates)
  - columnas y orden equivalentes

## 5) Ejecucion en Databricks

Notebook de entrada:

- `/notebooks/modelo_cotizaciones_afp_main.py`

Widgets:
- `storage_base_path`
- `source_table`
- `target_table`
- `chromedriver_path`
- `run_extract` (true/false)

### Recomendacion de cluster

- Databricks Runtime: **14.3 LTS** o superior
- Modo: Standard
- Autoscaling sugerido: 1-3 workers
- Worker sugerido: `Standard_D4ds_v5` (o equivalente)

Para Selenium en cluster:
- usar init script `scripts/databricks_init_selenium.sh`
- asegurar `AFP_CHROMEDRIVER_PATH=/databricks/driver/chromedriver`

## 5.1) Validacion E2E en Databricks (antes de env setup)

Notebook de validacion:

- `/notebooks/modelo_cotizaciones_afp_validate.py`

Este notebook ejecuta el pipeline y devuelve un JSON con checks:
- existencia de storage base
- existencia de chromedriver
- existencia y tamano de PDFs original/validado
- primera fila de `output/certificados.csv`
- `target_table_count`
- `res_afp`, `es_dif`, `afp`, `rut`, `codver`

### Modo recomendado de prueba inicial (sin extract)

Configurar widgets:
- `run_extract = false`
- `seed_doc_idn = 166088887`
- `seed_link = https://w3.provida.cl/validador/descarga.ashx?Id=245756274-188906699`
- `seed_periodo_produccion = 2024-12-01`
- `seed_fecha_ingreso = 2026-03-04`
- `target_table = <catalog>.<schema>.afp_certificados_output_tmp`

Resultado esperado:
- `res_afp = ok`
- `es_dif = False`
- `original_pdf_exists = true`
- `validated_pdf_exists = true`
- `target_table_count >= 1`

### Modo productivo (con extract)

Configurar:
- `run_extract = true`
- `source_table` y `target_table` reales en UC/Hive metastore

Nota: la consulta de extract mantiene el anti-join contra `target_table`.
Por eso `target_table` debe existir previamente en el catalogo.

## 6) Prueba E2E ejecutada (real)

Se ejecuto prueba real completa del flujo sobre un caso ProVida:

- `doc_idn`: **166088887**
- link real PDF:
  - `https://w3.provida.cl/validador/descarga.ashx?Id=245756274-188906699`

Pasos ejecutados:
1. `2_download`: descarga PDF real (HTTP 200)
2. `3_parse`: extrae metadata/afp/rut/codver
3. `4_afp`: valida contra AFP y descarga PDF validado
4. `5_compare`: compara textos
5. `6_upload`: persiste en tabla Spark temporal

Resultado observado:
- `res_afp = ok`
- `es_dif = False`
- filas en Spark table temporal: `1`
- artefactos PDF generados:
  - `166088887.pdf`
  - `_166088887.pdf`

## 7) CI/CD GitLab

Se incluye `/.gitlab-ci.yml` con:
- validacion de sintaxis (`compileall`)
- smoke test de imports
- job de integracion e2e manual (network-dependent)

## 8) Variables de entorno utiles

- `AFP_STORAGE_BASE_PATH`
- `AFP_SOURCE_TABLE`
- `AFP_TARGET_TABLE`
- `AFP_CHROMEDRIVER_PATH`
- `AFP_TABLE_PROVIDER` (`delta` en Databricks, `parquet` para local)

## 9) Checklist de migracion

- [x] Flujo funcional conservado en orden 1->6
- [x] Regex y validaciones AFP preservadas
- [x] Snowflake removido de ejecucion activa
- [x] Extract y upload migrados a Spark SQL
- [x] Storage preparado para ADLS Gen2 (UC Volume)
- [x] Secrets externalizados
- [x] Notebook Databricks funcional
- [x] Prueba E2E real ejecutada

## 10) Riesgos detectados

1. **Conectividad de origen interno** (`mt.colmena.cl`) depende de red corporativa/DNS privada.
2. **Selenium en Databricks** requiere init script y version compatible de Chrome/driver.
3. **PlanVital** depende de captcha y puede fallar por cambios anti-bot.
4. **Entornos no Databricks** pueden no tener Delta habilitado; usar `AFP_TABLE_PROVIDER=parquet` para pruebas locales.
