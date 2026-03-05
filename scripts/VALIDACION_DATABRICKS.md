# Validacion operativa en Databricks

Este documento sirve para validar que el pipeline corre end-to-end en Databricks.

## 1) Pre-requisitos

1. Cluster activo (DBR 14.3 LTS+ recomendado).
2. Repo sincronizado en `main`.
3. Dependencias instaladas:

```python
%pip install -r /Workspace/Repos/<tu_usuario>/<tu_repo>/requirements.txt
dbutils.library.restartPython()
```

4. (Opcional, AFPs Selenium) Init script aplicado:
`scripts/databricks_init_selenium.sh`

## 2) Validacion rapida (sin extract)

Ejecutar notebook:

- `/notebooks/modelo_cotizaciones_afp_validate.py`

Widgets recomendados:
- `run_extract = false`
- `seed_doc_idn = 166088887`
- `seed_link = https://w3.provida.cl/validador/descarga.ashx?Id=245756274-188906699`
- `seed_periodo_produccion = 2024-12-01`
- `seed_fecha_ingreso = 2026-03-04`
- `table_provider = delta`
- `target_table = <catalog>.<schema>.afp_certificados_output_tmp`
- `storage_base_path = dbfs:/tmp/modelo_cotizaciones_afp` (para prueba rapida)
  o `/Volumes/<catalog>/<schema>/<volume>` (si el Volume ya existe)

## 3) Criterios de aprobacion (OK)

En el JSON final deben cumplirse:

- `output_csv_exists = true`
- `original_pdf_exists = true`
- `validated_pdf_exists = true`
- `res_afp = "ok"`
- `es_dif = "False"`
- `target_table_count >= 1`

Y las rutas deben verse en formato Bronze:

- `.../bronze/afp_processing/year=YYYY/month=MM/day=DD/tipo_documento=original/<doc_idn>.pdf`
- `.../bronze/afp_processing/year=YYYY/month=MM/day=DD/tipo_documento=validacion/<doc_idn>.pdf`

## 4) Validacion completa (con extract)

1. Crear/asegurar tabla target existente.
2. En notebook de validacion:
   - `run_extract = true`
   - `source_table = <tabla_entrada_real>`
   - `target_table = <tabla_salida_real>`
3. Ejecutar notebook.
4. Verificar `target_table_count` y registros en `display(...)`.
