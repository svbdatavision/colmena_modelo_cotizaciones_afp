from __future__ import annotations

from dataclasses import dataclass, field
import os


def _default_storage_base_path() -> str:
    databricks_runtime = os.getenv("DATABRICKS_RUNTIME_VERSION")
    if databricks_runtime:
        # Safe default for execution without UC Volume dependency.
        return os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp")
    return os.getenv("AFP_STORAGE_BASE_PATH", "/workspace")


def normalize_local_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path.removeprefix("dbfs:/")
    return path


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


@dataclass
class PipelineConfig:
    storage_base_path: str = field(default_factory=_default_storage_base_path)
    source_table: str = field(
        default_factory=lambda: os.getenv("AFP_SOURCE_TABLE", "opx.p_ddv_opx.afp_certificados")
    )
    target_table: str = field(
        default_factory=lambda: os.getenv("AFP_TARGET_TABLE", "opx.p_ddv_opx.afp_certificados_output")
    )
    table_provider: str = field(default_factory=lambda: os.getenv("AFP_TABLE_PROVIDER", "delta"))
    use_partitioned_pdf_storage: bool = field(
        default_factory=lambda: _env_bool("AFP_USE_PARTITIONED_PDF_STORAGE", True)
    )
    bronze_pdf_prefix: str = field(
        default_factory=lambda: os.getenv("AFP_BRONZE_PDF_PREFIX", "bronze/afp_processing")
    )
    processing_date: str = field(default_factory=lambda: os.getenv("AFP_PROCESSING_DATE", ""))
    extract_days: int = 30
    extract_limit: int = 240
    request_timeout_seconds: int = 30
    request_retries: int = 2
    sleep_seconds_between_downloads: float = 1.0
    chromedriver_path: str = field(
        default_factory=lambda: os.getenv(
            "AFP_CHROMEDRIVER_PATH",
            "/databricks/driver/chromedriver",
        )
    )
    selenium_headless: bool = True

    @property
    def base_path(self) -> str:
        return normalize_local_path(self.storage_base_path)

    @property
    def input_dir(self) -> str:
        return os.path.join(self.base_path, "input")

    @property
    def output_dir(self) -> str:
        return os.path.join(self.base_path, "output")

    @property
    def pdfs_dir(self) -> str:
        return os.path.join(self.base_path, "pdfs")

    @property
    def temp_dir(self) -> str:
        return os.path.join(self.base_path, "tmp")

    @property
    def logs_dir(self) -> str:
        return os.path.join(self.base_path, "logs")

    @property
    def input_csv_path(self) -> str:
        return os.path.join(self.input_dir, "certificados.csv")

    @property
    def output_csv_path(self) -> str:
        return os.path.join(self.output_dir, "certificados.csv")

    @property
    def log_file_path(self) -> str:
        return os.path.join(self.logs_dir, "errors.log")

    def ensure_directories(self) -> None:
        if self.base_path.startswith("/Volumes/") and not os.path.exists(self.base_path):
            raise FileNotFoundError(
                f"storage_base_path does not exist: {self.base_path}. "
                "Use an existing UC Volume path (e.g. /Volumes/<catalog>/<schema>/<volume>)."
            )

        for path in [
            self.input_dir,
            self.output_dir,
            self.pdfs_dir,
            self.temp_dir,
            self.logs_dir,
            os.path.join(self.base_path, self.bronze_pdf_prefix),
        ]:
            os.makedirs(path, exist_ok=True)
