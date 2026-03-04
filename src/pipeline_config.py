from __future__ import annotations

from dataclasses import dataclass, field
import os


def _default_storage_base_path() -> str:
    databricks_runtime = os.getenv("DATABRICKS_RUNTIME_VERSION")
    if databricks_runtime:
        # Production default: Unity Catalog Volume backed by ADLS Gen2.
        return os.getenv(
            "AFP_STORAGE_BASE_PATH",
            "/Volumes/main/default/modelo_cotizaciones_afp",
        )
    return os.getenv("AFP_STORAGE_BASE_PATH", "/workspace")


def normalize_local_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path.removeprefix("dbfs:/")
    return path


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
        for path in [
            self.input_dir,
            self.output_dir,
            self.pdfs_dir,
            self.temp_dir,
            self.logs_dir,
        ]:
            os.makedirs(path, exist_ok=True)
