from __future__ import annotations

import os
from typing import Optional


def get_secret(
    key: str,
    scope: Optional[str] = None,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Reads secrets from Databricks Secret Scope first, then env vars.
    """
    env_key = key.upper().replace("-", "_")
    env_value = os.getenv(env_key)
    if env_value:
        return env_value

    dbutils_obj = globals().get("dbutils")
    if dbutils_obj and scope:
        try:
            return dbutils_obj.secrets.get(scope=scope, key=key)
        except Exception:
            pass

    return default


def configure_adls_oauth(
    spark,
    storage_account: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> None:
    """
    Optional helper when direct ABFSS access is needed outside UC Volumes.
    """
    fqdn = f"{storage_account}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{fqdn}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{fqdn}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{fqdn}",
        client_id,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{fqdn}",
        client_secret,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{fqdn}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )
