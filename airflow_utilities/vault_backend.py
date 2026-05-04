from __future__ import annotations

from functools import cached_property
from typing import Any

import hvac
from hvac.exceptions import VaultError
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class CertVaultClient:
    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        from airflow.providers.hashicorp._internal_client.vault_client import (
            _VaultClient,
        )

        class _CertVaultClient(_VaultClient):
            def __init__(
                self,
                *,
                cert_role: str = "",
                cert: list[str] | tuple[str, str] | None = None,
                **client_kwargs: Any,
            ) -> None:
                if cert is None or len(cert) != 2:
                    raise VaultError(
                        "The 'cert' authentication type requires cert and key paths"
                    )

                super().__init__(
                    auth_type="token", token="unused", cert=tuple(cert), **client_kwargs
                )
                self.auth_type = "cert"
                self.cert_role = cert_role

            @cached_property
            def _client(self) -> hvac.Client:
                if "session" not in self.kwargs:
                    adapter = HTTPAdapter(
                        max_retries=Retry(
                            total=3,
                            backoff_factor=0.1,
                            status_forcelist=[412, 500, 502, 503],
                            raise_on_status=False,
                        )
                    )
                    session = Session()
                    session.mount("http://", adapter)
                    session.mount("https://", adapter)
                    session.verify = self.kwargs.get("verify", session.verify)
                    session.cert = self.kwargs.get("cert", session.cert)
                    session.proxies = self.kwargs.get("proxies", session.proxies)
                    self.kwargs["session"] = session

                client = hvac.Client(url=self.url, **self.kwargs)
                self._auth_cert(client)
                if client.is_authenticated():
                    return client
                raise VaultError("Vault certificate authentication failed")

            def _auth_cert(self, client: hvac.Client) -> None:
                cert, key = self.kwargs["cert"]
                client.auth.cert.login(
                    name=self.cert_role,
                    cert_pem=cert,
                    key_pem=key,
                    mount_point=self.auth_mount_point or "cert",
                )

        return _CertVaultClient(*args, **kwargs)


class CertVaultBackend:
    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        from airflow.providers.hashicorp._internal_client.vault_client import (
            DEFAULT_KV_ENGINE_VERSION,
        )
        from airflow.providers.hashicorp.secrets.vault import VaultBackend

        class _CertVaultBackend(VaultBackend):
            def __init__(
                self,
                connections_path: str | None = "connections",
                variables_path: str | None = "variables",
                config_path: str | None = "config",
                url: str | None = None,
                auth_mount_point: str | None = "cert",
                mount_point: str | None = "secret",
                kv_engine_version: int = DEFAULT_KV_ENGINE_VERSION,
                cert_role: str = "",
                cert: list[str] | tuple[str, str] | None = None,
                **backend_kwargs: Any,
            ) -> None:
                super(VaultBackend, self).__init__()
                self.connections_path = (
                    connections_path.rstrip("/")
                    if connections_path is not None
                    else None
                )
                self.variables_path = (
                    variables_path.rstrip("/") if variables_path is not None else None
                )
                self.config_path = (
                    config_path.rstrip("/") if config_path is not None else None
                )
                self.mount_point = mount_point
                self.kv_engine_version = kv_engine_version
                self.vault_client = CertVaultClient(
                    url=url,
                    auth_mount_point=auth_mount_point,
                    mount_point=mount_point,
                    kv_engine_version=kv_engine_version,
                    cert_role=cert_role,
                    cert=cert,
                    **backend_kwargs,
                )

        return _CertVaultBackend(*args, **kwargs)
