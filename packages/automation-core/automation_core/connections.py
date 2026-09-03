from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import hvac
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


@dataclass(frozen=True)
class Connection:
    host: str = ""
    port: int | None = None
    login: str = ""
    password: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def parse_connection(payload: dict[str, Any]) -> Connection:
    if uri := payload.get("uri"):
        parsed = urlsplit(str(uri))
        query = {
            key: values[-1] if len(values) == 1 else values
            for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
        }
        encoded_extra = query.pop("__extra__", None)
        if encoded_extra:
            decoded_extra = json.loads(str(encoded_extra))
            if not isinstance(decoded_extra, dict):
                raise ValueError("connection __extra__ must be a JSON object")
            query.update(decoded_extra)
        if parsed.path and parsed.path != "/":
            query.setdefault("dbname", unquote(parsed.path.removeprefix("/")))
        return Connection(
            host=parsed.hostname or "",
            port=parsed.port,
            login=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            extra=query,
        )

    raw_extra = payload.get("extra") or {}
    if isinstance(raw_extra, str):
        raw_extra = json.loads(raw_extra)
    if not isinstance(raw_extra, dict):
        raise ValueError("connection extra must be a JSON object")
    raw_port = payload.get("port")
    return Connection(
        host=str(payload.get("host") or ""),
        port=int(raw_port) if raw_port not in (None, "") else None,
        login=str(payload.get("login") or ""),
        password=str(payload.get("password") or ""),
        extra=raw_extra,
    )


class VaultConnections:
    def __init__(self) -> None:
        address = os.environ["VAULT_ADDR"]
        ca_cert = os.environ["VAULT_CACERT"]
        cert = os.getenv("VAULT_CLIENT_CERT", "/run/secrets/fullchain")
        key = os.getenv("VAULT_CLIENT_KEY", "/run/secrets/key")
        session = Session()
        session.verify = ca_cert
        session.cert = (cert, key)
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=0.1,
                status_forcelist=[412, 500, 502, 503],
                raise_on_status=False,
            )
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        self.client = hvac.Client(url=address, session=session)
        self.client.auth.cert.login(
            name=os.getenv("VAULT_CERT_ROLE", ""),
            cert_pem=cert,
            key_pem=key,
            mount_point="cert",
        )
        if not self.client.is_authenticated():
            raise RuntimeError("Vault certificate authentication failed")

    def get(self, connection_id: str) -> Connection:
        response = self.client.secrets.kv.v2.read_secret_version(
            path=f"airflow/connections/{connection_id}", mount_point="kv"
        )
        return parse_connection(response["data"]["data"])

    def preflight(self) -> None:
        for connection_id in (
            "data",
            "djangodev",
            "stva",
            "telegram_default",
            "smtp_default",
            "loki",
            "openai_compatible",
            "operations_analyst_openrouter",
            "greynoise",
            "abuseipdb",
            "tavily",
        ):
            self.get(connection_id)
