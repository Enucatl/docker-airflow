from automation_core.connections import VaultConnections, parse_connection


def test_parse_legacy_vault_connection() -> None:
    connection = parse_connection(
        {
            "host": "db",
            "port": "5432",
            "login": "user",
            "password": "pass",
            "extra": '{"dbname":"output"}',
        }
    )

    assert connection.host == "db"
    assert connection.port == 5432
    assert connection.extra == {"dbname": "output"}


def test_vault_uses_certificate_session(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class Auth:
        class Cert:
            def login(self, **kwargs) -> None:
                captured["login"] = kwargs

        cert = Cert()

    class Client:
        auth = Auth()

        def is_authenticated(self) -> bool:
            return True

    def client(*, url, session):
        captured["url"] = url
        captured["session"] = session
        return Client()

    monkeypatch.setenv("VAULT_ADDR", "https://vault.example")
    monkeypatch.setenv("VAULT_CACERT", "/ca.pem")
    monkeypatch.setattr("automation_core.connections.hvac.Client", client)

    VaultConnections()

    assert captured["session"].verify == "/ca.pem"
    assert captured["session"].cert == (
        "/run/secrets/fullchain",
        "/run/secrets/key",
    )
    assert captured["login"]["mount_point"] == "cert"
