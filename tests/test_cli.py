from automation import cli


def test_unhandled_failure_returns_nonzero(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["automation", "main"])
    monkeypatch.setenv("AUTOMATION_PIPELINE", "exam")
    monkeypatch.setattr(
        cli, "VaultConnections", lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    assert cli.main() == 1
