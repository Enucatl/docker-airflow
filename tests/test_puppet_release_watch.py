from __future__ import annotations

from puppet_release_watch import (
    INDEX_URL,
    PACKAGE_NAME,
    dag,
    package_is_listed,
    render_telegram_message,
)


def test_package_is_listed_detects_the_release() -> None:
    assert package_is_listed(f"<a href='{PACKAGE_NAME}'>{PACKAGE_NAME}</a>")
    assert package_is_listed(
        "<a href='puppet8-release-noble.deb'>puppet8-release-noble.deb</a>",
        package_name="puppet8-release-noble.deb",
    )
    assert not package_is_listed("<html><body>no match</body></html>")


def test_render_telegram_message_mentions_the_index() -> None:
    assert render_telegram_message() == f"{PACKAGE_NAME} is available: {INDEX_URL}"


def test_puppet_release_watch_dag_wires_telegram_alert() -> None:
    check_release = dag.get_task("check_release")
    send_telegram = dag.get_task("send_telegram")

    assert set(dag.task_ids) == {"check_release", "send_telegram"}
    assert check_release.task_id == "check_release"
    assert send_telegram.telegram_conn_id == "telegram_default"
    assert send_telegram.upstream_task_ids == {"check_release"}
