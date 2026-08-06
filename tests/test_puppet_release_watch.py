from __future__ import annotations

from automation.pipelines.puppet_release_watch import (
    INDEX_URL,
    PACKAGE_NAME,
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
