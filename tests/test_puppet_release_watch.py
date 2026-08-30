from __future__ import annotations

from automation.pipelines.puppet_release_watch import (
    INDEX_URL,
    PACKAGE_NAMES,
    package_is_listed,
    render_telegram_message,
)


def test_package_is_listed_detects_the_release() -> None:
    package_name = PACKAGE_NAMES[0]
    assert package_is_listed(
        f"<a href='{package_name}'>{package_name}</a>", package_name
    )
    assert package_is_listed(
        "<a href='puppet8-release-noble.deb'>puppet8-release-noble.deb</a>",
        package_name="puppet8-release-noble.deb",
    )
    assert not package_is_listed("<html><body>no match</body></html>", package_name)


def test_package_names_include_puppet_8_and_9() -> None:
    assert PACKAGE_NAMES == (
        "puppet8-release-resolute.deb",
        "puppet9-release-resolute.deb",
    )


def test_render_telegram_message_mentions_the_found_package() -> None:
    package_name = "puppet9-release-resolute.deb"
    assert render_telegram_message(package_name) == (
        f"{package_name} is available: {INDEX_URL}"
    )


def test_render_telegram_message_mentions_the_index() -> None:
    package_name = PACKAGE_NAMES[0]
    assert (
        render_telegram_message(package_name)
        == f"{package_name} is available: {INDEX_URL}"
    )
