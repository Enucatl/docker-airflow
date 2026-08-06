from automation_core.clients import sanitize_failure


def test_failure_message_is_sanitized_and_bounded() -> None:
    message = sanitize_failure(
        "exam<script>", ValueError("token=<secret>\n" + "x" * 600)
    )

    assert "<script>" not in message
    assert "&lt;script&gt;" in message
    assert "secret" not in message
    assert "ValueError" in message
