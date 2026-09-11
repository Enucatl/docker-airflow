from email import policy
from email.parser import BytesParser

from automation_core.clients import send_email


class _Connection:
    host = "smtp.example.test"
    port = 587
    login = "user"
    password = "password"


def test_send_email_builds_html_alternative_and_text_attachment(monkeypatch):
    sent = []

    class SMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def starttls(self):
            pass

        def login(self, *_args):
            pass

        def send_message(self, message):
            sent.append(message.as_bytes())

    monkeypatch.setattr("automation_core.clients.smtplib.SMTP", SMTP)
    send_email(
        _Connection(),
        sender="sender@example.test",
        recipient="recipient@example.test",
        subject="subject",
        body="plain",
        html_body="<p>structured</p>",
        text_attachments={"playbook.txt": "task"},
    )

    message = BytesParser(policy=policy.default).parsebytes(sent[0])
    assert message.get_body("plain").get_content() == "plain\n"
    assert message.get_body("html").get_content() == "<p>structured</p>\n"
    attachment = next(part for part in message.iter_attachments())
    assert attachment.get_filename() == "playbook.txt"
    assert attachment.get_content() == "task\n"
