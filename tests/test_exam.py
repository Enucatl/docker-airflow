from __future__ import annotations

from exam import dag, render_appointments_for_telegram


def test_render_appointments_for_telegram_sorts_and_escapes() -> None:
    appointments = [
        {
            "location": "Zurich",
            "date": "02.09.2026",
            "time": "11:15",
        },
        {
            "location": "A&B",
            "date": "01.09.2026",
            "time": "09:00",
        },
    ]

    message = render_appointments_for_telegram(appointments)

    assert (
        message == "<b>Available exam dates</b>\n\n"
        "- <b>A&amp;B</b>: 01.09.2026 at 09:00\n"
        "- <b>Zurich</b>: 02.09.2026 at 11:15"
    )


def test_exam_dag_exposes_telegram_notification_task() -> None:
    format_appointments = dag.get_task("format_appointments_for_telegram")
    send_exam_dates = dag.get_task("send_exam_dates")

    assert set(dag.task_ids) >= {
        "extract_appointments",
        "format_appointments_for_telegram",
        "send_exam_dates",
    }
    assert format_appointments._pre_execute_hook is not None
    assert send_exam_dates.telegram_conn_id == "telegram_default"
    assert format_appointments.upstream_task_ids >= {"update_database"}
    assert send_exam_dates.upstream_task_ids == {"format_appointments_for_telegram"}
