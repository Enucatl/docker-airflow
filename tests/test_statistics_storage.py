from datetime import UTC, datetime

from podcast_statistics.storage import insert_records, source_log_id


class Cursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[object]]] = []
        self.execute_calls: list[tuple[str, object]] = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def executemany(self, sql: str, rows: list[object]) -> None:
        self.executemany_calls.append((sql, rows))

    def execute(self, sql: str, params: object) -> None:
        self.execute_calls.append((sql, params))


class Transaction:
    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class Database:
    def __init__(self) -> None:
        self.cursor_value = Cursor()
        self.transaction_value = Transaction()

    def transaction(self) -> Transaction:
        return self.transaction_value

    def cursor(self) -> Cursor:
        return self.cursor_value


def test_source_log_id_is_deterministic_and_stream_sensitive() -> None:
    first = source_log_id("100", {"service_name": "caddy"}, "line")
    assert first == source_log_id("100", {"service_name": "caddy"}, "line")
    assert first != source_log_id("101", {"service_name": "caddy"}, "line")


def test_insert_records_inserts_and_advances_watermark_in_one_transaction() -> None:
    database = Database()
    insert_records(
        database,
        [{"source_log_id": "one"}],
        watermark=datetime(2026, 9, 3, tzinfo=UTC),
    )
    assert len(database.cursor_value.executemany_calls) == 1
    assert len(database.cursor_value.execute_calls) == 1
