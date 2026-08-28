"""Generic sqlite-backed paging helpers shared by storage mixins."""

from __future__ import annotations

from typing import cast

from google.protobuf.message import Message

from tgsrl_runtime.storage.store_contracts import SQLiteStoreHelpers
from tgsrl_runtime.storage.types import Page


class SQLitePagingMixin:
    """Shared cursor paging helper for message tables."""

    def _page_messages[T: Message](
        self,
        *,
        table: str,
        id_column: str,
        seq_column: str,
        message_type: type[T],
        limit: int,
        after_cursor: str,
        string_sequence: bool = False,
    ) -> Page[T]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        sequence_number = 0
        sequence_text = ""
        item_id = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            if string_sequence:
                sequence_text = str(decoded[0])
            else:
                sequence_number = int(str(decoded[0]))
            item_id = str(decoded[1])
        if string_sequence:
            predicate = f"({seq_column} > ? OR ({seq_column} = ? AND {id_column} > ?))"
            params: tuple[object, ...] = (
                sequence_text,
                sequence_text,
                item_id,
                limit + 1,
            )
        else:
            predicate = f"({seq_column} > ? OR ({seq_column} = ? AND {id_column} > ?))"
            params = (
                sequence_number,
                sequence_number,
                item_id,
                limit + 1,
            )
        rows = list(
            store._connection.execute(
                f"""
                SELECT {seq_column} AS sequence, {id_column} AS item_id, payload
                FROM {table}
                WHERE {predicate}
                ORDER BY {seq_column} ASC, {id_column} ASC
                LIMIT ?
                """,
                params,
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["sequence"], tail["item_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(message_type, row["payload"], f"{table} {row['item_id']}")
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)
