import dataclasses
from typing import TypeAlias


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class SelectStatement:
    columns: list[str]
    tables: list[str]


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class InsertStatement:
    table: str


Statement: TypeAlias = InsertStatement | SelectStatement
