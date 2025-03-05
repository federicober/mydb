import logging
import pathlib
import urllib.parse
from collections.abc import Iterator, Mapping, Sequence
from io import IOBase
from typing import Any, Literal, Self

import polars
import pydantic

logger = logging.getLogger(__name__)


class MyDBError(Exception):
    pass


class ColumnDoesNotExist(MyDBError):
    pass


type DataTypes = Literal["STRING", "INTEGER"]

_DATATYPE_LENGHTS: dict[DataTypes, int] = {"STRING": 16, "INTEGER": 8}
ALL_DATATYPES: Sequence[DataTypes] = tuple(_DATATYPE_LENGHTS.keys())
DATATYPE_TO_TYPE: dict[DataTypes, type[Any]] = {"STRING": str, "INTEGER": int}
MAX_TABLE_NAME_LENGTH = 255


class ColumnInfo(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)
    name: str
    datatype: DataTypes


class TableInfo(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)
    columns: list[ColumnInfo]

    # TODO Cache these properties, probably using a materialized view
    @property
    def column_offsets(self) -> dict[str, int]:
        offset = 0
        offsets: dict[str, int] = {}
        for col in self.columns:
            offsets[col.name] = offset
            offset += _DATATYPE_LENGHTS[col.datatype]
        return offsets

    @property
    def columns_dict(self) -> dict[str, ColumnInfo]:
        return {col.name: col for col in self.columns}

    @property
    def row_length(self) -> int:
        return sum(_DATATYPE_LENGHTS[col.datatype] for col in self.columns)


def _serialize_header(table_info: TableInfo) -> bytes:
    as_string = table_info.model_dump_json(indent=None)
    if "\n" in as_string:
        raise RuntimeError("Found newline character in header")
    return as_string.encode("utf-8") + b"\n"


def _deserialize_header(serialized_header: bytes) -> tuple[TableInfo, int]:
    assert serialized_header.endswith(b"\n")
    return TableInfo.model_validate_json(serialized_header[:-1]), len(serialized_header)


class Table:
    def __init__(self, location: pathlib.Path) -> None:
        self._location = location
        self.name = urllib.parse.unquote_plus(self._location.name)
        with location.open("rb") as file:
            self.info, self._data_start = _deserialize_header(file.readline(None))

    @classmethod
    def create(cls, name: str, location: pathlib.Path, info: TableInfo) -> Self:
        if len(name) > MAX_TABLE_NAME_LENGTH:
            raise ValueError(
                f"Table name is too long: {len(name)} > {MAX_TABLE_NAME_LENGTH}"
            )
        table_location = location / urllib.parse.quote_plus(name)
        if table_location.exists():
            raise FileExistsError()

        table_location.write_bytes(_serialize_header(info))

        return cls(location=table_location)

    def insert(self, *inserted: tuple[str, Any]) -> None:
        inserted_column_names = {name for name, _ in inserted}
        table_columns = {col.name for col in self.info.columns}
        if extra_columns := inserted_column_names - table_columns:
            raise ColumnDoesNotExist(",".join(extra_columns))
        if missing_columns := table_columns - inserted_column_names:
            raise NotImplementedError(
                f"Missing columns: {missing_columns}. Default values is not implemented yet"
            )
        with self._location.open("ab") as fd:
            serialized = _serialize_row(
                self.info, {name: value for name, value in inserted}
            )
            fd.write(serialized)

    @property
    def length(self) -> int:
        total_data = self._location.stat().st_size - self._data_start
        return total_data // self.info.row_length

    def query(self, columns: Sequence[str]) -> polars.DataFrame:
        table_columns = {col.name for col in self.info.columns}
        if extra_columns := set(columns) - table_columns:
            raise ColumnDoesNotExist(",".join(extra_columns))
        rows: list[dict[str, Any]] = []
        with self._location.open("rb") as file:
            file.seek(self._data_start)
            for row_num in range(self.length):
                rows.append(
                    dict(  # pyright: ignore[reportCallIssue,reportArgumentType]
                        zip(
                            columns,
                            _deserialize_row(
                                self.info,
                                file,
                                self._data_start + row_num * self.info.row_length,
                                columns,
                            ),
                            strict=True,
                        )
                    )
                )
        assert len(rows) == self.length
        return polars.DataFrame(rows)


def left_pad(value: bytes, length: int) -> bytes:
    return b"\x00" * (length - len(value)) + value


def _serialize_value(datatype: DataTypes, value: Any) -> bytes:
    match (datatype, value):
        case ("STRING", str(value)):
            as_bytes = left_pad(value.encode("utf-8"), _DATATYPE_LENGHTS["STRING"])
        case ("INTEGER", int(value)):
            as_bytes = value.to_bytes(_DATATYPE_LENGHTS["INTEGER"], "big", signed=True)
        case _:
            raise ValueError(f"Cannot serialize {value!r} as {datatype}")
    assert len(as_bytes) == _DATATYPE_LENGHTS[datatype]
    return as_bytes


def _deserialize_value(datatype: DataTypes, fd: IOBase) -> Any:
    value_as_bytes = fd.read(_DATATYPE_LENGHTS[datatype])
    if datatype == "STRING":
        return value_as_bytes.lstrip(b"\x00").decode("utf-8")
    if datatype == "INTEGER":
        return int.from_bytes(value_as_bytes, "big", signed=True)
    raise ValueError(f"Unknown datatype {datatype}")


def _serialize_row(table_info: TableInfo, values: Mapping[str, Any]) -> bytes:
    serialized = b"".join(
        _serialize_value(col.datatype, values[col.name]) for col in table_info.columns
    )
    assert len(serialized) == table_info.row_length
    return serialized


def _deserialize_row(
    table_info: TableInfo,
    file_data: IOBase,
    row_start: int,
    columns: Sequence[str],
) -> Iterator[Any]:
    file_data.seek(row_start)

    offsets = table_info.column_offsets
    as_dict = table_info.columns_dict

    for col in columns:
        file_data.seek(row_start + offsets[col])
        yield _deserialize_value(as_dict[col].datatype, file_data)
