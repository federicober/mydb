# pyright: reportPrivateUsage=false
import io
import pathlib
import string
import tempfile
from io import BytesIO
from typing import Any

import hypothesis
import polars
import pytest
from hypothesis import strategies as st

from mydb import storage

st_n_rows = st.integers(min_value=0, max_value=20)
MAX_INT = 2 ** (storage._DATATYPE_LENGHTS["INTEGER"] * 4 - 1) - 1

_DATATYPE_TO_STRATEGY: dict[storage.DataTypes, st.SearchStrategy[Any]] = {
    "STRING": st.text(
        alphabet=string.printable, max_size=storage._DATATYPE_LENGHTS["STRING"]
    ),
    "INTEGER": st.integers(min_value=-MAX_INT, max_value=MAX_INT),
}


type TableInfoAndRows = tuple[storage.TableInfo, list[dict[str, Any]]]


@st.composite
def st_table_info(draw: st.DrawFn) -> storage.TableInfo:
    columns = draw(
        st.lists(
            st.tuples(
                st.text(alphabet=string.ascii_letters + "_", min_size=1),
                st.sampled_from(storage.ALL_DATATYPES),
            ),
            min_size=1,
        )
    )
    hypothesis.assume(len(set(col[0] for col in columns)) == len(columns))
    return storage.TableInfo(
        columns=[
            storage.ColumnInfo(name=name, datatype=datatype)
            for name, datatype in columns
        ]
    )


@st.composite
def st_table_info_and_rows(
    draw: st.DrawFn,
) -> TableInfoAndRows:
    info = draw(st_table_info())
    n_rows = draw(st_n_rows)
    columns: dict[str, list[Any]] = {}
    for col in info.columns:
        columns[col.name] = draw(
            st.lists(
                _DATATYPE_TO_STRATEGY[col.datatype], min_size=n_rows, max_size=n_rows
            )
        )
    return info, [
        {name: values[i] for name, values in columns.items()} for i in range(n_rows)
    ]


def _create_test_table(location: pathlib.Path) -> storage.Table:
    table = storage.Table.create(
        name="my_test_table",
        location=location,
        info=storage.TableInfo(
            columns=[
                storage.ColumnInfo(name="name", datatype="STRING"),
                storage.ColumnInfo(name="age", datatype="INTEGER"),
            ]
        ),
    )

    return table


@hypothesis.given(n_rows=st_n_rows)
def test_len_works(n_rows: int) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        table = _create_test_table(pathlib.Path(tmpdir))
        for i in range(n_rows):
            table.insert(("name", f"Spam{i}"), ("age", i))
        hypothesis.note(table._location.read_bytes())
        assert table.length == n_rows


def test_table_insert_column_that_does_not_exist(tmp_path: pathlib.Path) -> None:
    table = _create_test_table(tmp_path)
    with pytest.raises(storage.ColumnDoesNotExist, match="foo"):
        table.insert(("name", "Spam"), ("foo", 18))


@hypothesis.given(table_info=st_table_info())
def test_table_roundtrip_table_creation_and_get_info(
    table_info: storage.TableInfo,
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        table = storage.Table.create(
            name="my_test_table", location=pathlib.Path(tmpdir), info=table_info
        )
    assert table_info == table.info


@hypothesis.given(info=st_table_info())
def test_table_roundtrip_serialize_deserialize_header(
    info: storage.TableInfo,
) -> None:
    info, _ = storage._deserialize_header(storage._serialize_header(info))
    assert info == info


@hypothesis.given(value=_DATATYPE_TO_STRATEGY["INTEGER"])
def test_roundtrip_serialize_deserialize_integer(value: int) -> None:
    assert value == storage._deserialize_value(
        "INTEGER", io.BytesIO(storage._serialize_value("INTEGER", value))
    )


@hypothesis.given(value=_DATATYPE_TO_STRATEGY["STRING"])
def test_roundtrip_serialize_deserialize_string(value: str) -> None:
    assert value == storage._deserialize_value(
        "STRING", io.BytesIO(storage._serialize_value("STRING", value))
    )


@hypothesis.given(table_info_and_rows=st_table_info_and_rows())
def test_table_roundtrip_serialize_deserialize_row(
    table_info_and_rows: TableInfoAndRows,
) -> None:
    table_info, rows = table_info_and_rows
    query_columns = [col.name for col in table_info.columns]
    hypothesis.assume(len(rows) >= 0)
    for row in rows:
        expected = [row[col] for col in query_columns]
        assert expected == list(
            storage._deserialize_row(
                table_info,
                BytesIO(storage._serialize_row(table_info, row)),
                row_start=0,
                columns=query_columns,
            )
        )


@hypothesis.given(
    table_info_and_rows=st_table_info_and_rows(),
    name=st.text(min_size=1, max_size=storage.MAX_TABLE_NAME_LENGTH),
    data=st.data(),
)
def test_roundtrip_insert_query_no_filter(
    table_info_and_rows: TableInfoAndRows, name: str, data: st.DataObject
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        table_info, rows = table_info_and_rows
        table = storage.Table.create(
            name=name, location=pathlib.Path(tmpdir), info=table_info
        )
        hypothesis.note(f"{table._location.stem=}")
        assert table.name == name
        for row in rows:
            table.insert(*((k, v) for k, v in row.items()))
        query_columns = data.draw(
            st.lists(
                st.sampled_from([col.name for col in table_info.columns]),
                min_size=1,
            )
        )
        as_df = table.query(query_columns)
        assert isinstance(as_df, polars.DataFrame)
        assert len(as_df) == len(rows)

        assert as_df.to_dicts() == [
            {col: row[col] for col in query_columns} for row in rows
        ]
