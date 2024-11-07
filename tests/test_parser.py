from mydb import parser, syntax


def test_parser() -> None:
    parser.SIMPLE_SQL.run_tests(
        """\

        # multiple tables
        SELECT * from XYZZY, ABC

        # dotted table name
        select * from SYS.XYZZY

        Select A from Sys.dual

        Select A,B,C from Sys.dual

        Select A, B, C from Sys.dual, Table2

        # FAIL - invalid SELECT keyword
        Xelect A, B, C from Sys.dual

        # FAIL - invalid FROM keyword
        Select A, B, C frox Sys.dual

        # FAIL - incomplete statement
        Select

        # FAIL - incomplete statement
        Select * from

        # FAIL - invalid column
        Select &&& frox Sys.dual

        # where clause
        Select A from Sys.dual where a in ('RED','GREEN','BLUE')

        # compound where clause
        Select A from Sys.dual where a in ('RED','GREEN','BLUE') and b in (10,20,30)

        # where clause with comparison operator
        Select A,b from table1,table2 where table1.id eq table2.id
    
        # simple insert
        Insert Into foo Values
        """
    )


def test_parse_function_simple_select() -> None:
    assert parser.parse("SELECT * FROM foo") == syntax.SelectStatement(
        columns=["*"], tables=["FOO"]
    )


def test_parse_function_simple_insert() -> None:
    assert parser.parse("INSERT INTO foo VALUES") == syntax.InsertStatement(table="FOO")
