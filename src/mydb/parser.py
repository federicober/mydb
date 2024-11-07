import pyparsing
from pyparsing import pyparsing_common as ppc

from mydb import syntax

pyparsing.ParserElement.enable_packrat()

# define SQL tokens

SELECT, FROM, WHERE, AND, OR, IN, IS, NOT, NULL, INSERT, INTO, VALUES = map(
    pyparsing.CaselessKeyword,
    "select from where and or in is not null insert into values".split(),
)
NOT_NULL = NOT + NULL

IDENTIFIER = pyparsing.Word(
    pyparsing.alphas, pyparsing.alphanums + "_$"
).set_results_name("identifier")

COLUMN_NAME = (
    pyparsing.DelimitedList(IDENTIFIER, ".", combine=True)
    .set_results_name("column name")
    .add_parse_action(ppc.upcase_tokens)
)

COLUMN_NAME_LIST = pyparsing.Group(
    pyparsing.DelimitedList(COLUMN_NAME).set_results_name("column_list")
)
TABLE_NAME = (
    pyparsing.DelimitedList(IDENTIFIER, ".", combine=True)
    .set_results_name("table name")
    .add_parse_action(ppc.upcase_tokens)
)
TABLE_NAME_LIST = pyparsing.Group(
    pyparsing.DelimitedList(TABLE_NAME).set_results_name("table_list")
)

BIN_OP = pyparsing.one_of(
    "= != < > >= <= eq ne lt le gt ge", caseless=True
).set_results_name("binop")
REAL_NUM = ppc.real().set_results_name("real number")
INT_NUM = ppc.signed_integer()

COLUMN_RVAL = (
    REAL_NUM | INT_NUM | pyparsing.quoted_string | COLUMN_NAME
).set_results_name(
    "column_rvalue"
)  # need to add support for alg expressions

SELECT_STMT = pyparsing.Forward()

WHERE_CONDITION = pyparsing.Group(
    (COLUMN_NAME + BIN_OP + COLUMN_RVAL)
    | (
        COLUMN_NAME
        + IN
        + pyparsing.Group(
            "("
            + pyparsing.DelimitedList(COLUMN_RVAL).set_results_name("in_values_list")
            + ")"
        )
    )
    | (COLUMN_NAME + IN + pyparsing.Group("(" + SELECT_STMT + ")"))
    | (COLUMN_NAME + IS + (NULL | NOT_NULL))
).set_results_name("where_condition")

WHERE_EXPRESSION = pyparsing.infix_notation(
    WHERE_CONDITION,
    [
        (NOT, 1, pyparsing.OpAssoc.RIGHT),
        (AND, 2, pyparsing.OpAssoc.LEFT),
        (OR, 2, pyparsing.OpAssoc.LEFT),
    ],
).set_results_name("where_expression")


# define the grammar
SELECT_STMT <<= (
    SELECT
    + ("*" | COLUMN_NAME_LIST)("columns")
    + FROM
    + TABLE_NAME_LIST("tables")
    + pyparsing.Opt(pyparsing.Group(WHERE + WHERE_EXPRESSION), "")("where")
).set_results_name("select_statement")

INSERT_STMT = pyparsing.Forward()
INSERT_STMT <<= (INSERT + INTO + TABLE_NAME + VALUES).set_results_name(
    "insert_statement"
)
SIMPLE_SQL = SELECT_STMT | INSERT_STMT

# define Oracle comment format, and ignore them
ORACLE_SQL_COMMENT = "--" + pyparsing.rest_of_line
SIMPLE_SQL.ignore(ORACLE_SQL_COMMENT)


def parse(text: str) -> syntax.Statement:
    results = SIMPLE_SQL.parse_string(text)
    match results.get_name():
        case "select_statement":
            return syntax.SelectStatement(
                columns=results["columns"].as_list(), tables=results["tables"].as_list()
            )
        case "insert_statement":
            return syntax.InsertStatement(table=results["table name"])
        case _:  # pragma: nocover # should never happen because pyparsing should error
            raise RuntimeError
