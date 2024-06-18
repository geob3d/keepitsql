import re
from typing import (
    List,
    Optional,
    Tuple,
    Union,
)

from sqlalchemy import (
    create_engine,
    inspect,
    text,
)
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)

from keepitsql.sql_models.information_schema import (
    bigquery_query,
    db2_query,
    hana_query,
    mssql_query,
    mysql_query,
    oracle_query,
    postgresql_query,
    redshift_query,
    snowflake_query,
    sqlite_query,
    teradata_query,
)


def get_auto_increment_from_ddl(sql_ddl: str) -> Optional[str]:
    """
    Extracts the auto-increment column name from a DDL statement for SQLite.

    :param sql_ddl: The SQL DDL statement.
    :return: The name of the auto-increment column, if found; otherwise, None.
    """
    # Regular expression to match the autoincrement column definition (case-insensitive)
    pattern = r"(?P<col_name>\w+) INTEGER PRIMARY KEY AUTOINCREMENT"
    match = re.search(pattern, sql_ddl, re.IGNORECASE)  # Ignore case for 'AUTOINCREMENT'

    if match:
        autoincrement_col = match.group('col_name')
        return autoincrement_col
    else:
        return None


def get_dialect_query(dialect: str) -> str:
    """
    Retrieves the appropriate query for a given database dialect.

    :param dialect: The database dialect.
    :return: The query string for the specified dialect.
    :raises NotImplementedError: If the dialect is not supported.
    """
    queries = {
        'postgresql': postgresql_query,
        'mysql': mysql_query,
        'sqlite': sqlite_query,
        'mssql': mssql_query,
        'oracle': oracle_query,
        'db2': db2_query,
        'teradata': teradata_query,
        'hana': hana_query,
        'snowflake': snowflake_query,
        'redshift': redshift_query,
        'bigquery': bigquery_query,
    }
    if dialect not in queries:
        raise NotImplementedError(f"Dialect '{dialect}' is not supported by this script.")
    return queries[dialect]


def is_auto_increment(
    dialect: str, column_default: Optional[str], is_identity: Optional[bool], is_computed: Optional[bool]
) -> bool:
    """
    Determines if a column is auto-increment based on the database dialect and column properties.

    :param dialect: The database dialect.
    :param column_default: The default value of the column.
    :param is_identity: Indicates if the column is an identity column.
    :param is_computed: Indicates if the column is computed.
    :return: True if the column is auto-increment, False otherwise.
    """
    if dialect == 'postgresql' and column_default and 'nextval' in str(column_default):
        return True
    elif dialect == 'mysql' and column_default is not None:
        return True
    elif dialect == 'mssql' and is_identity:
        return True
    elif dialect == 'oracle' and column_default and 'SEQ' in column_default.upper():
        return True
    elif dialect == 'db2' and is_identity == 'Y':
        return True
    elif dialect == 'teradata' and column_default:
        return True
    elif dialect == 'hana' and column_default:
        return True
    elif dialect == 'snowflake':
        # Needs specific handling
        return False
    elif dialect == 'redshift' and column_default and 'nextval' in str(column_default):
        return True
    elif dialect == 'bigquery' and column_default:
        return True
    return False


def get_table_column_info(
    db_resource: Union[str, Session], table_name: str, schema_name: Optional[str] = None
) -> Tuple[List[str], List[str]]:
    auto_increment_columns: List[str] = []
    primary_key_columns: List[str] = []

    """
    Purpose: 
        Provides an overview of what the function does, which is to retrieve information about the columns of a specified table, identifying auto-increment and primary key columns.
    Parameters:
        db_resource: Describes that it can be either a database connection string or an existing SQLAlchemy Session object.
        table_name: Specifies the name of the table to inspect.
        schema_name: Indicates that this is optional and is the schema name where the table resides.
    Return: 
        Describes the return value, which is a tuple containing two lists: one for auto-increment columns and one for primary key columns.
    
    """

    # Check if db_resource is a connection string or an existing session
    if isinstance(db_resource, str):
        engine = create_engine(db_resource)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        session_provided = False
    else:
        session = db_resource
        session_provided = True

    try:
        # Determine the database dialect
        dialect = session.bind.dialect.name

        # Get the appropriate query for the dialect
        query = get_dialect_query(dialect)

        query = text(query)
        if dialect == "sqlite":
            result = session.execute(query, {'table_name': table_name})
        else:
            result = session.execute(query, {'table_name': table_name, 'schema_name': schema_name})

        columns = result.keys()
        rows = result.fetchall()

        data_list = [dict(zip(columns, row)) for row in rows]

        for row in data_list:
            column_name = row.get('name', None)
            column_default = row.get('column_default')
            is_identity = row.get('is_identity')
            is_computed = row.get('is_computed')

            # Determine if the column is auto-incrementing
            if is_auto_increment(dialect, column_default, is_identity, is_computed):
                auto_increment_columns.append(column_name)
            elif dialect == 'sqlite':
                column_name = get_auto_increment_from_ddl(row.get('is_identity'))
                auto_increment_columns.append(column_name)

        # Use SQLAlchemy inspect to get primary key columns
        inspector = inspect(session.bind)
        pk_columns = inspector.get_pk_constraint(table_name, schema=schema_name)['constrained_columns']
        primary_key_columns.extend(pk_columns)

    finally:
        if not session_provided:
            session.close()

    return auto_increment_columns, primary_key_columns


# # Example usage:
# connection_string = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"
# table_name = 'human'
# get_table_column_info(connection_string, table_name)
