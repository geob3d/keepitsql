from sqlalchemy.sql import text

# Queries for different database systems
postgresql_query = """
    SELECT 
    column_name, 
    column_default, 
    is_nullable, 
    is_identity
    FROM information_schema.columns
    WHERE table_name = :table_name AND table_schema = COALESCE(:schema_name, 'public')
"""

mysql_query = """
    SELECT 
    column_name, 
    column_default, 
    is_nullable, 
    is_identity
    FROM information_schema.columns
    WHERE table_name = :table_name AND table_schema = COALESCE(:schema_name, database())
"""

sqlite_query = """
    SELECT 
        name AS table_name
        ,SQL AS is_identity
        ,*
    FROM sqlite_master
    WHERE 
        name = :table_name
        AND type = 'table'

"""

mssql_query = """
    SELECT 
        c.name AS column_name, 
        c.is_identity AS is_identity,
        c.is_computed AS is_computed
    FROM sys.columns c
    INNER JOIN sys.tables t ON t.object_id = c.object_id
    WHERE t.name = :table_name AND SCHEMA_NAME(t.schema_id) = COALESCE(:schema_name, SCHEMA_NAME())
"""

oracle_query = """
    SELECT column_name, data_default 
    FROM all_tab_columns
    WHERE table_name = :table_name AND owner = COALESCE(:schema_name, USER)
"""

db2_query = """
    SELECT colname AS column_name, 
           identity AS is_identity
    FROM syscat.columns 
    WHERE tabname = :table_name AND tabschema = COALESCE(:schema_name, CURRENT SCHEMA)
"""

teradata_query = """
    SELECT columnname AS column_name, 
           defaultvalue AS column_default
    FROM dbc.columns 
    WHERE tablename = :table_name AND databasename = COALESCE(:schema_name, DATABASE)
"""

hana_query = """
    SELECT column_name, default_value 
    FROM tables 
    WHERE table_name = :table_name AND schema_name = COALESCE(:schema_name, CURRENT_SCHEMA)
"""

snowflake_query = """
    SHOW COLUMNS IN TABLE :schema_name.:table_name
"""

redshift_query = """
    SELECT column_name, column_default 
    FROM information_schema.columns 
    WHERE table_name = :table_name AND table_schema = COALESCE(:schema_name, 'public')
"""

bigquery_query = """
    SELECT column_name, column_default 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = :table_name AND table_schema = COALESCE(:schema_name, 'default')
"""
