## 1. Creating an Insert Statement

To generate an INSERT statement from a dataframe, use the FromDataframe class:


```
import pandas as pd
from keepitsql.core.fromdataframe 


# Sample data
data = {
    "ItemID": ["ID101", "ID102", "ID103", "ID104", "ID105"],
    "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor", "Mouse"],
    "Description": [
        "15-inch laptop with 8GB RAM",
        "Ergonomic office chair",
        "1m USB-C charging cable",
        "24-inch LED monitor",
        "Magic Apple",
    ],
    "Category": ["Electronics", "Furniture", "Electronics", "Electronics", "Accessories"],
    "Quantity": [10, 5, 50, 8, 4],
    "Location": ["Warehouse A", "Warehouse B", "Warehouse A", "Warehouse C", "Warehouse C"],
}

df = pd.DataFrame(data)

# Generate Insert Statement
insert_generator = FromDataframe(target_table="your_table_name", dataframe=df, target_schema="your_schema")
insert_statement = insert_generator.insert()
print(insert_statement)
```


## 2. Creating an Upsert Statement

The FromDataframe class can also generate an UPSERT statement, dynamically choosing between a MERGE or INSERT ON CONFLICT statement based on the target database:

```

import pandas as pd
from keepitsql.core.fromdataframe


# Sample data
data = {
    "ItemID": ["ID101", "ID102", "ID103", "ID104", "ID105"],
    "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor", "Mouse"],
    "Description": [
        "15-inch laptop with 8GB RAM",
        "Ergonomic office chair",
        "1m USB-C charging cable",
        "24-inch LED monitor",
        "Magic Apple",
    ],
    "Category": ["Electronics", "Furniture", "Electronics", "Electronics", "Accessories"],
    "Quantity": [10, 5, 50, 8, 4],
    "Location": ["Warehouse A", "Warehouse B", "Warehouse A", "Warehouse C", "Warehouse C"],
}

df = pd.DataFrame(data)

# Generate Upsert Statement
upsert_generator = FromDataframe(
    target_table="your_table_name",
    dataframe=df,
    target_schema="your_schema",
    source_table="source_table_name",
    match_condition=["ItemID"],
    dbms_output="postgresql"  # or "mssql", "oracle", etc.
)
upsert_statement = upsert_generator.upsert()
print(upsert_statement)

```


### DBMS Upsert Types

The following table shows the supported DBMS and their respective upsert types:


| DMBS       | DBMS Output Parameter | Upsert Type |   
|  :-------: | :-------:             |  :-------:     |
| Oracle     |                       | MERGE   |
| SQL Server | MERGE                 |  MERGE   |
| IBM Db2	 | MERGE                 |  MERGE   |
| PostgreSQL | postgresql            | ON CONFLICT   |
| SQLite     | sqlite                | ON CONFLICT   |
| Teradata   | teradata              | MERGE   |
| SAP HANA   | sap_hana              | MERGE   |
| Snowflake  | snowflake             | MERGE   |
| Redshift   | redshift              | MERGE   |
| BigQuery   | bigquery              | MERGE   |




<!-- DBMS	Upsert Type
Oracle	MERGE
SQL Server	MERGE
IBM Db2	MERGE
PostgreSQL	ON CONFLICT / MERGE (15+)
MySQL	ON DUPLICATE KEY UPDATE
SQLite	ON CONFLICT
Teradata	MERGE
SAP HANA	MERGE
Snowflake	MERGE
Redshift	MERGE
BigQuery	MERGE -->