# Getting Started with KeepItSQL

Welcome to KeepItSQL, a powerful package designed to generate SQL syntax from a dataframe. This package simplifies SQL operations by automatically creating INSERT and UPSERT statements based on your dataframe and the target database.


## Installation

First, you need to install the KeepItSQL package. If it’s available on PyPI, you can install it using pip:

```
pip install keepitsql
```

If it’s not available on PyPI, you can install it directly from the source:
```
git clone https://github.com/geob3d/keepitsql.git
cd keepitsql
pip install .
```

## Examples

### 1. Creating an Insert Statement

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


### 2. Creating an Upsert Statement

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


## Modules Overview

KeepItSQL provides two main functionalities:

	•	Insert: Generates an INSERT statement from a dataframe.
	•	Upsert: Generates an UPSERT statement, which can be either a MERGE or INSERT ON CONFLICT statement, based on the database type.

## Resources
[Documentation](https://geob3d.github.io/keepitsql/)
