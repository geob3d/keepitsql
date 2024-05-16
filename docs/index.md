# Welcome to KeepItSql

For full documentation visit [mkdocs.org](https://www.mkdocs.org).



## Installation

First, you need to install the KeepItSQL package. If it’s available on PyPI, you can install it using pip:

```
pip install keepitsql
```

If it’s not available on PyPI, you can install it directly from the source:
```
git clone https://github.com/your-repo/keepitsql.git
cd keepitsql
pip install .
```


## Modules Overview

KeepItSQL provides two main functionalities:

	•	Insert: Generates an INSERT statement from a dataframe.
	•	Upsert: Generates an UPSERT statement, which can be either a MERGE or INSERT ON CONFLICT statement, based on the database type.

## Quick Start

## Commands

### 1. Creating an Insert Statement

To generate an INSERT statement from a dataframe, use the FromDataframe class:

import pandas as pd
from keepitsql.core.fromdataframe import FromDataframe

```
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

<!-- * `mkdocs new [dir-name]` - Create a new project.
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.
## Project layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.


This site contains the project documentation for the
`calculator` project that is a toy module used in the
Real Python tutorial
[Build Your Python Project Documentation With MkDocs](
    https://realpython.com/python-project-documentation-with-mkdocs/).
Its aim is to give you a framework to build your
project documentation using Python, MkDocs,
mkdocstrings, and the Material for MkDocs theme.

## Table Of Contents

The documentation follows the best practice for
project documentation as described by Daniele Procida
in the [Diátaxis documentation framework](https://diataxis.fr/)
and consists of four separate parts:

1. [Tutorials](tutorials.md)
2. [How-To Guides](how-to-guides.md)
3. [Reference](reference.md)
4. [Explanation](explanation.md)

Quickly find what you're looking for depending on
your use case by looking at the different pages.

## Acknowledgements -->

I want to thank my house plants for providing me with
a negligible amount of oxygen each day. Also, I want
to thank the sun for providing more than half of their
nourishment free of charge.