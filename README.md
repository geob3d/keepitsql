## Getting Started with KeepItSQL

Welcome to KeepItSQL, a powerful package designed to generate SQL syntax from a dataframe. This package simplifies SQL operations by automatically creating INSERT and UPSERT statements based on your dataframe and the target database.


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