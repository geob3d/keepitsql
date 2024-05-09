from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from data_engineer_utils import schema_formatter
from sqlalchemy import (
    create_engine,
    inspect,
)

from keepitsql.sql_models import alter_table as at
from keepitsql.sql_models import create_table as ct

# from sql_models import


def generate_add_foreign_key_statement(
    foreign_keys: dict,
    local_table: str,
    schema_prefix: str,
) -> list:
    foreign_key_statements = []

    for fk in foreign_keys:
        new_schema_name = schema_formatter(fk['referred_schema'], schema_prefix)
        referred_table = f"{new_schema_name}.{fk.get('referred_table')}"
        referred_column = fk.get('referred_columns')[0]
        local_column = fk.get('constrained_columns')[0]
        constraint_name = fk.get('name')

        statement = (
            f'ALTER TABLE {local_table} ADD CONSTRAINT {constraint_name}'
            f' FOREIGN KEY ({local_column}) REFERENCES {referred_table} ({referred_column});'
        )

        foreign_key_statements.append(statement)

    return foreign_key_statements if foreign_key_statements else ''


def convert_field_type_by_name(field_name: str, field_type_changes: dict):
    """Converts the field type based on the provided field name and a dictionary of field type changes.

    Parameters
    ----------
    - field_name (str): The name of the field for which the type needs to be converted.
    - field_type_changes (dict): A dictionary mapping field names to their corresponding new types.

    Returns
    -------
    - str: The new field type after conversion. If no conversion is specified for the given field name,
           returns None.
    """
    field_type = field_type_changes.get(field_name) if field_type_changes and field_name in field_type_changes else None

    return field_type


database_url = 'sqlite:///test.db'

database_url = 'sqlite:///test.db'


@dataclass
class CopyDDl:
    database_url: str
    local_table_name: str
    local_schema_name: Optional[str] = None

    def __post_init__(self):
        # Establish a connection and create an inspector in the constructor
        self.db_engine = create_engine(self.database_url)
        self.inspector = inspect(self.db_engine)

    def get_table_info(self):
        """Converts the field type based on the provided field name and a dictionary of field type changes.

        Parameters
        ----------
        - field_name (str): The name of the field for which the type needs to be converted.
        - field_type_changes (dict): A dictionary mapping field names to their corresponding new types.

        Returns
        -------
        - str: The new field type after conversion. If no conversion is specified for the given field name,
            returns None.
        """
        if self.local_schema_name:
            return self.inspector.get_columns(
                self.local_table_name,
                schema=self.local_schema_name,
            )
        else:
            return self.inspector.get_columns(self.local_table_name)

    def get_primary_key_info(self):
        primary_key_info = self.inspector.get_pk_constraint(
            self.local_table_name,
            schema=self.local_schema_name,
        )
        primary_keys = primary_key_info.get('constrained_columns')

        if not primary_keys:
            identity_int_columns = [
                column.get('name') for column in self.get_table_info() if column.get('autoincrement')
            ]
            primary_key_ddl = f"PRIMARY KEY ({','.join(identity_int_columns)})\n" if identity_int_columns else ''
        else:
            primary_key_ddl = f"PRIMARY KEY ({','.join(primary_keys)})\n"

        return primary_key_ddl

    def create_column_ddl(self) -> list:
        column_info = self.get_table_info()
        column_ddl = ',\n'.join(
            [
                ct.create_tbl_column.format(
                    column_name=column.get('name'),
                    column_type=column.get('type', 'UNKNOWN_TYPE'),
                )
                for column in column_info
            ],
        )
        return column_ddl

    def create_constraint_name(
        self,
        constraint_name,
        local_table,
        local_column,
        referred_table,
        referred_column,
    ) -> str:
        if constraint_name is None:
            constraint = ct.create_column_constraint.format(
                referred_table_name=referred_table,
                referred_column_name=referred_column,
                local_table_name=local_table,
                local_column_name=local_column,
            )
        else:
            constraint = constraint_name
        return constraint

    def create_table_name_format(self, new_table_name: str, new_schema_name) -> str:
        # Directly use `or` to fall back to class attributes if arguments are None
        tbl_name = new_table_name or self.local_table_name
        sch_name = new_schema_name or self.local_schema_name

        # Construct and return the fully qualified table name
        return f'{sch_name}.{tbl_name}' if sch_name is not None else tbl_name

    def create_foriegn_key_statements(self, new_schema_name: str = None) -> list:
        foreign_key_info = self.inspector.get_foreign_keys(
            schema=self.local_schema_name,
            table_name=self.local_table_name,
        )

        foreign_key_ddl = '\n'.join(
            [
                at.foreign_key_contraints.format(
                    local_table_name=self.local_table_name,
                    constraint_name=self.create_constraint_name(
                        fk.get(
                            'name',
                        ),  # Use the actual constraint name from the foreign key info
                        self.local_table_name,
                        fk.get('constrained_columns')[0],
                        fk.get('referred_table'),
                        fk.get('referred_columns')[0],
                    )
                    # ,fk.get('name')  ## create function to replace none values
                    ,
                    local_column=fk.get('constrained_columns')[0],  # This will fuck you in the end
                    reffered_table=fk.get('referred_table'),
                    reffered_column=fk.get('referred_columns')[0],
                )
                for fk in foreign_key_info
            ],
        )
        return foreign_key_ddl

    def create_ddl(
        self,
        include_fk: Optional[str] = 'Y',
        new_schema_name: Optional[str] = None,
        new_table_name: Optional[str] = None,
        temp_dll_output: Optional[str] = None,
    ) -> str:
        table_name = (
            self.create_table_name_format(new_table_name, new_schema_name)
            if temp_table_dbms is None
            else (new_table_name or self.local_table_name)
        )

        # tbl_header = (
        #     ct.create_temp_table_header.get(temp_dll_output).format(table_name=table_name)
        #     if temp_table_dbms is not None
        #     else ct.create_table_header.format(table_name=table_name)
        # )

        table_header = ct.create_table_header.format(table_name=table_name)
        temp_table_header = ct.create_temp_table_header.get(temp_dll_output).format(table_name=table_name)

        table_ddl = ct.create_table.format(
            table_header=table_header,
            column_list=self.create_column_ddl(),
            primary_key=self.get_primary_key_info(),
        )
        table_ddl += '\n' + self.create_foriegn_key_statements()

        temp_table_ddl = ct.create_table.format(
            table_header=temp_table_header,
            column_list=self.create_column_ddl(),
            primary_key=self.get_primary_key_info(),
        )

        return table_ddl, temp_table_ddl


# def replace_table_name():


# def create_table_name_formatz(new_table_name: str = None, new_schema_name=None):
#     # Directly use `or` to fall back to class attributes if arguments are None


# def gen_ddl_script(
#     db_connection_string: str,
#     schema_prefix: Optional[str],
#     include_fk: Optional[str],
#     new_table_name: Optional[str],
# ):
