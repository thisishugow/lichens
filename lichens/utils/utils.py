from enum import Enum, auto
from types import DynamicClassAttribute
from pandas.core.frame import DataFrame
import pendulum


class EnumBase(Enum):
    def __str__(self):
        return self.name.lower()

    @DynamicClassAttribute
    def name(self):
        """The name of the Enum member."""
        return self._name_.lower()


class Status(EnumBase):
    SUCCESS = auto()
    FAIL = auto()
    SKIP = auto()
    PROCESSING = auto()


class DupPolicy(EnumBase):
    REPLACE = auto()
    SKIP = auto()
    RAISE_ERROR = auto()


def generate_insert_sql(
    source_df: DataFrame,
    tablename: str,
    chunksize: int = None,
    unique_key: list[str] = None,
    skip_on_conflict: bool = False,
) -> str | list[str]:
    """
    Generate SQL INSERT or UPDATE statements for a given DataFrame.

    Args:
        source_df (DataFrame): The source DataFrame containing the data to be inserted into the database.
        tablename (str): The name of the database table where the data will be inserted.
        chunksize (int, optional): The size of each chunk of data to be inserted. If specified, the function generates
            multiple SQL statements, each containing a chunk of rows. If None, a single SQL statement is generated for
            the entire DataFrame. Default is None.
        unique_key (list of str, optional): The column(s) to be used as a unique key to check for duplicates in the
            database table. If duplicates are found, the function generates UPDATE statements instead of INSERT
            statements. Default is None.
        skip_on_conflict (bool, optional): If True, generates SQL statements to skip insertion on conflict (DO NOTHING).
            If False, generates UPDATE statements on conflict. Default is False.

    Returns:
        str | list[str]: If chunksize is provided, a list of SQL statements is returned, with each statement
            containing a chunk of data. If chunksize is None, a single SQL statement is returned as a string,
            containing all the data from the DataFrame.

    Examples:
    ```
    # Generate SQL INSERT or UPDATE statements for the entire DataFrame
    sql_statements = generate_insert_sql(df, 'your_table_name', unique_key=['column1'])

    # Generate SQL INSERT or SKIP statements for the entire DataFrame
    sql_statements = generate_insert_sql(df, 'your_table_name', unique_key=['column1'], skip_on_conflict=True)

    # Generate SQL INSERT or UPDATE statements in chunks of 2 rows
    sql_statements = generate_insert_sql(df, 'your_table_name', chunksize=2, unique_key=['column1', 'column2'])
    ```
    """
    if chunksize:
        sql_text_list: list[str] = []

        for i in range(0, len(source_df), chunksize):
            chunk_df = source_df.iloc[i : i + chunksize]
            if unique_key:
                insert_values = ", ".join(
                    [str(tuple(row)) for _, row in chunk_df.iterrows()]
                )
                if skip_on_conflict:
                    sql_text_list.append(
                        f"INSERT INTO {tablename} ({', '.join(source_df.columns)}) "
                        f"VALUES {insert_values} "
                        f"ON CONFLICT ({', '.join(unique_key)}) DO NOTHING"
                    )
                else:
                    update_values = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in source_df.columns]
                    )
                    sql_text_list.append(
                        f"INSERT INTO {tablename} ({', '.join(source_df.columns)}) "
                        f"VALUES {insert_values} "
                        f"ON CONFLICT ({', '.join(unique_key)}) DO UPDATE SET {update_values}"
                    )
            else:
                insert_values = ", ".join(
                    [str(tuple(row)) for _, row in chunk_df.iterrows()]
                )
                sql_text_list.append(
                    f"INSERT INTO {tablename} ({', '.join(source_df.columns)}) VALUES {insert_values}"
                )

        return sql_text_list
    else:
        sql_text: list[str] = []
        for _, row in source_df.iterrows():
            if unique_key:
                if skip_on_conflict:
                    sql_text.append(
                        f"INSERT INTO {tablename} ({', '.join(source_df.columns)}) "
                        f"VALUES {str(tuple(row.values))} "
                        f"ON CONFLICT ({', '.join(unique_key)}) DO NOTHING"
                    )
                else:
                    update_values = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in source_df.columns]
                    )
                    sql_text.append(
                        f"INSERT INTO {tablename} ({', '.join(source_df.columns)}) "
                        f"VALUES {str(tuple(row.values))} "
                        f"ON CONFLICT ({', '.join(unique_key)}) DO UPDATE SET {update_values}"
                    )
            else:
                sql_text.append(
                    f"INSERT INTO {tablename} ({', '.join(source_df.columns)}) "
                    f"VALUES {str(tuple(row.values))}"
                )

        return sql_text
    

def get_now_str(format="%Y%m%d%H%M%s"):
    return pendulum.now().strftime(format)
