import os
import sys
from typing import Literal, Any
import argparse
from time import sleep
from pandas.core.frame import DataFrame
import pandas as pd
import pendulum

from lichens import EtlManager
from lichens.errors.db_errors import ProgramNotFoundError
from lichens.logging import logger as log
from lichens.tools import add_etl

CONNECTION_STRING: str = "postgresql+psycopg://username:password@localhost:5432/dbname"
ETL_NAME: str = "Sample"

em: EtlManager = None

try:
    em = EtlManager(constr=CONNECTION_STRING, name=ETL_NAME)
except ProgramNotFoundError as e:
    add_etl(
        name=ETL_NAME,
        src_folder="path/to/files",
        dst_folder="path/to/archive",
        json_setting={},
        update_by=1,
        con=CONNECTION_STRING,
    )
    em = EtlManager(constr=CONNECTION_STRING, name=ETL_NAME)


def extract(fp: os.PathLike) -> DataFrame:
    df:DataFrame = pd.read_csv(fp)
    # optionally do simple cleaning some here
    return df


def transform(df: DataFrame) -> DataFrame:
    """Transform and validate the df. Use `pandera` to construct 
    the pipe and conduct validation is highly recommended.

    Vist [pandera](https://pandera.readthedocs.io/en/stable/)

    Example:
    ```
    import pandas as pd
    import pandera as pa

    # data to validate
    df = pd.DataFrame({
        "column1": [1, 4, 0, 10, 9],
        "column2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        "column3": ["value_1", "value_2", "value_3", "value_2", "value_1"],
    })

    # define schema
    schema = pa.DataFrameSchema({
        "column1": pa.Column(int, checks=pa.Check.le(10)),
        "column2": pa.Column(float, checks=pa.Check.lt(-1.2)),
        "column3": pa.Column(str, checks=[
            pa.Check.str_startswith("value_"),
            # define custom checks as functions that take a series as input and
            # outputs a boolean or boolean Series
            pa.Check(lambda s: s.str.split("_", expand=True).shape[1] == 2)
        ]),
    })

    validated_df = schema(df)
    print(validated_df)


    # Use `check_io`
    from pandera import DataFrameSchema, Column, Check, check_input


    df = pd.DataFrame({
    "column1": [1, 4, 0, 10, 9],
    "column2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    })

    in_schema = DataFrameSchema({
    "column1": Column(int),
    "column2": Column(float),
    })

    out_schema = in_schema.add_columns({"column3": Column(float)})

    @pa.check_io(df1=in_schema, df2=in_schema, out=out_schema)
    def preprocessor(df1, df2):
        return (df1 + df2).assign(column3=lambda x: x.column1 + x.column2)

    preprocessed_df = preprocessor(df, df)
    print(preprocessed_df)
    ```



    Args:
        df (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    return df


def load(df):
    em.load_df(
        df=df,
        tablename="sample_table",
        schema="public",
        if_exists="raise_error",
        chunksize=500,
        unique_key=["column1", "column2"],
    )


def main():
    parser = argparse.ArgumentParser(description="An example ETL")
    parser.add_argument("-i", "--interval", help="Batch interval in seconds", default=30, required=False)
    args = parser.parse_args
    interval:int = args.interval
    em.reload_conf()

    # config of the program 
    conf:dict[str, Any] = em.conf
    src_folder = em.src_folder
    while True:
        for _, f in os.listdir(src_folder):
            _status:str = None
            _last_log:dict[str, Any] = {
                "filename":f,
                "update_dtt":None,
                "message":None,
            }
            try:
                log.info(f"Start to process: {f}")
                extract(fp=f)\
                    .pipe(transform)\
                    .pipe(load)
                _status = 'success'

            except KeyboardInterrupt:
                log.info('<ctrl+c> detected.')
                sys.exit(0)
            
            except Exception as e:
                log.error(e, exc_info=True)
                _status = 'fail'
                _last_log["message"] = e

            finally:
                _last_log["update_dtt"] = pendulum.now()
                em.update_status(
                    filename=f, 
                    status=_status, 
                    last_log=_last_log)
                em.move(
                    src=os.path.join(em.src_folder, f),
                    status=_status
                )
                log.info(f"{f} processed. Status: {_last_log}")
        log.info(f'Waiting {interval} for next run.')
        sleep(interval)


if __name__ == "__main__":
    main()
