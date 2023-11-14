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

CONNECTION_STRING: str = "postgresql+psycopg2://username:password@localhost:5432/default"
ETL_NAME: str = "demodb-etl"

em: EtlManager = None

try:
    em = EtlManager(constr=CONNECTION_STRING, name=ETL_NAME)
except ProgramNotFoundError as e:
    add_etl(
        name=ETL_NAME,
        src_folder="source",
        dst_folder="archive",
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
    return df.where(df.notnull(), 'null')


def load(df):
    em.load_df(
        df=df,
        tablename="sample_data",
        schema="pharmquer",
        if_exists="replace",
        chunksize=10,
        unique_key=["process", "param_name", "update_dtt"],
    )

def do(dont_move:bool):
    src_folder = em.src_folder

    for _, f in enumerate(os.listdir(src_folder)):
        em.reload_conf()
        # config of the program 
        conf:dict[str, Any] = em.conf

        _status:str = None
        _last_log:dict[str, Any] = {
            "filename":f,
            "update_dtt":None,
            "message":None,
        }
        try:
            log.info(f"Start to process: {f}")
            extract(fp= os.path.join(src_folder, f))\
                .pipe(transform)\
                .pipe(load)
            _status = 'success'

        except KeyboardInterrupt:
            log.info('<ctrl+c> detected.')
            sys.exit(0)
        
        except Exception as e:
            log.error(e, exc_info=True)
            _status = 'fail'
            _last_log["message"] = str(e)

        finally:
            _last_log["update_dtt"] = pendulum.now().__str__()
            _last_log["statue"] = _status

            em.update_status(
                filename=f, 
                status=_status, 
                user_id=1,
                last_log=_last_log)
            if not dont_move:
                em.move(
                    src=os.path.join(em.src_folder, f),
                    status=_status
                )
            log.info(f"{f} processed. Status: {_last_log}")

@em.scheduled(crontab="*/1 * * * *", times_=10)
def do_with_decorator(dont_move:bool):
    do(dont_move=dont_move)

def main():
    parser = argparse.ArgumentParser(description="An example ETL")
    parser.add_argument("-c", "--cronlike", help="schedule with crontab-like string, e.g., \"*/5 * * * *\"", type=str, default='*/5 * * * *', required=False)
    parser.add_argument("--test-mode", help="keep processed file in source folder", action='store_true', required=False)
    parser.add_argument("-t,", "--times", help="run N times", type=int, default=None, required=False)
    args = parser.parse_args()
    crontab:int = args.cronlike
    dont_move:bool = args.test_mode
    times_:bool = args.times
    
    em.run_as_schtask(func=do, crontab=crontab, times_=times_, dont_move=dont_move)
    # Alternatively, you can run with fixed with decorator @em.scheduled(crontab="* * * * *", times_=10)
    do_with_decorator(dont_move)



if __name__ == "__main__":
    main()
