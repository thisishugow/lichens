from os import PathLike
import os
from typing import Literal
import pendulum
import shutil
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from lichens.db.models import EtlProcHist, EtlProgMng
from lichens.errors.db_errors import *
from lichens.utils import Status, generate_insert_sql, DupPolicy
from pandas.core.frame import DataFrame
import abc


class EtlManager:
    def __init__(
        self,
        constr: str,
        name: str,
    ) -> None:
        """An ETL manager coworks with Pharmquer

        Args:
            constr (str): database connection string.
            name (str): name of ETL.
        """
        self.constr: str = constr
        self.name: str = name
        self._engine: Engine = None
        self._etl_setting: EtlProgMng = None
        self.src_folder: PathLike = None
        self.dst_folder: dict[str, PathLike] = {}
        self.conf:dict = None

        self._fetch_config()

    def _fetch_config(
        self,
    ) -> None:
        try:
            self._engine: Engine = create_engine(self.constr)
            with Session(self._engine) as sess:
                self._etl_setting = (
                    sess.query(EtlProgMng).filter(EtlProgMng.name == self.name).first()
                )
            if not self._etl_setting:
                raise ProgramNotFoundError(f"Name={self.name} not found. You can use lichens.tools.add_etl() to add one first.")
            
            self.src_folder = self._etl_setting.src_folder
            self.dst_folder[Status.FAIL.name] = os.path.join(
                self._etl_setting.dst_folder, Status.FAIL.name
            )
            self.dst_folder[Status.SUCCESS.name] = os.path.join(
                self._etl_setting.dst_folder, Status.SUCCESS.name
            )
            self.dst_folder[Status.SKIP.name] = os.path.join(
                self._etl_setting.dst_folder, Status.SKIP.name
            )
            self.conf:dict = self._etl_setting.json_setting

        except Exception as e:
            raise DatabaseConnectingFailed(e)
        
    def reload_conf(self):
        self._fetch_config()

    def move(self, src:os.PathLike, status: Literal[Status.FAIL, Status.SUCCESS, Status.SKIP]) -> None:
        """Move the processed file to the destination folder according to the status.

        Args:
            src (PathLike): The path of source file. 
            status (Literal[&#39;fail&#39;, &#39;skip&#39;, &#39;success&#39;]): The process status
        """
        dst:os.PathLike = os.join(self.dst_folder, f"{status}/")
        try: 
            shutil.move(src, dst)
        except Exception as e:
            raise e

    def update_status(
        self,
        filename: str,
        status: Literal[Status.FAIL, Status.SUCCESS, Status.SKIP, Status.PROCESSING],
        last_log: dict[str, str] = None,
    ) -> None:
        """update the current status and log to the database.
        Args:
            filename (str): processed filename.
            status (Literal[&#39;fail&#39;, &#39;skip&#39;, &#39;success&#39;, &#39;processing&#39;]): The current status.
            last_log (dict[str, str]): log in json. Recommended&Default={ "status": "processing", "filename":"sample.csv", "update_dtt": pendulum.now()}.
        """
        if not last_log:
            last_log = {
                "status": status,
                "filename": filename,
                "update_dtt": pendulum.now(),
            }
        with Session(self._engine) as s:
            try:
                s.query(EtlProgMng).filter(
                    EtlProgMng.id == self._etl_setting.id
                ).update({EtlProgMng.last_log: last_log})

                s.query(EtlProcHist).filter(EtlProcHist.file_name == filename).update(
                    EtlProcHist.status == status
                )

            except Exception as e:
                s.rollback()
                raise UpdateStatusFailed(f"""File: {filename}. Errors: {e}""")

    def load_df(
        self,
        df: DataFrame,
        tablename: str,
        schema:str=None,
        if_exists: Literal[
            DupPolicy.REPLACE, DupPolicy.RAISE_ERROR, DupPolicy.SKIP
        ] = DupPolicy.RAISE_ERROR.name,
        chunksize:int=None,
        unique_key:list[str]=None,
    )->None:
        """Load a DataFrame to the target table. 

        Args:
            df (DataFrame): the transformed pd.DataFrame, which has identical schema to the target table. 
            tablename (str): targer table name. If the schema is NOT default, then is MUST BE ADDED.
            schema (str): schema name
            if_exists (Literal[ &#39;replace&#39;, , &#39;skip&#39;, , &#39;raise_error&#39;, ], optional): . Defaults to "replace".
        """
        try:
            sess:Session = Session(self._engine)
        except Exception as e:
            raise e
        
        if if_exists!=DupPolicy.SKIP.name and not unique_key:
            raise UniqueKeyMissedError(f"Please specify the unique key from {str(tuple(df.columns))}")
        
        if schema:
            tablename = f"{schema}.{tablename}"

        def _do_insert(unique_key_:list[str]=None, skip_on_conflict_:bool=False):
            _sql: list[str] | str = generate_insert_sql(df, tablename, chunksize, unique_key_, skip_on_conflict_)
            _sql = _sql if isinstance(_sql, list) else [_sql, ]
            return list(map(sess.execute, [text(a) for a in _sql]))
        
        try:
            if if_exists == DupPolicy.REPLACE.name:
                _ = _do_insert(unique_key, True)
            elif if_exists == DupPolicy.SKIP.name:
                _ = _do_insert(unique_key, False)
            else: #  if_exists == DupPolicy.RAISE_ERROR.name
                _ = _do_insert(None, False)
 
        except Exception as e:
            sess.rollback()
            raise InsertInterruptedError(e)
        finally:
            sess.close()
                


        