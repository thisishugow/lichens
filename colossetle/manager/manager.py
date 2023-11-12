from os import PathLike
import os
from typing import Literal
import pendulum
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from colossetle.db.connection import get_db_session
from colossetle.db.models import EtlProcHist, EtlProgMng
from colossetle.errors.db_errors import *
from colossetle.utils import Status, generate_insert_sql, DupPolicy
from pandas.core.frame import DataFrame


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
        self._src_folder: PathLike = None
        self._dst_folder: dict[str, PathLike] = {}

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
                self._src_folder = self._etl_setting.src_folder
                self._dst_folder[Status.FAIL.name] = os.path.join(
                    self._etl_setting.dst_folder, Status.FAIL.name
                )
                self._dst_folder[Status.SUCCESS.name] = os.path.join(
                    self._etl_setting.dst_folder, Status.SUCCESS.name
                )
                self._dst_folder[Status.SKIP.name] = os.path.join(
                    self._etl_setting.dst_folder, Status.SKIP.name
                )

        except Exception as e:
            raise DatabaseConnectingFailed(e)

    def validate(self) -> bool:
        """Valid a schema of a transformed csv is legal

        Returns:
            bool: if the format is valid.
        """
        pass

    def move(status: Literal[Status.FAIL, Status.SUCCESS, Status.SKIP]) -> None:
        """Move the processed file to the destination folder according to the status.

        Args:
            status (Literal[&#39;fail&#39;, &#39;skip&#39;, &#39;success&#39;]): The process status
        """
        pass

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
        ] = DupPolicy.REPLACE.name,
        chunksize:int=None,
        unique_key:list[str]=None,
    ):
        """_summary_

        Args:
            df (DataFrame): the transformed pd.DataFrame, which has identical schema to the target table. 
            tablename (str): targer table name. If the schema is NOT default, then is MUST BE ADDED.
            schema (str): schema name
            if_exists (Literal[ &#39;replace&#39;, , &#39;skip&#39;, , &#39;raise_error&#39;, ], optional): . Defaults to "replace".
        """
        sess:Session = Session(self._engine)
        if if_exists == DupPolicy.REPLACE.name:
            unique_key = None
            
        if chunksize:
            sql_text_list = [text(a) for a in generate_insert_sql(df, tablename)]
            for _, _sql in enumerate(sql_text_list):
                if if_exists == DupPolicy.REPLACE.name:
                    sess.execute(_sql)
                elif if_exists == DupPolicy.SKIP.name:
                    sess.execute(_sql)
                elif if_exists == DupPolicy.RAISE_ERROR.name:
                    raise RowExistsAlreadyError(msg=f"""""")


        