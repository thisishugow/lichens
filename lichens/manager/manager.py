from os import PathLike
import os
from time import sleep
from typing import Literal, Callable
from crontab import CronTab
import pendulum
import shutil
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from lichens.db.models import EtlProcHist, EtlProgMng
from lichens.errors.db_errors import *
from lichens.utils import Status, generate_insert_sql, DupPolicy
from pandas.core.frame import DataFrame
import abc
from logging import getLogger

log = getLogger()


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
        self.id: int = None
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
            self.id = self._etl_setting.id
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

        except ProgramNotFoundError as e:
            raise e
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
        fn:os.PathLike = os.path.basename(src)
        if not os.path.exists(self.dst_folder.get(status)):
            os.makedirs(self.dst_folder.get(status))
        dst:os.PathLike = os.path.join(self.dst_folder.get(status), fn)
        try: 
            shutil.move(src, dst)
        except Exception as e:
            raise e
    def get_queue_list(self,)->list[str]:
        """Get the queued files

        Raises:
            e: _description_

        Returns:
            list[str]: list contains unprocessed file names. 
        """
        try:
            sess:Session = Session(self._engine)
        except Exception as e:
            raise e
        
        try:
            res:list[EtlProcHist] = sess.query(EtlProcHist)\
                .filter(
                    EtlProcHist.etl_id == self.id,
                    EtlProcHist.status.in_(['queue', None]) ).all()
            return [a.file_name for a in res]
        except Exception as e:
            sess.rollback()
        finally:
            sess.close()
        
    def update_status(
        self,
        filename: str,
        user_id:int,
        status: Literal[Status.FAIL, Status.SUCCESS, Status.SKIP, Status.PROCESSING],
        last_log: dict[str, str] = None,
    ) -> None:
        """update the current status and log to the database.
        Args:
            filename (str): processed filename.
            user_id (int): the user who upload or process the file. 
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
                print()
                s.query(EtlProgMng).filter(
                    EtlProgMng.id == self._etl_setting.id
                ).update({EtlProgMng.last_log: last_log})

                updated_rows:int = s.query(EtlProcHist)\
                    .filter(EtlProcHist.file_name == filename)\
                    .update({
                        EtlProcHist.status: status,
                        EtlProcHist.last_log: last_log,
                    })
                if updated_rows==0:
                    log.warning(f"{filename} not registed. Insert new when processed.")
                    new:dict = {
                        "file_name":filename,
                        "etl_id":self._etl_setting.id,
                        "status": status,
                        "update_by": user_id,
                        "last_log": last_log,
                        "create_dtt": pendulum.now().__str__(),
                    }
                    s.add(EtlProcHist(**new))
                else: 
                    log.info(f"{filename} done with status={status}. Log updated.")
                s.commit()
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
            sess.commit()
        except Exception as e:
            sess.rollback()
            raise InsertInterruptedError(e)
        finally:
            sess.close()

    def run_as_schtask(self, func:Callable, crontab:str, times_:int=-1, *args, **kwargs)->None:
        """
        Run a function based on a cron-like schedule using a Schtasks approach.

        Args:
            func (Callable): The function to be executed.
            crontab (str): A string representing the cron-like schedule for function execution.
            times_ (int, optional): The number of times the function should be executed. 
                If set to -1 (default), the function runs indefinitely based on the cron schedule.

        Returns:
        None

        Example:
        ```
        # Create an instance of EtlManager
        em = EtlManager()

        # Define a function to be executed
        def your_function():
            # Your function logic here
            pass

        # Run the function every minute for a total of 5 times
        em.run_as_schtask(your_function, '*/1 * * * *', times_=5)
        ```

        Note:
        - The `crontab` parameter follows the standard cron format.
        - If `times_` is set to -1, the function runs indefinitely based on the cron schedule.
        - The `func` parameter should be a callable function with no arguments.

        """
        runs:int = 0
        while runs<times_:
            sec_waiting_next_run:int = CronTab(crontab).next()
            func(*args, **kwargs)
            runs += 1
            if runs<times_:
                log.info(f'>>> Wait {sec_waiting_next_run} seconds for next run <<<')
                sleep(sec_waiting_next_run)

    def scheduled(self, crontab:str, times_:int=-1)->None:
        """
        Decorator to schedule a function to run based on a cron-like schedule.

        Args:
            crontab (str): A string representing the cron-like schedule for function execution.
            times_ (int, optional): The number of times the function should be executed.
                If set to -1 (default), the function runs indefinitely based on the cron schedule.

        Returns:
        Callable: The decorated function.

        Example:
        ```
        # Create an instance of EtlManager
        em = EtlManager()

        # Decorate the function with the scheduled decorator
        @em.scheduled(crontab='*/1 * * * *', times_=5)
        def your_scheduled_function():
            # Your function logic here
            pass
        ```

        Note:
        - The `crontab` parameter follows the standard cron format.
        - If `times_` is set to -1, the function runs indefinitely based on the cron schedule.
        - The decorated function should not have any arguments.

        """
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                self.run_as_schtask(func, crontab, times_, *args, **kwargs)
            return wrapper
        return decorator
                


        