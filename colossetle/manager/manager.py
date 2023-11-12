from os import PathLike
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from colossetle.db.connection import SessionLocal, get_db_session
from colossetle.errors.db_errors import DatabaseConnectingFailed
from pandas.core.frame import DataFrame

class EtlManager:
    def __init__(self, constr:str, name:str, ) -> None:
        """An ETL manager coworks with Pharmquer

        Args:
            constr (str): database connection string.
            name (str): name of ETL.
        """
        self.constr = constr
        self.name = name
        self._engine:Engine = None
        self._src_folder:PathLike = None
        self._dst_folder:dict[str, PathLike] = {}

    def _fetch_config(self,)->None:
        try:
            self._engine:Engine = create_engine(self.constr)
            _sess:Session = get_db_session()
                
        except Exception as e:
            raise DatabaseConnectingFailed(e)
        
    def valid(self)-> bool:
        """Valid a schema of a transformed csv is legal

        Returns:
            bool: _description_
        """
        pass
        
    def move(self, status:str)->None:
        """Move the processed file to the destination folder according to the status. 

        Args:
            status (str): _description_
        """
        pass

    def update_status(self, )->None:
        """update the status and log to the database.
        """
        pass

    def load_df(self, df:DataFrame, tablename:str, if_exists:str='replace'):
        pass

