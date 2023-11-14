import os
import pendulum 
from lichens.db.models import EtlProgMng, EtlProcHist
from lichens.db.utils import add_etl as _add_etl
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from typing import Any

def add_etl(name:str, src_folder:os.PathLike, dst_folder:os.PathLike, json_setting:dict, update_by:int, con:str | Engine)->str:
    """_summary_

    Args:
        name (str): Name of the program
        src_folder (os.PathLike): source folder
        dst_folder (os.PathLike): archive folder
        json_setting (dict): setting for program
        update_by (int): user id.
        con (str | Engine): connection strings or an sqlalchemy.Engine

    Returns:
        str: _description_
    """
    if isinstance(con, str):
        con = create_engine(con)

    orm_dict:dict = {
        "name":name,
        "src_folder":src_folder,
        "dst_folder":dst_folder,
        "json_setting":json_setting,
        "last_log":None,
        "update_by": update_by
    }
    etl_name:str = _add_etl(orm=EtlProgMng(**orm_dict), con=con)
    return etl_name

def add_file(file_name:str, etl_id:int, update_by:int, con:str | Engine):
    """Regist the file to queue.

    Args:
        file_name (str): filename
        etl_id (int): belong to which etl
        update_by (int): user id
        con (str | Engine): target database. Connection string or a sqlalchemy.Engine are accepted.

    Raises:
        e: Fail to add. 
    """
    if isinstance(con, str):
        con = create_engine(con)
    orm_dict:dict[str, Any] = {
        "file_name":file_name,
        "etl_id": etl_id,
        "update_by":update_by,
        "status": 'queue',
        "create_dtt": pendulum.now(),
    }
    with Session(con) as sess:
        try:
            sess.add(EtlProcHist(**orm_dict))
            sess.commit()
        except Exception as e:
            sess.rollback()
            raise e

etl_prog_mng_template:dict[str, Any] = {
        "name": 'program_name',
        "src_folder": 'path/to/files',
        "dst_folder":'path/to/archive',
        "json_setting": {},
        "last_log": {},
        "update_by": 0
    }