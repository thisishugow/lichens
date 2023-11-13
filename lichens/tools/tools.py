import os 
from lichens.db.models import EtlProgMng
from lichens.db.utils import add_etl as _add_etl
from sqlalchemy import Engine, create_engine
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

etl_prog_mng_template:dict[str, Any] = {
        "name": 'program_name',
        "src_folder": 'path/to/files',
        "dst_folder":'path/to/archive',
        "json_setting": {},
        "last_log": {},
        "update_by": 0
    }