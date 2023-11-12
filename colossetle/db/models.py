from click import DateTime
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base(metadata=MetaData(schema='pharmquer'))


class EtlProcHist(Base):
    __tablename__ = "etl_proc_hist"

    file_name  = Column(String(256), nullable=False, )
    etl_id = Column(Integer, ForeignKey('etl_prog_mng.id'))
    status = Column(String(16), nullable=False,)
    update_by = Column(Integer)
    create_dtt = Column(DateTime, default=func.now(), nullable=False)
    update_dtt = Column(DateTime, default=func.now(), nullable=False)


class EtlProgMng(Base):
    __tablename__ = "etl_prog_mng"

    id = Column(Integer, nullable=False, primary_key=True)
    name  = Column(String(128), nullable=False, unique=True)
    src_folder = Column(String(256), nullable=False,)
    dst_folder = Column(String(256), nullable=False,)
    json_setting = Column(JSONB, nullable=False,)
    last_log = Column(JSONB)
    update_by = Column(Integer)
    update_dtt = Column(DateTime, default=func.now(), nullable=False)