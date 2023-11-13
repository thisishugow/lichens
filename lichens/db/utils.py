from lichens.db.models import EtlProgMng
from sqlalchemy import Engine
from sqlalchemy.orm import Session

def add_etl(orm:EtlProgMng, con:Engine)->str | None:    
    sess:Session = None
    try:
        sess = Session(con)
        sess.add(orm)
        sess.commit()
        return orm.name
    except Exception as e:
        sess.rollback()
        raise e
    finally:
        if sess:
            sess.close()
