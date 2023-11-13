from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# postgresql+psycopg2://scott:tiger@host/dbname
DB_URL = ''
engine = create_engine(DB_URL, echo=True)
SessionLocal:Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_session():
    """Create a user auth db session.

    Returns:
        Session: a sqlalchemy Session instance used to conduct db operations. 
    """
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


