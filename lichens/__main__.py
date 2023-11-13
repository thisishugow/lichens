import json
import os, click
import sys

from sqlalchemy import create_engine
from lichens.db.models import EtlProgMng
from lichens.db.utils import add_etl as _add_etl
from lichens.tools import etl_prog_mng_template

os.environ['LICHENS_HOME'] = os.path.dirname(__file__)
os.environ['ALEMBIC_CONFIG'] = os.path.join(os.environ.get('LICHENS_HOME'), "./db/migrations/alembic.ini")

@click.group()
def cli():
    pass


@click.command(help="Make and migrate the system tables.")
@click.option(
    '-c',
    '--connection-string',
    type=str,
    help="Set the string in format: driver://user:pass@localhost:port/dbname. ",
    show_default=True
)
def migrate(connection_string:str):
    try:
        os.environ['LICHENS_SQLALCHEMY_URL'] = connection_string
        _ = os.popen("""alembic revision --autogenerate -m \"init\"""").read()
        _ = os.popen("""alembic upgrade head""").read()
    except Exception as e:
        print(e)
        sys.exit(1)

@click.command(help="Add an ETL program setting from a JSON")
@click.option(
    '-c',
    '--connection-string',
    type=str,
    help="Set the string in format: driver://user:pass@localhost:port/dbname. ",
    show_default=True
)
@click.option(
    '-j',
    '--json-config',
    type=str,
    help="the ETL program info in json. You can use `get_template` to generate an blank one.",
    show_default=True
)
def add_etl(config:str, connection_string:str):
    try:
        with open(config, 'r') as f:
            orm_conf:dict = json.loads(f.read())
        _ = _add_etl(orm=EtlProgMng(**orm_conf), con=create_engine(connection_string))
        print(f'{orm_conf["name"]} created. ')
    except Exception as e:
        print(e)
        sys.exit(1)

@click.command(help="Generate an ETL program setting to a JSON")
@click.option(
    '-d',
    '--dir',
    type=str,
    default='.',
    help="The directory to generate the json.",
    show_default=True
)
def get_template(dir:os.PathLike)->None:
    with open(os.path.join(dir, 'etl-setting.json'), 'w') as f:
        json.dump(etl_prog_mng_template, f, indent=4)


cli.add_command(migrate)
cli.add_command(add_etl)
cli.add_command(get_template)
cli()