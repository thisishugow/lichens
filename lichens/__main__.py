import os, click
import sys


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

cli.add_command(migrate)
cli()