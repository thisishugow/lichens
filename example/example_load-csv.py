from lichens import EtlManager
from lichens.logging import logger as log 
import argparse


em:EtlManager = EtlManager(constr='postgresql+psycopg://username:password@localhost:5432/dbname', name='Sample')


def extract():
    pass

def transform():
    pass 

def load():
    pass 

def update_status():
    pass 


def main():
    parser = argparse.ArgumentParser(description="An example ETL")
    extract()
    transform()
    load()
    update_status()
    pass 


if __name__ == '__main__':
    main()


