#!/usr/bin/python

import os
import sys
import logging
from urllib.parse import quote
import sqlalchemy as sa
from sqlalchemy import text, inspect
import yaml
import pandas as pd
from unidecode import unidecode

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


def get_engine(username: str, password: str, host: str, database: str, port: str = 3306, *_):
    return sa.create_engine(f"postgresql://{username}:{quote(password)}@{host}:{port}/{database}")


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


def engine_1():
    return get_engine(**source1).connect()


def extract(path):

    df = pd.read_excel(path)
    logging.info(f'{path}')
    logging.info(f'{len(df)} datos')
    return df


def load(df: pd.DataFrame, __table__name__: str, conn):
    
    df.columns = [unidecode(col) for col in df.columns]
    
    df.columns = df.columns.str.replace(' ','_')
    
    """cols = pd.Series(df.columns)
    duplicados = cols.duplicated(keep=False)
    cols[duplicados] = cols[duplicados].astype(str) + '_duplicado'
    print(cols[duplicados])
    df.columns = cols"""
    
    print(df.columns)

    with conn as con:

        df.to_sql(__table__name__, con, if_exists='replace', index=False)


def validate(conn, __table__name__):

    inspector = inspect(conn)

    tablas = inspector.get_table_names()

    with conn as con:
        
        intsert_table =text("""INSERT INTO archive ("archive", "table") VALUES
                                ('SBS Cancer FZ - Abril 2024 - VFinal.xlsx', 'base_sbs_cancer_fz'),
                                ('VIGENTES Y ANULADAS TVNOVEDADES ABRIL 2024.xlsx', 'base_sbs_vigentes_anuladas_tvnovedades')""")

        if __table__name__ in tablas:
            logging.info('Existe')
            #con.execute(text(intsert_table))
        else:
            logging.info('No existe')
        consulta = f"SELECT COUNT(*) FROM {__table__name__};"
        """intsert_table = "INSERT INTO archive ("archive", "table") VALUES
                            ('caribe_mar_20240409.csv', 'caribe_mar_20240409'),
                            ('caribe_sol_20240409.csv', 'caribe_sol_20240409'),
                            ('edeq_20240409.csv', 'edeq_20240409'),
                            ('eep_20240402.csv', 'eep_20240402'),
                            ('gas_cundi_20240409.csv', 'gas_cundi_20240409'),
                            ('gas_nacer_20240409.csv', 'gas_nacer_20240409'),
                            ('vanti_20240409.csv', 'vanti_20240409'),
                            ('gas_oriente_20240409.csv', 'gas_oriente_20240409'),
                            ('datacredito.csv', 'datacredito_20240409')"""
        resultado = con.execute(text(consulta))
        num_filas = resultado.scalar()
        logging.info(f'{__table__name__} con {num_filas} datos')
