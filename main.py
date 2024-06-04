#!/usr/bin/python

from src.utils import *
import sys
import os

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir_src = os.path.join(act_dir, 'src')
sys.path.append(proyect_dir_src)


__table__name__ = 'base_sbs_stock_hogar'

if __name__ == "__main__":

    load(extract('data/Stock Hogar SBS.xlsx'),
         __table__name__,
         engine_1())

    validate(engine_1(), __table__name__)
    #encoding('data/Asistencia_SME_COMUNES_20240425.csv')
