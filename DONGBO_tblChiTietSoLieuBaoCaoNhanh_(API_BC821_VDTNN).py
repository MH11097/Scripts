from os import close
import time
from sqlalchemy import create_engine, engine
import sqlalchemy as db
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import select
from urllib.parse import quote_plus
import pandas as pd
import logging
import sys
import pyodbc as pyodbc
from sqlalchemy.sql.schema import MetaData


def Get_Engine(ConnectionString):
    engine = create_engine(
        'mssql+pyodbc:///?odbc_connect={}'.format(quote_plus(ConnectionString)))
    return engine


def Get_Connection(ConnectionString):
    engine = Get_Engine(ConnectionString)
    connection = engine.raw_connection()
    logging.info('Connected to SQL')
    return connection


def Select_max_date_from_des(table_Des, cursor):
    cmd = 'Select max(ngayketso) from ' + table_Des
    data_query = cursor.execute(cmd).fetchall()
    if str(data_query[0][0]) == 'None':
        return '0'
    return str(data_query[0][0])


def Delete_from_des(table_Des, cursor):
    cmd = "DELETE FROM " + table_Des
    cursor.execute(cmd)
    logging.info('Deleted from des')


def Select_new_data_src(table_Src, cursor, max):
    cmd = "SELECT intid, idChiTieuBaoCaoNhanh, intNam, intSothang, \
                    floatGiaTriSoThang, SoVoiCungKy, floatItem \
                FROM " + table_Src
    data_query = cursor.execute(cmd).fetchall()
    datas = pd.DataFrame(data_query)
    logging.info('Retrived lastest datas from source: Table ' + table_Src)
    return datas


def Insert_into_des(datas, table_Des, cursor):
    count = 0
    while count < len(datas):
        cmd = 'INSERT INTO ' + table_Des + \
            ' (intid, idChiTieuBaoCaoNhanh, intNam, intSothang, floatGiaTriSoThang, SoVoiCungKy, floatItem) VALUES '
        if len(datas) - count > 200:
            for i in range(200):
                temp = str(datas[0][count+i])
                if datas[0][count+i][6] is None:
                    temp = temp[:len(temp)-6] + ' NULL)'
                cmd += temp + ','
            count += 200
        else:
            for i in range(len(datas)-count):
                cmd += str(datas[0][count+i]) + ','
            count = len(datas)
        cmd = cmd[:len(cmd)-1]
        print(cmd)
        cursor.execute(cmd)

    logging.info('Inserted ' + str(len(datas)) +
                 ' row(s) into Des: Table ' + table_Des)


def Get_time():
    return time.strftime(r"%Y-%m-%d %H:%M:%S", time.localtime())


def main():

    # Variables
    Start_time = Get_time()
    # Src
    ServerSrc = '10.2.9.61'
    DatabaseSrc = 'DataCongKhaiThongTin'
    UIDSrc = 'cktt_edsolabs'
    PWDSrc = 'CkTT_3ds0!@bs_2o20'

    # Des
    ServerDes = '10.2.6.20,1433'
    DatabaseDes = 'DataCongKhaiThongTin_MART'
    UIDDes = 'edsolabs'
    PWDDes = 'axz@312'

    conn_Str_Src = 'Driver={SQL Server};'+'Server={};Database={};UID={};PWD={};Trusted_Connection=no'.format(
        ServerSrc, DatabaseSrc, UIDSrc, PWDSrc)
    conn_Str_Des = 'Driver={SQL Server};'+'Server={};Database={};UID={};PWD={};Trusted_Connection=no'.format(
        ServerDes, DatabaseDes, UIDDes, PWDDes)

    table_Src = 'tblChiTietSoLieuBaoCaoNhanh'
    table_Des = 'tblChiTietSoLieuBaoCaoNhanh'
    logging.basicConfig(filename="DONGBO_tblChiTietSoLieuBaoCaoNhanh_(API_BC821_VDTNN)/"+time.strftime(r"%Y-%m-%d %H-%M-%S",
                        time.localtime())+".log", level=logging.INFO, format='%(asctime)s - %(levelname)-5s - %(message)s', datefmt='%H:%M:%S')
    logging.info('---Start running: ' + Start_time + '---')

    # Methods
    try:
        conn_Src = Get_Connection(conn_Str_Src)
        cursor = conn_Src.cursor()
        datas = Select_new_data_src(table_Src, cursor, max)
        conn_Src.commit()
        conn_Src.close()

        conn_Des = Get_Connection(conn_Str_Des)
        cursor = conn_Des.cursor()
        Delete_from_des(table_Des, cursor)
        Insert_into_des(datas, table_Des, conn_Des)
        conn_Des.commit()
        conn_Des.close()

        End_time = Get_time()
        logging.info('---Successfully Synced: ' + End_time + '---')

    except Exception:

        logging.error('Failed to Sync: ', exc_info=sys.exc_info())
        Stop_time = Get_time()
        logging.info('---Stop Syncing: ' + Stop_time + '---')


# Main
main()
