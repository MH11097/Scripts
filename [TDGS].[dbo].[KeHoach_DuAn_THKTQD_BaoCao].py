import os
from os import close
import time
import datetime
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


def Select_new_data_src(table_Src, cursor, max):
    cmd = "SELECT Ghichu, ID, DonViID, NamKeHoach, TrangThai, NgayCapNhat, \
                   LoaiDonVi, DotKeHoach, IDUser, IsKhoa FROM " + table_Src
    data_query = cursor.execute(cmd).fetchall()
    datas = pd.DataFrame(data_query)
    logging.info('Retrived lastest datas from source: Table ' + table_Src)
    return datas


def Delete_from_des(table_Des, cursor):
    cmd = "DELETE FROM " + table_Des
    cursor.execute(cmd)
    logging.info('Deleted from des')


def Insert_into_des(datas, table_Des, cursor):
    count = 0
    while count < len(datas):
        cmd = 'INSERT INTO ' + table_Des + ' (Ghichu, ID, DonViID, NamKeHoach, TrangThai, NgayCapNhat, \
                  LoaiDonVi, DotKeHoach, IDUser, IsKhoa) VALUES '
        if len(datas) - count > 200:
            for i in range(200):
                temp = datas[0][count+i]
                for j in range(len(temp)):
                    if temp[j] is True:
                        temp[j] = 1
                    if temp[j] is False:
                        temp[j] = 0
                    if temp[j] is None:
                        temp[j] = 'NULL'
                    if isinstance(temp[j], datetime.datetime):
                        temp[j] = temp[j].strftime('%Y-%m-%d %H:%M:%S')
                temp_str = str(temp)
                temp_str = temp_str[0] + 'N'+temp_str[1:]
                cmd += temp_str + ','
            count += 200
        else:
            for i in range(len(datas)-count):
                temp = datas[0][count+i]
                for j in range(len(temp)):
                    if temp[j] is True:
                        temp[j] = 1
                    if temp[j] is False:
                        temp[j] = 0
                    if temp[j] is None:
                        temp[j] = 'NULL'
                    if isinstance(temp[j], datetime.datetime):
                        temp[j] = temp[j].strftime('%Y-%m-%d %H:%M:%S')
                temp_str = str(temp)
                cmd += temp_str + ','
            count = len(datas)
        cmd = cmd[:len(cmd)-1].replace("\'NULL\'", "NULL")
        cmd = cmd.replace("NNULL", "NULL")

        print(cmd)
        cursor.execute(cmd)
    logging.info('Inserted ' + str(len(datas)) +
                 ' row(s) into Des: Table ' + table_Des)


def Get_time():
    return time.strftime(r"%Y-%m-%d %H:%M:%S", time.localtime())


def main():

    Start_time = Get_time()
    # Src
    ServerSrc = '10.2.9.21,1433'
    UIDSrc = 'dtc_edsolabs'
    PWDSrc = 'DTc_3ds0!@bs_2o20'
    DatabaseSrc = 'TDGS'
    table_Src = 'KeHoach_DuAn_THKTQD_BaoCao'

    # Des
    ServerDes = '10.2.6.20,1433'
    UIDDes = 'edsolabs'
    PWDDes = 'axz@312'
    DatabaseDes = 'TDGS_MART_21'
    table_Des = 'KeHoach_DuAn_THKTQD_BaoCao_hieulm'

    conn_Str_Src = 'Driver={SQL Server};'+'Server={};Database={};UID={};PWD={};Trusted_Connection=no'.format(
        ServerSrc, DatabaseSrc, UIDSrc, PWDSrc)
    conn_Str_Des = 'Driver={SQL Server};'+'Server={};Database={};UID={};PWD={};Trusted_Connection=no'.format(
        ServerDes, DatabaseDes, UIDDes, PWDDes)

    if not os.path.exists("logs/"):
        os.makedirs("logs/")

    logging.basicConfig(filename="[TDGS].[dbo].[KeHoach_DuAn_THKTQD_BaoCao]/"+time.strftime(r"%Y-%m-%d %H-%M-%S", time.localtime(
    ))+".log", level=logging.INFO, format='%(asctime)s - %(levelname)-5s - %(message)s', datefmt='%H:%M:%S')
    logging.info('---Start running: ' + Start_time + '---')

    #
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
