from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.schema import DropTable, Table, MetaData


def getConnection():
    return create_engine('postgresql://postgres:postgrespw@localhost:49153/')


def dropTable():
    conn = getConnection()
    conn.execute(DropTable(
        Table('temp_vehicle', MetaData(), schema='public')
    ))


def loadDB():
    conn = getConnection()
    file = "D:\\Users\\jhonf\\Documents\\Programacion\\Codigo\\Python\\ai-fuel-consumption-ratings\\src\\main" \
           "\\resoures\\MY2022FuelConsumptionRatings.csv"
    datos = pd.read_csv(file, names=['strModelYear', 'strMake', 'strModel', 'strClass'])
    dropTable()
    datos.to_sql('temp_vehicle', con=conn)


def queryPerClass(clase):
    query = """ SELECT *
                FROM "temp_vehicle" """
    dataQuery = pd.read_sql_query(query, con=getConnection())
    return dataQuery


if __name__ == '__main__':
    loadDB()
    print(queryPerClass(""))
