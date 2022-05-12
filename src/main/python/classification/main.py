from sqlalchemy import create_engine
import pandas as pd


def getConnection():
    return create_engine('postgres://postgres:postgrespw@localhost:49153/')


def loadDB():
    file = "MY2022FuelConsumptionRatings.csv"
    datos = pd.read_csv(file, sep='\s+', names=['Name', 'mcg', 'gvh', 'lip', 'chg', 'aac', 'alm1', 'alm2', 'Class'])
    datos.to_sql('temp_vehicle', con=getConnection())


def queryPerClass(clase):
    query = """ SELECT *
                FROM "Ecoli"
                WHERE "Class" = %(clase)s """
    queryParameters = {'clase': clase}
    dataQuery = pd.read_sql_query(query, con=getConnection(), params=queryParameters)
    return dataQuery


if __name__ == '__main__':
    loadDB()
    queryPerClass("")
