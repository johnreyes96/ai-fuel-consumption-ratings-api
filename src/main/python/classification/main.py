from sqlalchemy import create_engine
import pandas as pd


def getConnection():
    return create_engine('postgresql://postgres:postgrespw@localhost:49153/')


def loadDB():
    conn = getConnection()
    file = "D:\\Users\\jhonf\\Documents\\Programacion\\Codigo\\Python\\ai-fuel-consumption-ratings\\src\\main" \
           "\\resoures\\MY2022FuelConsumptionRatings.csv"
    datos = pd.read_csv(file, names=['strModelYear', 'strMake', 'strModel', 'strClass'])
    datos.to_sql('temp_vehicle', con=conn)


def queryPerClass(clase):
    query = """ SELECT *
                FROM "temp_vehicle" """
    dataQuery = pd.read_sql_query(query, con=getConnection())
    return dataQuery


if __name__ == '__main__':
    loadDB()
    print(queryPerClass(""))
