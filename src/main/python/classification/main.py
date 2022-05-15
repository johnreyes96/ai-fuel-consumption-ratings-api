from sqlalchemy import create_engine
import pandas as pd


def getConnection():
    return create_engine('postgresql://postgres:postgrespw@localhost:49153/')


def loadDB():
    conn = getConnection()
    file = "D:\\Users\\jhonf\\Documents\\Programacion\\Codigo\\Python\\ai-fuel-consumption-ratings\\src\\main" \
           "\\resoures\\MY2022FuelConsumptionRatings.csv"
    vehicleColumnNames = ['strModelYear', 'strMake', 'strModel', 'strClass']
    fuelConsumptionColumnNames = ['douEngineSize_L', 'intCylinders', 'strTransmission', 'strFuelType',
                                  'douFuelConsumption_City_L-100km', 'douFuelConsumption_Hwy_L-100km',
                                  'douFuelConsumption_Comb_L-100km', 'intFuelConsumption_Comb_mpg',
                                  'intCO2Emissions_g-km', 'intCO2Rating', 'intSmogRating']
    vehicleData = pd.read_csv(file, usecols=vehicleColumnNames)
    print(vehicleData)
    fuelConsumptionData = pd.read_csv(file, usecols=fuelConsumptionColumnNames)
    vehicleData = vehicleData.drop_duplicates()
    vehicleData.to_sql('tbl_vehicle', if_exists='replace', con=conn)
    fuelConsumptionData.to_sql('tbl_fuelConsumption', if_exists='replace', con=conn)


def queryPerClass(clase):
    query = """ SELECT *
                FROM "tbl_vehicle" """
                # WHERE "Class" = %(clase)s """
    # queryParameters = {'clase': clase}
    # dataQuery = pd.read_sql_query(query, con=getConnection(), params=queryParameters)
    dataQuery = pd.read_sql_query(query, con=getConnection())
    return dataQuery


if __name__ == '__main__':
    loadDB()
    print(queryPerClass(""))
