from sqlalchemy import create_engine
import pandas as pd


def getConnection():
    return create_engine('postgresql://postgres:postgrespw@localhost:49153/')


def getVehiclesId(fuelConsumptionData):
    allVehicles = findAllVehicles()
    vehiclesId = []
    for index, fuelConsumption in fuelConsumptionData.iterrows():
        vehicleId = allVehicles[(allVehicles['strModelYear'] == fuelConsumption['strModelYear'])
                                & (allVehicles['strMake'] == fuelConsumption['strMake'])
                                & (allVehicles['strModel'] == fuelConsumption['strModel'])
                                & (allVehicles['strClass'] == fuelConsumption['strClass'])]
        vehiclesId.append(vehicleId.iloc[0]['id'])
    return vehiclesId


def relateVehicleTableToFuelConsumption(fuelConsumptionData):
    vehiclesId = getVehiclesId(fuelConsumptionData)
    fuelConsumptionData['vehicleId'] = vehiclesId
    return fuelConsumptionData


def loadVehicleTable(file):
    conn = getConnection()
    vehicleColumnNames = ['strModelYear', 'strMake', 'strModel', 'strClass']
    vehicleData = pd.read_csv(file, usecols=vehicleColumnNames)
    vehicleData = vehicleData.drop_duplicates()
    vehicleData.to_sql('tbl_vehicle', con=conn, if_exists='replace', index=True, index_label='id')


def dropVehicleColumnsToFuelConsumption(fuelConsumptionData):
    return fuelConsumptionData.drop(fuelConsumptionData.columns[[0, 1, 2, 3]], axis=1)


def loadFuelConsumptionTable(file):
    conn = getConnection()
    vehicleColumnNames = ['strModelYear', 'strMake', 'strModel', 'strClass']
    fuelConsumptionColumnNames = ['douEngineSize_L', 'intCylinders', 'strTransmission', 'strFuelType',
                                  'douFuelConsumption_City_L-100km', 'douFuelConsumption_Hwy_L-100km',
                                  'douFuelConsumption_Comb_L-100km', 'intFuelConsumption_Comb_mpg',
                                  'intCO2Emissions_g-km', 'intCO2Rating', 'intSmogRating']
    fuelConsumptionData = pd.read_csv(file, usecols=vehicleColumnNames + fuelConsumptionColumnNames)
    fuelConsumptionData = relateVehicleTableToFuelConsumption(fuelConsumptionData)
    fuelConsumptionData = dropVehicleColumnsToFuelConsumption(fuelConsumptionData)
    fuelConsumptionData.to_sql('tbl_fuelConsumption', con=conn, if_exists='replace', index=True, index_label='id')


def findAllVehicles():
    query = """ SELECT *
                FROM "tbl_vehicle" """
    return pd.read_sql_query(query, con=getConnection())


def findFuelConsumptionRatings():
    query = """ SELECT *
                FROM "tbl_fuelConsumption" """
    print(pd.read_sql_query(query, con=getConnection()))


if __name__ == '__main__':
    fileName = "D:\\Users\\jhonf\\Documents\\Programacion\\Codigo\\Python\\ai-fuel-consumption-ratings\\src\\main" \
           "\\resoures\\MY2022FuelConsumptionRatings.csv"
    loadVehicleTable(fileName)
    loadFuelConsumptionTable(fileName)
    findFuelConsumptionRatings()
