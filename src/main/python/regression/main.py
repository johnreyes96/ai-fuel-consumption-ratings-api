from sqlalchemy import create_engine
import pandas as pd
from flask import Flask, jsonify
from scipy import stats


app = Flask(__name__)


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
    vehicleData.to_sql('tbl_vehicle', con=conn, if_exists='replace', index_label='id')


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
    fuelConsumptionData.to_sql('tbl_fuelConsumption', con=conn, if_exists='replace', index_label='id')


def dropConstraintsToTables():
    conn = getConnection()
    conn.execute("ALTER TABLE \"tbl_fuelConsumption\" DROP CONSTRAINT IF EXISTS fk_id_vehicle_fuel_consumption")


def addConstraintsToTables():
    conn = getConnection()
    conn.execute("ALTER TABLE tbl_vehicle ADD PRIMARY KEY (id)")
    conn.execute("ALTER TABLE \"tbl_fuelConsumption\" ADD PRIMARY KEY (id)")
    conn.execute("ALTER TABLE \"tbl_fuelConsumption\" ADD CONSTRAINT fk_id_vehicle_fuel_consumption"
                 " FOREIGN KEY (\"vehicleId\")"
                 " REFERENCES tbl_vehicle (id)")


def findAllVehicles():
    query = """ SELECT *
                FROM tbl_vehicle """
    return pd.read_sql_query(query, con=getConnection())


def findAllDataSet():
    query = """ SELECT ve."strModelYear", ve."strMake", ve."strModel", ve."strClass", fc."douEngineSize_L",
                fc."intCylinders", fc."strTransmission", fc."strFuelType", fc."douFuelConsumption_City_L-100km",
                fc."douFuelConsumption_Hwy_L-100km", fc."douFuelConsumption_Comb_L-100km",
                fc."intFuelConsumption_Comb_mpg", fc."intCO2Emissions_g-km", fc."intCO2Rating", fc."intSmogRating"
                FROM "tbl_fuelConsumption" fc
                INNER JOIN tbl_vehicle ve ON ve.id = fc."vehicleId" """
    pd.set_option('display.max_columns', 15)
    return pd.read_sql_query(query, con=getConnection())


def findFuelConsumptionRatings():
    query = """ SELECT ve."strModelYear", ve."strMake", ve."strModel", ve."strClass", fc."douEngineSize_L",
                fc."intCylinders", fc."strTransmission", fc."strFuelType", fc."douFuelConsumption_City_L-100km",
                fc."intCO2Emissions_g-km", fc."intCO2Rating"
                FROM "tbl_fuelConsumption" fc
                INNER JOIN tbl_vehicle ve ON ve.id = fc."vehicleId" """
    pd.set_option('display.max_columns', 15)
    return pd.read_sql_query(query, con=getConnection())


@app.route('/populate-db', methods=['GET'])
def populateDB():
    fileName = "D:\\Users\\jhonf\\Documents\\Programacion\\Codigo\\Python\\ai-fuel-consumption-ratings\\src\\main" \
           "\\resoures\\MY2022FuelConsumptionRatings.csv"
    dropConstraintsToTables()
    loadVehicleTable(fileName)
    loadFuelConsumptionTable(fileName)
    addConstraintsToTables()
    dataFrame = findFuelConsumptionRatings()
    response = {
        'data': dataFrame.to_json(orient='index')
    }
    return jsonify(response)


@app.route('/person-stats', methods=['GET'])
def getPersonRStats():
    dataFrame = findAllDataSet()
    pearsonCoef, p_value = stats.pearsonr(dataFrame['intCO2Emissions_g-km'], dataFrame['douFuelConsumption_Comb_L-100km'])
    stat1 = "The Pearson Correlation Coefficient of CO2 Emissions(g/km) vs Fuel Consumption(Comb (L/100 km)) is" + str(pearsonCoef) + " with a P-value of P =" + str(p_value)
    pearsonCoef, p_value = stats.pearsonr(dataFrame['intCO2Rating'], dataFrame['douFuelConsumption_Comb_L-100km'])
    stat2 = "The Pearson Correlation Coefficient of CO2 Rating vs Fuel Consumption(Comb (L/100 km)) is" + str(pearsonCoef) + " with a P-value of P =" + str(p_value)
    pearsonCoef, p_value = stats.pearsonr(dataFrame['intSmogRating'], dataFrame['douFuelConsumption_Comb_L-100km'])
    stat3 = "The Pearson Correlation Coefficient of Smog Rating vs Fuel Consumption(Comb (L/100 km)) is" + str(pearsonCoef) + " with a P-value of P =" + str(p_value)
    pearsonCoef, p_value = stats.pearsonr(dataFrame['intCylinders'], dataFrame['douFuelConsumption_Comb_L-100km'])
    stat4 = "The Pearson Correlation Coefficient of Cylinders vs  Combined City & Highway Fuel Consumption is" + str(pearsonCoef) + " with a P-value of P =" + str(p_value)
    pearsonCoef, p_value = stats.pearsonr(dataFrame['douEngineSize_L'], dataFrame['douFuelConsumption_Comb_L-100km'])
    stat5 = "The Pearson Correlation Coefficient Engine Size vs Combined City & Highway Fuel Consumption is" + str(pearsonCoef) + " with a P-value of P =" + str(p_value)
    response = {
        'CO2 Emissions(g/km)': stat1,
        'CO2 Rating': stat2,
        'Smog Rating': stat3,
        'Cylinders': stat4,
        'Engine Size': stat5
    }
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=8090)
