# Air Quality Calculator
This project uses existing data from the __[PM2.5 Footprint Calculator v1.01](https://www.eg.mahidol.ac.th/dept/egce/pmfootprint/files/1_01/PM2.5%20Footprint%20Calculator%20Report%20v1.01_081221.pdf)__, developed by Mahidol University, Thailand.

The aim of this project is to raise awareness about individuals' PM2.5 emissions and encourage individuals to shift to greener modes of transportation. This can be achieved by integrating into a real-time AQI application already available. The following is an example interface of what our calculator would look like:
<br/><br/>
![example-interface](https://github.com/SUTAMPU/air-quality-calculator/blob/main/example.gif?raw=true)

# About the calculator
The passenger transport modes considered are:
- Cars, pickups, public buses, and motorcycles — emissions depend on engine type and fuel type (Gasoline, B7, B20, LPG, CNG).
- Skytrain — emissions from electricity consumption.

The calculator categorises emissions into two types:
#### Tank-to-Wheel (TtW) —> considers only emissions from fuel combustion.
> TtW Emission = Vehicle Emission Factor × Activity Data
#### Well-to-Wheel (WtW) —> considers both fuel combustion emissions and energy production emissions.
> WtW Emission = (Vehicle Emission Factor×Activity Data) + (Fuel Production Emission Factor×Activity Data)
...Where the both types will be available in _calculation.ipynb_ but only WtW emission is available in _prediction.ipynb_.

There are two types of calculation available. Both types utilises PySpark to account for large-scale data processing for future developments (e.g., recommending best routes based on the current AQI):
### Direct Calculation (_calculation.py_)
This type filters out DataFrame based on inputs to find the _vehicle emission_ and _energy production_ rate. The values are then used to calculate the PM2.5 emissions.
```
ttw = (vehicle_emission*distance)*pow(10, 6)
wtw = ((vehicle_emission*distance)+(energy_production*distance))*pow(10, 6)
```
### ML Prediction (_prediction.py_)
This type predicts the PM2.5 emission rate without _vehicle emission_ and _energy production_ rate using RandomForestRegressor.
```
prediction = model.transform(input_df)
predicted_emission = prediction.select('Prediction').first()[0]
```
