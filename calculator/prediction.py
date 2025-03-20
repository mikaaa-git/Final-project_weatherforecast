import os
import requests
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer

# Create session
spark = SparkSession.builder.appName("PM2.5 Calculator").getOrCreate()

# Load data
url = "https://raw.githubusercontent.com/SUTAMPU/air-quality-calculator/refs/heads/main/sample_data.csv"
local_file = "sample_data.csv"

response = requests.get(url)
if response.status_code == 200:
  with open(local_file, "wb") as f:
    f.write(response.content)
  df = spark.read.csv(local_file, inferSchema=True, header=True)
else:
  print("Failed to download the file")
  spark.stop()
# ============================================================================================================
# Model Training
# Clean missing data
final_df = df.filter(~((df['Vehicle type'] != 'Skytrain') & (df['Vehicle emission'] == 0)))

# Convert category data into numbers
vehicle_indexer = StringIndexer(inputCol='Vehicle type', outputCol='VehicleIndex')
fuel_indexer = StringIndexer(inputCol='Fuel', outputCol='FuelIndex')

# Combine features
assembler = VectorAssembler(
  inputCols=['VehicleIndex','Age','FuelIndex','Distance'],
  outputCol='features'
  )

# Create random forest classifier
rf_emission = RandomForestRegressor(labelCol='Emission', featuresCol='features')

# Pipeline configuration
pipeline = Pipeline(stages = [vehicle_indexer, fuel_indexer, assembler, rf_emission])

# Train/Test split
train_data, test_data = final_df.randomSplit([0.9,0.1])

# Fit model
model = pipeline.fit(train_data)
results = model.transform(test_data)

from pyspark.ml.evaluation import RegressionEvaluator
# Evaluation
eval = RegressionEvaluator(predictionCol='prediction',labelCol='Emission')
eval.evaluate(results)

# ============================================================================================================
# User input
print("Let's calculate the PM2.5 emissions for your journey based on your vehicle choice! 🤔💭")
print("============================================================================================================")
vehicle = input("Please select your vehicle type from the following: Car, Pickup, Motorcycle, Bus, or Skytrain\n - ")

if vehicle == "Skytrain":
  age = 1
  fuel = "Gasoline"
  distance = float(input("How many kilometers are you planning to travel today?\n - "))
  province = "Bangkok"
else:
  age = float(input("How old is your vehicle?\n - "))
  fuel = input("What type of fuel does it use? (Gasoline, B7, B20, CNG, LPG)\n - ")
  distance = float(input("How many kilometers are you planning to travel today?\n - "))
  province = input("In which province will you be traveling in?\n - ")
# ============================================================================================================
# Calculator
# Prepare data for selected vehicle types
input_df = spark.createDataFrame(
    [(vehicle, age, fuel, distance)],
    ['Vehicle type', 'Age', 'Fuel', 'Distance']
)
prediction = model.transform(input_df)
predicted_emission = prediction.select('Prediction').first()[0]

# Output
print("============================================================================================================")
print(f"📌 Your predicted PM2.5 emissions is around {predicted_emission:.2f} mg/person!")
# ============================================================================================================
# Recommender
print("\nNow, let's compare your vehicle's emissions with other modes of transportation 🚗💨")
print(f"(Assuming the vehicle is {age} years old with {fuel.lower()} fuel type)\n")

# Show only not chosen vehicle
vehicle_types = ["Car", "Pickup", "Motorcycle", "Bus", "Skytrain"]
vehicle_types.remove(vehicle)

# Prepare data for other vehicle types
results = []
for types in vehicle_types:
  compare_df = spark.createDataFrame(
  [(types, age, fuel, distance)],
  ['Vehicle type', 'Age', 'Fuel', 'Distance']
  )
  compare_prediction = model.transform(compare_df)
  compare_predicted_emission = compare_prediction.select('Prediction').first()[0]
  results.append(((compare_predicted_emission - predicted_emission) / predicted_emission) * 100)

# Output
rec = []
for i, num in enumerate(results):
  if num > 0:
    print(f"Compared to a {vehicle_types[i].lower()}, your emissions are {results[i]:.0f}% lower ✅")
  else:
    rec.append(vehicle_types[i].lower())
    print(f"Compared to a {vehicle_types[i].lower()}, your emissions are {abs(results[i]):.0f}% higher ⛔")

if len(rec) == 1:
  print(f"📌 You can reduce your emission further by switching to {rec[0]}!")
elif len(rec) == 2:
  print(f"📌 You can reduce your emission further by switching to {rec[0]} or {rec[1]}!")
elif len(rec) == 3:
  print(f"📌 You can reduce your emission further by switching to {rec[0]}, {rec[1]}, or {rec[2]}!")
elif len(rec) == 4:
  print(f"📌 You can reduce your emission further by switching to {rec[0]}, {rec[1]}, {rec[2]}, or {rec[3]}!")
else:
  print("Great job!🤩 You are currently using the best form of transportation!")

spark.stop()