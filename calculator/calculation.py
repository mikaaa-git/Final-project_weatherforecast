import os
import requests
from pyspark.sql import SparkSession

# Create session
spark = SparkSession.builder.appName("PM2.5 Calculator").getOrCreate()

# Load data
url = "https://raw.githubusercontent.com/SUTAMPU/air-quality-calculator/refs/heads/main/emission_inventory.csv"
local_file = "emission_inventory.csv"

response = requests.get(url)
if response.status_code == 200:
  with open(local_file, "wb") as f:
    f.write(response.content)
  df = spark.read.csv(local_file, inferSchema=True, header=True)
else:
  print("Failed to download the file")
  spark.stop()
# ============================================================================================================
# Data cleaning
# Shorten vehicle type names
shorten = {
    "Passenger Car": "Car",
    "Passenger Pick up": "Pickup",
    "Public Bus": "Bus",
}
# Replace values from dictionary keys
final_df = df.replace(shorten, subset=["Vehicle type"])
vehicle_values = final_df.select("Vehicle type").distinct()

# Set age range
def age_range(age):
    if age < 1:
        return "<1"
    elif age >= 11 and age <= 15:
        return "11 to 15"
    elif age >= 16 and age <= 20:
        return "16 to 20"
    elif age > 20:
        return ">20"
    else:
        return age
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
if vehicle == "Skytrain":
  filtered_df = final_df.filter(
      (final_df['Vehicle type'] == vehicle)
  ).collect()
else:
  filtered_df = final_df.filter(
      (final_df['Province'] == province) &
      (final_df['Vehicle type'] == vehicle) &
      (final_df['Age'] == age_range(age)) &
      (final_df['Fuel'] == fuel)
  ).collect()

# Access the values by column names as dictionary keys
vehicle_emission = filtered_df[0].asDict()['Vehicle emission']
energy_production = filtered_df[0].asDict()['Energy production']

ttw = (vehicle_emission * distance) * pow(10, 6)
wtw = ((vehicle_emission  *distance) + (energy_production*distance)) * pow(10, 6)

# Output
print("============================================================================================================")
print("📌 Here's the calculated PM2.5 emissions for your selected vehicle:")
print(f"Tank-to-Wheel (TtW): {ttw:.2f} mg/person!")
print(f"Well-to-Wheel (WtW): {wtw:.2f} mg/person!")
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
  if types == "Skytrain":
    filtered_df = final_df.filter(
        (final_df['Vehicle type'] == types)
    ).collect()
  else:
    filtered_df = final_df.filter(
        (final_df['Province'] == province) &
        (final_df['Vehicle type'] == types) &
        (final_df['Age'] == age_range(age)) &
        (final_df['Fuel'] == fuel)
    ).collect()

  # Access the values by column names as dictionary keys
  compare_vehicle = filtered_df[0].asDict()['Vehicle emission']
  compare_energy = filtered_df[0].asDict()['Energy production']

  compare_wtw = ((compare_vehicle * distance) + (compare_energy * distance)) * pow(10, 6)
  results.append(((compare_wtw - wtw) / wtw) * 100)

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