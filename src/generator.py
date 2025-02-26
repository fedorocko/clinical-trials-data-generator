import random
import pandas as pd
from datetime import timedelta, datetime
import snowflake.connector

# Generate random names
FIRST_NAMES_MALE = ["John", "Bob", "Charlie", "David", "Michael", "James", "Daniel", "Matthew", "Ethan", "Alexander"]
FIRST_NAMES_FEMALE = ["Jane", "Alice", "Emily", "Laura", "Sarah", "Emma", "Sophia", "Olivia", "Isabella", "Mia"]
LAST_NAMES = ["Doe", "Smith", "Brown", "White", "Black", "Green", "Blue", "Adams", "Scott", "Johnson", "Miller", "Wilson", "Harris", "Clark", "Lewis", "Walker", "Hall", "Allen", "Young", "King"]

# Predefined viral load progression scenarios
VIRAL_LOAD_SCENARIOS = {
    "Fast recovery": [1000, 250, 60, 0, 0, 0, 0],
    "Standard recovery": [1000, 400, 160, 60, 25, 0, 0],
    "Slow recovery": [1000, 750, 600, 400, 250, 120, 0],
    "Strong reinfection": [1000, 600, 360, 600, 800, 1000, 1000],
    "Weak reinfection": [1000, 600, 360, 250, 0, 360, 400],
    "Standard reinfection": [1000, 750, 300, 450, 550, 600, 600],
    "No Effect": [1000, 900, 950, 800, 900, 1000, 900],
    "Worsen conditions": [1000, 1000, 800, 1200, 1250, 1100, 1200],
    "Fast semi-recovery": [1000, 500, 400, 250, 125, 100, 100],
    "Standard semi-recovery": [1000, 700, 500, 350, 300, 250, 250],
    "Slow semi-recovery": [1000, 800, 650, 500, 450, 300, 250]
}

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "user": "fedorocko",
    "password": "XkPHL.CrmWEv,a*cETW0",
    "account": "fy34660.eu-central-1",
    "warehouse": "COMPUTE_WH",
    "database": "TRIALS",
    "schema": "LOAD",
    "role": "ACCOUNTADMIN"
}

# Generate unique patients with specific probabilities for conditions
def generate_patients(num_patients=200):
    patients = []
    for i in range(num_patients):
        sex = random.choice(["Male", "Female"])
        first_name = random.choice(FIRST_NAMES_MALE if sex == "Male" else FIRST_NAMES_FEMALE)
        last_name = random.choice(LAST_NAMES)
        obesity = random.choices([True, False], weights=[30, 70])[0]
        weight = random.randint(80, 120) if obesity else random.randint(50, 100)
        patients.append({
            "patient_id": i + 1,
            "name": f"{first_name} {last_name}",
            "sex": sex,
            "weight": float(weight),
            "height": float(random.randint(150, 200)),
            "obesity": obesity,
            "diabetes": random.choices([True, False], weights=[25, 75])[0],
            "high_blood_pressure": random.choices([True, False], weights=[40, 60])[0],
            "cancer": random.choices([True, False], weights=[10, 90])[0],
            "HIV": random.choices([True, False], weights=[5, 95])[0]
        })
    return pd.DataFrame(patients)

# Generate unique treatment protocols
def generate_protocols(num_protocols=5):
    seen_protocols = set()
    protocols = []
    for i in range(num_protocols):
        while True:
            protocol = (
                random.choice([2.5, 5, 7.5, 10]),
                random.choice([1, 2, 3, 4]),
                random.choice([3, 5, 7, 10])
            )
            if protocol not in seen_protocols:
                seen_protocols.add(protocol)
                protocols.append({
                    "protocol_id": len(protocols) + 1,
                    "dosage": float(protocol[0]),
                    "frequency": int(protocol[1]),
                    "duration": int(protocol[2]),
                    "protocol_name": f"{protocol[0]}mg-{protocol[1]}x-{protocol[2]}d"
                })
                break
    return pd.DataFrame(protocols)


# Generate observations for 100 to 150 patients per protocol
def generate_observations(patients, protocols):
    observations = []
    start_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365 - 7))  # Random start date in 2024
    for _, protocol in protocols.iterrows():
        num_patients = random.randint(120, 180)
        selected_patients = patients.sample(n=num_patients, replace=False)
        for _, patient in selected_patients.iterrows():
            scenario_name = random.choice(list(VIRAL_LOAD_SCENARIOS.keys()))
            base_viral_loads = VIRAL_LOAD_SCENARIOS[scenario_name]
            viral_loads = [max(0, int(v * random.uniform(0.8, 1.2))) for v in base_viral_loads]
            for day in range(7):
                systolic, diastolic = random.randint(90, 140), random.randint(60, 90)
                observations.append({
                    "patient_id": patient["patient_id"],
                    "protocol_id": protocol["protocol_id"],
                    "date": (start_date + timedelta(days=day)).strftime('%Y-%m-%d'),
                    "day": day + 1,
                    "viral_load": viral_loads[day],
                    "body_temperature": round(random.uniform(36.0, 39.0), 1),
                    "systolic_pressure": systolic,
                    "diastolic_pressure": diastolic,
                    "headache": random.choices([True, False], weights=[20, 80])[0],
                    "vomiting": random.choices([True, False], weights=[10, 90])[0]
                })
    return pd.DataFrame(observations)

patients_df = generate_patients(400)
protocols_df = generate_protocols(10)
observations_df = generate_observations(patients_df, protocols_df)

patients_df.to_csv("data/patients.csv", index=False)
protocols_df.to_csv("data/protocols.csv", index=False)
observations_df.to_csv("data/observations.csv", index=False)

# Upload data to Snowflake
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS patients")
cursor.execute("CREATE TABLE patients (patient_id INTEGER PRIMARY KEY, name STRING, sex STRING, weight FLOAT, height FLOAT, obesity BOOLEAN, diabetes BOOLEAN, high_blood_pressure BOOLEAN, cancer BOOLEAN, HIV BOOLEAN)")

cursor.execute("DROP TABLE IF EXISTS protocols")
cursor.execute("CREATE TABLE protocols (protocol_id INTEGER PRIMARY KEY, dosage FLOAT, frequency INTEGER, duration INTEGER, protocol_name STRING)")

cursor.execute("DROP TABLE IF EXISTS observations")
cursor.execute("CREATE TABLE observations (patient_id INTEGER, protocol_id INTEGER, date DATE, day INTEGER, viral_load INTEGER, body_temperature FLOAT, systolic_pressure INTEGER, diastolic_pressure INTEGER, headache BOOLEAN, vomiting BOOLEAN)")

for table, df in zip(["patients", "protocols", "observations"], [patients_df, protocols_df, observations_df]):
    for _, row in df.iterrows():
        cursor.execute(f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(row))})", tuple(row))

cursor.close()
conn.close()

print("Data generation and processing completed.")
