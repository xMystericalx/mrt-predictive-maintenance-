import json
import time
import numpy as np
import paho.mqtt.client as mqtt
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

print("Starting AI Live Processor... Loading dataset...")

try:
    # 1. Load baseline binary
    X = np.load("X.npy")
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    if len(X.shape) == 3:
        samples, time_steps, features = X.shape
        X_flattened = X.reshape(samples, time_steps * features)
    else:
        X_flattened = X

    calibration_data = X_flattened[:1000]
    print(f"Dataset loaded! Calibrating AI model on {calibration_data.shape[0]} samples...")

    pca = PCA(n_components=20, random_state=42)
    X_compressed = pca.fit_transform(calibration_data)

    model = IsolationForest(contamination=0.02, random_state=42, n_jobs=-1)
    model.fit(X_compressed)
    print("✅ AI Engine Calibrated Successfully!")

except Exception as e:
    print(f"❌ Error during model calibration: {e}")
    exit(1)

# --- 2. INFLUXDB SETUP ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-auth-token"
INFLUX_ORG = "mrt_org"
INFLUX_BUCKET = "mrt_telemetry"

try:
    db_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = db_client.write_api(write_options=SYNCHRONOUS)
    print("✅ Connected to InfluxDB!")
except Exception as e:
    print(f"⚠️ InfluxDB connection warning: {e}")

# --- 3. MQTT SETUP & INGESTION ---
BROKER = "broker.hivemq.com"
TOPIC = "lta/mrt/train/042/telemetry"

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"🚀 Connected to MQTT Broker! Subscribed to topic: '{TOPIC}'")
        client.subscribe(TOPIC)
    else:
        print(f"❌ Failed to connect to MQTT, code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        
        train_id = payload.get("train_id", "MRT-NSL-042")
        bogie_vib = payload.get("bogie_vibration", 0.0)
        bogie_temp = payload.get("bogie_temperature", 0.0)

        # Reconstruct synthetic feature vector across feature length
        live_features = np.full((1, X_flattened.shape[1]), bogie_vib)
        live_features[0, :10] = bogie_temp

        # Model Inference
        live_compressed = pca.transform(live_features)
        prediction = model.predict(live_compressed)[0]

        # Hybrid Rule: Flag anomaly if model predicts -1 OR thresholds are crossed
        threshold_exceeded = (bogie_temp > 60.0) or (bogie_vib > 0.12)
        if prediction == -1 or threshold_exceeded:
            is_anomaly = 1
        else:
            is_anomaly = 0

        # Log to InfluxDB
        point = (
            Point("train_status")
            .tag("train_id", train_id)
            .field("bogie_temperature", float(bogie_temp))
            .field("bogie_vibration", float(bogie_vib))
            .field("is_anomaly", int(is_anomaly))
            .time(time.time_ns())
        )
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

        status_str = "🚨 ALERT (Anomaly)" if is_anomaly == 1 else "✅ NORMAL"
        print(f"[{status_str}] Train: {train_id} | Vib: {bogie_vib} | Temp: {bogie_temp}°C")

    except Exception as e:
        print(f"Error processing incoming message: {e}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

print(f"Connecting to MQTT Broker at {BROKER}...")
client.connect(BROKER, 1883, 60)
client.loop_forever()