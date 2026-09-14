import time
import random
import json
import paho.mqtt.client as mqtt

# --- MQTT SETUP ---
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "lta/mrt/train/042/telemetry"

# Initialize MQTT Client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

print("Connecting to MQTT Broker...")
client.connect(BROKER, PORT, 60)
client.loop_start()  # Run network loop in background

def generate_train_data(step):
    # Base healthy values
    bogie_vibration = random.uniform(0.01, 0.03)
    bogie_temp = random.uniform(38.0, 42.0)
    relay_voltage = random.uniform(23.8, 24.2)
    relay_current = random.uniform(1.9, 2.1)
    
    # Introduce failure state after step 50
    if step > 50:
        bogie_vibration += random.uniform(0.05, 0.15) 
        bogie_temp += random.uniform(15.0, 25.0)
        relay_current -= random.uniform(0.4, 0.8)

    payload = {
        "train_id": "MRT-NSL-042",
        "timestamp": time.time(),
        "bogie_vibration": round(bogie_vibration, 4),
        "bogie_temperature": round(bogie_temp, 2),
        "relay_voltage": round(relay_voltage, 2),
        "relay_current": round(relay_current, 2)
    }
    return payload

# --- MAIN LOOP ---
try:
    print("Starting Edge Sensor Transmission...")
    step = 1
    while True:
        telemetry = generate_train_data(step)
        json_payload = json.dumps(telemetry)
        
        # Stream over MQTT
        client.publish(TOPIC, json_payload)
        print(f"[Step {step}] Sent live telemetry to topic '{TOPIC}'")
        
        step += 1
        time.sleep(1)  # Broadcast every 1 second
except KeyboardInterrupt:
    print("Stopping simulator...")
finally:
    client.loop_stop()
    client.disconnect()