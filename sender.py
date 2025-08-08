import pandas as pd
import yaml
import os
import requests

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def process_battery_hourly(config_path="config.yaml", flask_url="http://192.168.12.49:5000/api/battery_status"):
    config = load_config(config_path)
    logger_id = str(config["logger_id"])
    thresholds = config["battery_voltage_threshold"]

    # C:/Users/fadel/Downloads/BackUpSyarif/mqtt/10300/2025-08-07.csv
    # filepath = f"C:/Users/fadel/Downloads/BackUpSyarif/mqtt/{logger_id}"
    filepath = f"/home/weston/riset/mqtt/data/{logger_id}"
    files = sorted([f for f in os.listdir(filepath) if f.endswith(".csv")])
    if not files:
        print("❌ Tidak ada file .csv ditemukan di direktori:", filepath)
        return

    latest_file = os.path.join(filepath, files[-1])

    df = pd.read_csv(latest_file)
    # Gabungkan kolom tanggal + jam menjadi timestamp
    df["timestamp"] = pd.to_datetime(df["tanggal"] + " " + df["jam"], errors="coerce")
    # Rename sensor15 menjadi voltage
    df["voltage"] = pd.to_numeric(df["sensor15"], errors="coerce")
    # Pilih kolom yang diperlukan dan set index
    df_battery = df[["timestamp", "voltage"]].dropna()
    df_battery = df_battery.set_index("timestamp")

    resampled_rows = []
    for time, group in df_battery.resample("h"):
        if group.empty:
            continue

        hour = time.hour
        cycle = "Charging" if 6 <= hour <= 17 else "Discharging"
        voltage = group["voltage"].max() if cycle == "Charging" else group["voltage"].min()

        resampled_rows.append({
            "timestamp": time,
            "voltage_resampled": voltage,
            "cycle": cycle
        })

    df_hourly = pd.DataFrame(resampled_rows).set_index("timestamp")
    df_hourly["roc_percentage"] = df_hourly["voltage_resampled"].diff().fillna(0.00)

    def classify(row):
        v = row["voltage_resampled"]
        roc = row["roc_percentage"]
        cycle = row["cycle"]

        if pd.isna(v): return "Unknown"

        if cycle == "Charging":
            if v > thresholds["over_voltage"]:
                return "Over Voltage"
            elif v >= thresholds["normal_min"]:
                if roc > 2.2:
                    return "Abnormal"
                return "Normal"
            elif v <= thresholds["low_max"]:
                return "Low Voltage"
            else:
                return "Unknown"

        elif cycle == "Discharging":
            if v <= thresholds["under_voltage"]:
                return "Under Voltage"
            elif v <= thresholds["low_max"]:
                return "Low Voltage"
            elif roc > 2.5:
                return "Abnormal"
            return "Normal"

        return "Unknown"

    df_hourly["status"] = df_hourly.apply(classify, axis=1)
    df_hourly.reset_index(inplace=True)

    for _, row in df_hourly.iterrows():
        payload = {
            "logger_id": logger_id,
            "timestamp": row["timestamp"].isoformat(),
            "voltage": round(row["voltage_resampled"], 2),
            "cycle": row["cycle"],
            "roc": round(row["roc_percentage"], 3),
            "status": row["status"]
        }

        try:
            response = requests.post(flask_url, json=payload, timeout=5)
            print(f"✅ POST {payload['timestamp']} => {response.status_code}")
        except requests.RequestException as e:
            print(f"❌ Gagal kirim data untuk {payload['timestamp']}: {e}")

    return df_hourly

if __name__ == "__main__":
    process_battery_hourly()
