from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

@app.route("/api/battery_status", methods=["POST"])
def receive_battery_status():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data received"}), 400

    print("Diterima:")
    print(f"{data['timestamp']} |{data['voltage']} V | {data['cycle']} | {data['roc']} | Status: {data['status']}")
    
    # Simpan ke log/database jika perlu
    return jsonify({"message": "Battery status received"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
