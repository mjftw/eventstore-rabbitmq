from flask import Flask, render_template, request, jsonify
import requests

app = Flask(__name__)

# FastAPI URL
API_URL = "http://localhost:8000/replay"

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        event_type = request.form.get("event_type")
        target_queue = request.form.get("target_queue")

        # Send replay request to FastAPI
        if event_type and target_queue:
            payload = {"event_type": event_type, "target_queue": target_queue}
            response = requests.post(API_URL, json=payload)

            if response.status_code == 200:
                return render_template("index.html", success=True, message="Replay triggered successfully!")
            else:
                return render_template("index.html", error=True, message=f"Failed to trigger replay.\n{response.text}")

    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
