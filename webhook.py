from flask import Flask, request
import os

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("🔔 New event received:")
    print(data)
    return "ok", 200

@app.route("/")
def home():
    return "Bot is alive!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
