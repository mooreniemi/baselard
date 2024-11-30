from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/process", methods=["POST"])
def process():
    data = request.json
    features = data.get("features", [])

    if not features:
        return jsonify({"error": "Features array is empty"}), 400

    processed_data = [x + 1 for x in features]  # Example processing
    print(processed_data)
    return jsonify({"processed_features": processed_data})


if __name__ == "__main__":
    # Run the server on port 4000
    app.run(host="127.0.0.1", port=4000)
