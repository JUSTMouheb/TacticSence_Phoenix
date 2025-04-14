from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/test', methods=['GET'])
def test():
    return jsonify({"status": "ok", "message": "Test endpoint working"})

@app.route('/api/test/<param>', methods=['GET'])
def test_param(param):
    return jsonify({"status": "ok", "param": param})

if __name__ == '__main__':
    print("Starting test API on http://localhost:5001")
    app.run(host='0.0.0.0', port=5001, debug=True)