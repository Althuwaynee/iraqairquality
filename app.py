from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')  # Your existing homepage

@app.route('/historical')
def historical():
    return render_template('historical.html')  # Our new historical view

@app.route('/data/<path:filename>')
def serve_data(filename):
    return send_from_directory('data', filename)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
