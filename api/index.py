from flask import Flask, request, jsonify

app = Flask(__name__)

# Ye temporary memory (Locker) hai
data_store = {}

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=['GET', 'POST'])
def catch_all(path):
    if request.method == 'POST':
        try:
            # Render se data lene ke liye
            incoming_data = request.get_json()
            token = str(incoming_data.get('token'))
            price = str(incoming_data.get('price'))
            data_store[token] = price
            return "OK", 200
        except Exception as e:
            return f"Error: {str(e)}", 400
    else:
        # APK ko price dene ke liye (e.g., /api?token=3045)
        token = request.args.get('token')
        if not token:
            return "Vercel is Live! Send a token to get price.", 200
        return str(data_store.get(token, "0.0")), 200

# Ye line Vercel ke liye zaroori hai
app = app
