import eventlet
eventlet.monkey_patch()  # यह ट्रेडिंग WebSocket के लिए बहुत ज़रूरी है

from main import app  # यह आपकी main.py से app को लोड करेगा

if __name__ == "__main__":
    app.run()
