import time
from pusher import Pusher

# Setup Pusher with Soketi Host
pusher_client = Pusher(
    app_id="1", 
    key="myt_key", 
    secret="myt_secret", 
    host="myt-market-socket.onrender.com",
    port=443,
    ssl=True
)

# 1 lakh users ke liye tracking logic
last_push_times = {}

def broadcast_live_price(token, price):
    now = time.time()
    
    # 1.5 second ka gap (Throttling)
    # Isse server par load 80% kam ho jayega aur price live rahegi
    if token not in last_push_times or (now - last_push_times[token]) >= 1.5:
        try:
            pusher_client.trigger('market-channel', 'price-update', {
                't': str(token), 
                'p': str(price)
            })
            last_push_times[token] = now
            print(f"✅ Sent: {token} -> {price}")
        except Exception as e:
            print(f"❌ Socket Busy: {e}")

# Example Use:
# broadcast_live_price("26009", "572.45")
