import cbpro
import json
import websocket
import dateutil.parser
import numpy as np
import copy
import time
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from threading import Thread


# Coin base sandbox stuff
#===================================================================================================================
coinbase_API_key = "819744d7c013532e78850c9d39fe4f4f"
pass_phrase = "es1xqkb0j7s"
coinbase_API_secret = "mBc9J7AEsbfseEnY2QZHiGel4c2UnWCFVBw6bTqE77798DEZfFYe5QltBD+fWFHltozWr4sG1rnyhsNUeanvUQ=="

url = 'https://api-public.sandbox.pro.coinbase.com'
client = cbpro.AuthenticatedClient(
    key=coinbase_API_key,
    b64secret=coinbase_API_secret,
    passphrase=pass_phrase, 
    api_url=url)
# ===================================================================================================================

# SQL alchemy stuff
# ==================================================================================================================
app = Flask(__name__)
db = SQLAlchemy(app)
ma = Marshmallow(app)

username = 'root'
password = 'wgt92OIJ'
server = '127.0.0.1'
# configuring our database uri
# note an error here
app.config["SQLALCHEMY_DATABASE_URI"] = "mysql+mysqlconnector://{}:{}@{}/crypto_bot".format(username, password, server)

# basic model
class Orders(db.Model):
    __tablename__ = 'orders'
    id = db.Column('order_id',db.Integer, primary_key=True)
    side = db.Column(db.Text)
    amount = db.Column(db.Text)
    product_id = db.Column(db.Text)

    def __init__(self, side=None, amount=None, product_id=None):
       self.side = side
       self.amount = amount
       self.product_id = product_id

class OrdersSchema(ma.Schema):
    class Meta:
        # Fields to expose
        fields = ('id','side', 'amount','product_id')

class Balance(db.Model):
    __tablename__ = 'balance'
    id = db.Column('balance_id',db.Integer, primary_key=True)
    currency = db.Column(db.Text)
    acc_id = db.Column(db.Text)
    balance = db.Column(db.Text)
    hold = db.Column(db.Text)
    available =db.Column(db.Text)
    time = db.Column(db.Text)

    def __init__(self, currency=None, acc_id=None, balance=None, hold=None, available=None, time=None):
        self.currency = currency
        self.balance = balance
        self.acc_id = acc_id
        self.hold = hold
        self.available = available
        self.time = time

class BalanceSchema(ma.Schema):
    class Meta:
        # Fields to expose
        fields = ('id','currency', 'balance','acc_id', 'hold','available','time')

class CurrentPositions(db.Model):
    __tablename__ = 'current_position'
    id = db.Column("current_position_id", db.Integer, primary_key=True)
    currency = db.Column(db.String(8), unique=True, index=True)
    acc_id = db.Column(db.Text)
    balance = db.Column(db.Text)
    hold = db.Column(db.Text)
    available =db.Column(db.Text)
    time = db.Column(db.Text)

    def __init__(self, currency=None, acc_id=None, balance=None, hold=None, available=None, time=None):
        self.currency = currency
        self.balance = balance
        self.acc_id = acc_id
        self.hold = hold
        self.available = available
        self.time = time

class CurrentPositionsSchema(ma.Schema):
    class Meta:
        # Fields to expose
        fields = ('id','currency', 'balance','acc_id', 'hold','available','time')


balance_schema = BalanceSchema()
balances_schema = BalanceSchema(many=True)
order_schema = OrdersSchema(many=True)
current_position_schema = CurrentPositionsSchema(many=True)

# ==================================================================================================================

# Flask Endpoints 
# ===================================================================================================================
@app.route("/balances", methods=["GET"])
def get_balances():
    all_balance = Balance.query.all()
    result = balances_schema.dump(all_balance)
    return jsonify(result)

@app.route("/balance/<id>", methods=["GET"])
def get_balance(id):
    one_balance = Balance.query.get(id)
    return balance_schema.jsonify(one_balance)

@app.route("/orders", methods=["GET"])
def get_orders():
    all_orders = Orders.query.all()
    result = order_schema.dump(all_orders)
    return jsonify(result)


@app.route("/current_positions", methods=["GET"])
def get_current_position():
    current_position = CurrentPositions.query.all()
    result = current_position_schema.dump(current_position)
    return jsonify(result)
    
# ==================================================================================================================

# Sell Bitcoin
# place order
# funds = bitcoin amount of x is dollars
# size = just btc 
#sell 1000 dollars of bitcoin
def market_order_sell_bitcoin():
    order_details=client.place_market_order(product_id='BTC-USD',side ='sell', funds=10)
    order = Orders(side= order_details['side'],amount=order_details['funds'], product_id=order_details['product_id'])
    db.session.add(order)
    db.session.commit()
    return order_details

# Buy Bitcoin
def market_order_buy_bitcoin():
    order_details=client.place_market_order(product_id='BTC-USD',side ='buy', funds=10)
    order = Orders(side= order_details['side'], amount=order_details['funds'],product_id=order_details['product_id'])
    db.session.add(order)
    db.session.commit()
    return order_details

# Account balance
def balance():
    acc_id=0
    accounts =client.get_accounts()
    for acc in accounts:
        currency = acc.get('currency')
        acc_id = acc.get('id')
        balance = float(acc.get('balance'))
        hold = acc.get('hold')
        available =acc.get('available')
        if balance > 0:
            print(acc)
            balance_time = time.time()
            balance = Balance(currency=acc.get("currency"), acc_id=acc.get("id"), 
                              balance=acc.get("balance"), hold=acc.get("hold"), 
                              available=acc.get("available"), time= balance_time)

            db.session.add(balance)
            db.session.commit()

            current = CurrentPositions.query.filter_by(acc_id=acc.get("id")).first()
            current.currency=acc.get("currency")
            current.balance=acc.get("balance")
            current.hold=acc.get("hold")
            current.available=acc.get("available")
            current.time=balance_time
            db.session.commit()

# websocket data
# ==================================================================================================================
minute_processed = {}
minute_candlesticks = []

current_tick = None
previous_tick = None
std_temp = None
# Varaible set to 12 minutes long
minutes_checked = 12
# Increment every 12 minutes
minute_incrementer=0

# Check market data live stream data coming in from coinbase 
socket= "wss://ws-feed-public.sandbox.pro.coinbase.com"
def on_open(ws):
    print("connection open")

    sub_message={
        "type": "subscribe",
        "channels": [
            "level2",
            "heartbeat",
            {
                "name": "ticker",
                "product_ids": [
                    "BTC-USD"
                ]
            }
        ]
    } 
    ws.send(json.dumps(sub_message))

def on_message(ws, message):
    global current_tick, previous_tick, avg_temp, std_temp, minutes_checked, minute_incrementer
    previous_tick = current_tick
    current_tick = json.loads(message)
     
    print("received message")
    # print("{} @ {}".format(current_tick['time'], current_tick['price']))

    tick_datetime_object = dateutil.parser.parse(current_tick['time'])
    tick_dt = tick_datetime_object.strftime("%m/%d/%Y %H:%M")
    # print(tick_dt)

    if tick_dt not in minute_processed:
        print('new candle stick')
        minute_processed[tick_dt] = True
        print(minute_processed)

        if len(minute_candlesticks)>0:
            minute_candlesticks[-1]['close'] = previous_tick['price']
        if len(minute_candlesticks)==0:
             avg_temp=0.0
             std_temp=0.0 
             balance()
             print(avg_temp)
             print(std_temp)
        minute_incrementer=minute_incrementer+1
        print(minute_incrementer)
        minute_candlesticks.append({
            "minute": tick_dt,
            "open": current_tick['price'],
            "high": current_tick['price'],
            "low": current_tick['price']
        })


    if len(minute_candlesticks)>0:
        current_candlestick = minute_candlesticks[-1]
        if current_tick['price'] > current_candlestick['high']:
            current_candlestick['high'] = current_tick['price']
        if current_tick['price'] < current_candlestick['low']:
            current_candlestick['low'] = current_tick['price']
        print('candlesticks')

        for candlesticks in minute_candlesticks:
            print(candlesticks)
            
        avg_12minute = []
        std_12minute = []
        if len(minute_candlesticks) > minutes_checked and minute_incrementer== minutes_checked+1:
            candlestick_12minute = copy.deepcopy(minute_candlesticks[-minutes_checked:])
            for i, candle in enumerate(candlestick_12minute):
                # print(candle)
                avg_12minute.append(np.average([float(candle['high']), float(candle['low']), float(candle['open'])]))
                
                if i==minutes_checked-1:
                    avg = np.average(avg_12minute)
                    if avg > avg_temp:
                        sell = market_order_sell_bitcoin()
                        balance()
                        json_formatted_str = json.dumps(sell, indent=2)
                        print(json_formatted_str)
                    else:
                        buy = market_order_buy_bitcoin()
                        balance()
                        json_formatted_str = json.dumps(buy, indent=2)
                        print(json_formatted_str)
                    avg_temp = avg
                    candlestick_12minute.clear()
                    minute_incrementer =0


ws = websocket.WebSocketApp(socket, \
                            on_open = on_open, \
                            on_message = on_message)

# ==================================================================================================================

def websocket_thread():
    ws.run_forever()

def flask_thread():
    app.run(host='0.0.0.0', port=5000)

if __name__ == "__main__":
    db.create_all()
    Thread(target = websocket_thread).start()
    Thread(target = flask_thread).start()
   
