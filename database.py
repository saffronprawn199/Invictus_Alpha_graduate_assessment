from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

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

def __init__(Orders, side, amount, product_id):
    self.side = side
    self.amount = amount
    self.product_id = product_id

class BalanceSchema(ma.Schema):
    class Meta:
        # Fields to expose
        fields = ('side', 'amount','product_id')

class Balance(db.Model):
    __tablename__ = 'balance'
    id = db.Column('balance_id',db.Integer, primary_key=True)
    currency = db.Column(db.Text)
    acc_id = db.Column(db.Text)
    balance = db.Column(db.Text)
    hold = db.Column(db.Text)
    available =db.Column(db.Text)
    time =db.Column(db.Text)
def __init__(self, currency, acc_id, balance, hold, available, time):
    self.currency = currency
    self.balance = balance
    self.acc_id = acc_id
    self.hold = hold
    self.available = available
    self.time =time

class CurrentPositions(db.Model):
    __tablename__ = 'current_position'
    id = db.Column('current_position_id',db.Integer, primary_key=True)
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
