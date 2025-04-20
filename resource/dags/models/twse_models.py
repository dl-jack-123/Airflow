from sqlalchemy import Column, Date, String, Numeric, BigInteger, TIMESTAMP, PrimaryKeyConstraint, Integer, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TwseIndex(Base):
    __tablename__ = 'twse_index'
    
    date = Column(Date, primary_key=True)
    index_name = Column(String(50), primary_key=True)
    closing_price = Column(Numeric(10, 2))
    change_value = Column(Numeric(10, 2))
    change_percent = Column(Numeric(10, 2))
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    created_at = Column(TIMESTAMP, server_default='CURRENT_TIMESTAMP')

class DailyQuote(Base):
    __tablename__ = 'dailyquote'
    
    date = Column(Date, primary_key=True)
    stock_code = Column(String(10), primary_key=True)  # 證券代號
    stock_name = Column(String(50))  # 證券名稱
    trade_volume = Column(BigInteger)  # 成交股數
    trade_count = Column(Integer)  # 成交筆數
    trade_amount = Column(BigInteger)  # 成交金額
    open_price = Column(Numeric(10, 2))  # 開盤價
    high_price = Column(Numeric(10, 2))  # 最高價
    low_price = Column(Numeric(10, 2))  # 最低價
    close_price = Column(Numeric(10, 2))  # 收盤價
    change_value = Column(Numeric(10, 2))  # 漲跌價差（正數表示上漲，負數表示下跌）
    last_bid_price = Column(Numeric(10, 2))  # 最後揭示買價
    last_bid_volume = Column(Integer)  # 最後揭示買量
    last_ask_price = Column(Numeric(10, 2))  # 最後揭示賣價
    last_ask_volume = Column(Integer)  # 最後揭示賣量
    pe_ratio = Column(Float)  # 本益比

    __table_args__ = (
        PrimaryKeyConstraint('date', 'stock_code'),
    ) 

class TradingDate(Base):
    __tablename__ = 'trading_date'
    
    date = Column(Date, primary_key=True)  # 日期
    name = Column(String(100))  # 名稱
    is_holiday = Column(Boolean)  # 是否為休假日
    description = Column(String(500))  # 說明
    country = Column(String(50))  # 國別

class ShortSelling(Base):
    __tablename__ = 'short_selling'
    
    date = Column(Date, primary_key=True)  # 日期
    stock_code = Column(String(10), primary_key=True)  # 證券代號
    stock_name = Column(String(50))  # 證券名稱
    margin_selling_volume = Column(Integer)  # 融券賣出成交股數
    margin_selling_amount = Column(BigInteger)  # 融券賣出成交金額
    securities_borrowing_volume = Column(Integer)  # 借券賣出成交股數
    securities_borrowing_amount = Column(BigInteger)  # 借券賣出成交金額
    total_volume = Column(Integer)  # 總成交股數
    total_amount = Column(BigInteger)  # 總成交金額
    margin_selling_ratio = Column(Numeric(10, 2))  # 融券賣出成交股數佔總成交股數比率
    securities_borrowing_ratio = Column(Numeric(10, 2))  # 借券賣出成交股數佔總成交股數比率

    __table_args__ = (
        PrimaryKeyConstraint('date', 'stock_code'),
    )

class AfterHoursTrading(Base):
    __tablename__ = 'after_hours_trading'
    
    date = Column(Date, primary_key=True)  # 日期
    stock_code = Column(String(10), primary_key=True)  # 證券代號
    stock_name = Column(String(50))  # 證券名稱
    trade_volume = Column(Integer)  # 成交數量
    trade_count = Column(Integer)  # 成交筆數
    trade_amount = Column(BigInteger)  # 成交金額
    trade_price = Column(Numeric(10, 2))  # 成交價
    last_bid_volume = Column(Integer)  # 最後揭示買量
    last_ask_volume = Column(Integer)  # 最後揭示賣量

    __table_args__ = (
        PrimaryKeyConstraint('date', 'stock_code'),
    )