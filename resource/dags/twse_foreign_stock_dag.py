from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import insert
import logging
from typing import List, Dict
import httpx
from models.twse_models import Base, DailyQuote
from util import (
    TAIPEI_TZ,
    DataConverter,
    get_db_engine,
    create_trading_day_branch
)
import holidays
import pytz

logger = logging.getLogger(__name__)

# 設定台北時區
TAIPEI_TZ = pytz.timezone('Asia/Taipei')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# 設定台灣的假日
tw_holidays = holidays.TW()


@task(task_id='create_tables')
def create_tables():
    try:
        engine = get_db_engine()
        Base.metadata.create_all(engine)
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

@task(task_id='scrape_foreign_stock_data')
def scrape_foreign_stock(**context) -> List[Dict]:
    try:
        # 從 Airflow context 獲取執行時間
        execution_date = context['execution_date']
        # 使用 execution_date 的日期，而不是當前時間
        date_str = execution_date.strftime("%Y%m%d")
        
        logger.info(f"Processing data for date: {date_str}")
        
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_FIRST"
        params = {
            "date": date_str,
            "response": "json",
            "_": int(execution_date.timestamp() * 1000)  # 使用時間戳作為 _ 參數
        }
        
        with httpx.Client() as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data["stat"] != "OK":
                raise ValueError(f"API returned error status: {data['stat']}")
                
            quote_data = []
            
            # 處理第一上市外國股票資料
            for row in data["data"]:
                try:
                    # 解析漲跌符號和價差
                    change_symbol = row[9]
                    change_value = DataConverter.convert_value(row[10])
                    if change_value is not None:
                        if "<p style= color:red>+</p>" in change_symbol:
                            change_value = abs(change_value)
                        elif "<p style= color:green>-</p>" in change_symbol:
                            change_value = -abs(change_value)
                        else:
                            change_value = 0.0

                    quote_data.append({
                        "date": execution_date.date().isoformat(),
                        "stock_code": row[0],
                        "stock_name": row[1],
                        "trade_volume": DataConverter.convert_value(row[2], is_int=True),
                        "trade_count": DataConverter.convert_value(row[3], is_int=True),
                        "trade_amount": DataConverter.convert_value(row[4], is_int=True),
                        "open_price": DataConverter.convert_value(row[5]),
                        "high_price": DataConverter.convert_value(row[6]),
                        "low_price": DataConverter.convert_value(row[7]),
                        "close_price": DataConverter.convert_value(row[8]),
                        "change_value": change_value,  # 合併後的漲跌價差
                        "last_bid_price": DataConverter.convert_value(row[11]),
                        "last_bid_volume": DataConverter.convert_value(row[12], is_int=True),
                        "last_ask_price": DataConverter.convert_value(row[13]),
                        "last_ask_volume": DataConverter.convert_value(row[14], is_int=True),
                        "pe_ratio": DataConverter.convert_value(row[15])
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing foreign stock row: {str(e)}")
                    continue
                    
            if not quote_data:
                raise ValueError("No data was parsed from the response")
                
            logger.info(f"Successfully parsed {len(quote_data)} foreign stock records for date {execution_date.date()}")
            return {"quote_data": quote_data}
            
    except httpx.RequestError as e:
        logger.error(f"Error making request: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

@task(task_id='save_quote_data')
def save_quote_data(data: Dict):
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 將日期字串轉換回 Date 物件
        for record in data["quote_data"]:
            record["date"] = date.fromisoformat(record["date"])
        
        # 批次獲取所有已存在的記錄
        existing_records = session.query(DailyQuote).filter(
            DailyQuote.date.in_([r["date"] for r in data["quote_data"]]),
            DailyQuote.stock_code.in_([r["stock_code"] for r in data["quote_data"]])
        ).all()
        
        # 建立已存在記錄的映射
        existing_map = {(r.date, r.stock_code): r for r in existing_records}
        
        # 批次處理更新和插入
        for record in data["quote_data"]:
            try:
                key = (record["date"], record["stock_code"])
                if key in existing_map:
                    # 更新現有記錄
                    existing_record = existing_map[key]
                    for key, value in record.items():
                        setattr(existing_record, key, value)
                else:
                    # 插入新記錄
                    new_record = DailyQuote(**record)
                    session.add(new_record)
            except Exception as e:
                logger.error(f"Error processing quote record {record}: {str(e)}")
                continue
        
        session.commit()
        logger.info(f"Successfully saved {len(data['quote_data'])} foreign stock records to database")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving quote data to database: {str(e)}")
        raise
    finally:
        session.close()

with DAG(
    'twse_foreign_stock_dag',
    default_args=default_args,
    description='爬取台灣證券交易所第一上市外國股票收盤行情',
    schedule='0 15 * * *',  # 每天15:00執行
    start_date=datetime(2024, 1, 1, tzinfo=TAIPEI_TZ),  # 設定開始時間為台北時區
    catchup=False,  # 啟用回補功能
    tags=['twse', 'foreign_stock'],
) as dag:

    create_tables_task = create_tables()
    check_trading_day_task, skip_execution_task = create_trading_day_branch(dag, 'scrape_foreign_stock_data')
    scrape_data_task = scrape_foreign_stock()
    save_data_task = save_quote_data(scrape_data_task)

    create_tables_task >> check_trading_day_task
    check_trading_day_task >> [scrape_data_task, skip_execution_task]
    scrape_data_task >> save_data_task 