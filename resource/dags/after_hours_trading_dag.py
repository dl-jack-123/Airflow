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
from models.twse_models import Base, AfterHoursTrading
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

@task(task_id='scrape_after_hours_trading_data')
def scrape_after_hours_trading(**context) -> List[Dict]:
    try:
        # 從 Airflow context 獲取執行時間
        execution_date = context['execution_date']
        # 使用 execution_date 的日期，而不是當前時間
        date_str = execution_date.strftime("%Y%m%d")
        
        logger.info(f"Processing data for date: {date_str}")
        
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/BFT41U"
        params = {
            "date": date_str,
            "selectType": "ALL",
            "response": "json",
            "_": int(execution_date.timestamp() * 1000)  # 使用時間戳作為 _ 參數
        }
        
        with httpx.Client() as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data["stat"] != "OK":
                # 檢查是否為空資料
                if "很抱歉，沒有符合條件的資料!" in str(data):
                    logger.info(f"No data found for date {date_str}")
                    return {"after_hours_trading_data": []}
                raise ValueError(f"API returned error status: {data['stat']}")
                
            after_hours_trading_data = []
            
            # 檢查資料結構
            if "data" not in data:
                logger.warning(f"No 'data' field found in response for date {date_str}")
                return {"after_hours_trading_data": []}
                
            # 處理盤後定價交易資料
            for row in data["data"]:
                try:
                    # 檢查資料列長度
                    if len(row) < 8:
                        logger.warning(f"Row has insufficient columns: {row}")
                        continue
                        
                    # 解析股票代號和名稱
                    stock_info = row[0].strip()
                    stock_code = stock_info[:6].strip()
                    stock_name = stock_info[6:].strip()
                    
                    after_hours_trading_data.append({
                        "date": execution_date.date().isoformat(),  # 使用 execution_date 的日期
                        "stock_code": stock_code,  # 證券代號
                        "stock_name": stock_name,  # 證券名稱
                        "trade_volume": DataConverter.convert_value(row[8], is_int=True),  # 成交張數
                        "trade_count": DataConverter.convert_value(row[7], is_int=True),  # 成交筆數
                        "trade_amount": DataConverter.convert_value(row[9], is_int=True),  # 成交金額
                        "trade_price": DataConverter.convert_value(row[6]),  # 成交價(元)
                        "last_bid_volume": DataConverter.convert_value(row[3], is_int=True),  # 委買張數
                        "last_ask_volume": DataConverter.convert_value(row[5], is_int=True)  # 委賣張數
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing after hours trading row: {str(e)}, row data: {row}")
                    continue
                    
            if not after_hours_trading_data:
                logger.warning(f"No valid data found in the response for date {date_str}")
                return {"after_hours_trading_data": []}
                
            logger.info(f"Successfully parsed {len(after_hours_trading_data)} after hours trading records for date {date_str}")
            return {"after_hours_trading_data": after_hours_trading_data}
            
    except httpx.RequestError as e:
        logger.error(f"Error making request: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

@task(task_id='save_after_hours_trading_data')
def save_after_hours_trading_data(data: Dict):
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 將日期字串轉換回 Date 物件
        for record in data["after_hours_trading_data"]:
            record["date"] = date.fromisoformat(record["date"])
        
        # 批次獲取所有已存在的記錄
        existing_records = session.query(AfterHoursTrading).filter(
            AfterHoursTrading.date.in_([r["date"] for r in data["after_hours_trading_data"]]),
            AfterHoursTrading.stock_code.in_([r["stock_code"] for r in data["after_hours_trading_data"]])
        ).all()
        
        # 建立已存在記錄的映射
        existing_map = {(r.date, r.stock_code): r for r in existing_records}
        
        # 批次處理更新和插入
        for record in data["after_hours_trading_data"]:
            try:
                key = (record["date"], record["stock_code"])
                if key in existing_map:
                    # 更新現有記錄
                    existing_record = existing_map[key]
                    for key, value in record.items():
                        setattr(existing_record, key, value)
                else:
                    # 插入新記錄
                    new_record = AfterHoursTrading(**record)
                    session.add(new_record)
            except Exception as e:
                logger.error(f"Error processing after hours trading record {record}: {str(e)}")
                continue
        
        session.commit()
        logger.info(f"Successfully saved {len(data['after_hours_trading_data'])} after hours trading records to database")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving after hours trading data to database: {str(e)}")
        raise
    finally:
        session.close()

with DAG(
    'after_hours_trading_dag',
    default_args=default_args,
    description='爬取盤後定價交易資料',
    schedule='0 16 * * *',  # 每天15:00執行
    start_date=datetime(2024, 1, 1, tzinfo=TAIPEI_TZ),  # 設定開始時間為台北時區
    catchup=False,  # 啟用回補功能
    tags=['twse', 'after_hours_trading'],
) as dag:

    create_tables_task = create_tables()
    check_trading_day_task, skip_execution_task = create_trading_day_branch(dag, 'scrape_after_hours_trading_data')
    scrape_data_task = scrape_after_hours_trading()
    save_data_task = save_after_hours_trading_data(scrape_data_task)

    create_tables_task >> check_trading_day_task
    check_trading_day_task >> [scrape_data_task, skip_execution_task]
    scrape_data_task >> save_data_task 