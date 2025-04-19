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
from models.twse_models import Base, ShortSelling
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

@task(task_id='scrape_short_selling_data')
def scrape_short_selling(**context) -> List[Dict]:
    try:
        # 從 Airflow context 獲取執行時間
        execution_date = context['execution_date']
        # 使用 execution_date 的日期，而不是當前時間
        date_str = execution_date.strftime("%Y%m%d")
        
        logger.info(f"Processing data for date: {date_str}")
        
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/TWTASU"
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
                # 檢查是否為空資料
                if "很抱歉，沒有符合條件的資料!" in str(data):
                    logger.info(f"No data found for date {date_str}")
                    return {"short_selling_data": []}
                raise ValueError(f"API returned error status: {data['stat']}")
                
            short_selling_data = []
            
            # 檢查資料結構
            if "data" not in data:
                logger.warning(f"No 'data' field found in response for date {date_str}")
                return {"short_selling_data": []}
                
            # 處理融券賣出與借券賣出成交量值資料
            for row in data["data"]:
                try:
                    # 檢查資料列長度
                    if len(row) < 5:
                        logger.warning(f"Row has insufficient columns: {row}")
                        continue
                        
                    # 解析股票代號和名稱
                    stock_info = row[0].strip()
                    stock_code = stock_info[:6].strip()
                    stock_name = stock_info[6:].strip()
                    
                    # 解析融券賣出資料
                    margin_selling_volume = DataConverter.convert_value(row[1], is_int=True)
                    margin_selling_amount = DataConverter.convert_value(row[2], is_int=True)
                    
                    # 解析借券賣出資料
                    securities_borrowing_volume = DataConverter.convert_value(row[3], is_int=True)
                    securities_borrowing_amount = DataConverter.convert_value(row[4], is_int=True)
                    
                    # 計算總量
                    total_volume = (margin_selling_volume or 0) + (securities_borrowing_volume or 0)
                    total_amount = (margin_selling_amount or 0) + (securities_borrowing_amount or 0)
                    
                    # 計算比率
                    margin_selling_ratio = (margin_selling_volume / total_volume * 100) if total_volume > 0 else 0
                    securities_borrowing_ratio = (securities_borrowing_volume / total_volume * 100) if total_volume > 0 else 0
                    
                    short_selling_data.append({
                        "date": execution_date.date().isoformat(),  # 使用 execution_date 的日期
                        "stock_code": stock_code,  # 證券代號
                        "stock_name": stock_name,  # 證券名稱
                        "margin_selling_volume": margin_selling_volume,  # 融券賣出成交股數
                        "margin_selling_amount": margin_selling_amount,  # 融券賣出成交金額
                        "securities_borrowing_volume": securities_borrowing_volume,  # 借券賣出成交股數
                        "securities_borrowing_amount": securities_borrowing_amount,  # 借券賣出成交金額
                        "total_volume": total_volume,  # 總成交股數
                        "total_amount": total_amount,  # 總成交金額
                        "margin_selling_ratio": margin_selling_ratio,  # 融券賣出成交股數佔總成交股數比率
                        "securities_borrowing_ratio": securities_borrowing_ratio  # 借券賣出成交股數佔總成交股數比率
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing short selling row: {str(e)}, row data: {row}")
                    continue
                    
            if not short_selling_data:
                logger.warning(f"No valid data found in the response for date {date_str}")
                return {"short_selling_data": []}
                
            logger.info(f"Successfully parsed {len(short_selling_data)} short selling records for date {date_str}")
            return {"short_selling_data": short_selling_data}
            
    except httpx.RequestError as e:
        logger.error(f"Error making request: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

@task(task_id='save_short_selling_data')
def save_short_selling_data(data: Dict):
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 將日期字串轉換回 Date 物件
        for record in data["short_selling_data"]:
            record["date"] = date.fromisoformat(record["date"])
        
        # 批次獲取所有已存在的記錄
        existing_records = session.query(ShortSelling).filter(
            ShortSelling.date.in_([r["date"] for r in data["short_selling_data"]]),
            ShortSelling.stock_code.in_([r["stock_code"] for r in data["short_selling_data"]])
        ).all()
        
        # 建立已存在記錄的映射
        existing_map = {(r.date, r.stock_code): r for r in existing_records}
        
        # 批次處理更新和插入
        for record in data["short_selling_data"]:
            try:
                key = (record["date"], record["stock_code"])
                if key in existing_map:
                    # 更新現有記錄
                    existing_record = existing_map[key]
                    for key, value in record.items():
                        setattr(existing_record, key, value)
                else:
                    # 插入新記錄
                    new_record = ShortSelling(**record)
                    session.add(new_record)
            except Exception as e:
                logger.error(f"Error processing short selling record {record}: {str(e)}")
                continue
        
        session.commit()
        logger.info(f"Successfully saved {len(data['short_selling_data'])} short selling records to database")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving short selling data to database: {str(e)}")
        raise
    finally:
        session.close()

with DAG(
    'short_selling_dag',
    default_args=default_args,
    description='爬取融券賣出與借券賣出成交量值資料',
    schedule='0 15 * * *',  # 每天15:00執行
    start_date=datetime(2024, 1, 1, tzinfo=TAIPEI_TZ),  # 設定開始時間為台北時區
    catchup=False,  # 啟用回補功能
    tags=['twse', 'short_selling'],
) as dag:

    create_tables_task = create_tables()
    check_trading_day_task, skip_execution_task = create_trading_day_branch(dag, 'scrape_short_selling_data')
    scrape_data_task = scrape_short_selling()
    save_data_task = save_short_selling_data(scrape_data_task)

    create_tables_task >> check_trading_day_task
    check_trading_day_task >> [scrape_data_task, skip_execution_task]
    scrape_data_task >> save_data_task 