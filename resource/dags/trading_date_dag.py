from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.decorators import task
from sqlalchemy.orm import sessionmaker
import logging
from typing import List, Dict
import httpx
from models.twse_models import Base, TradingDate
from util import (
    TAIPEI_TZ,
    get_db_engine
)
import pytz
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

@task(task_id='create_tables')
def create_tables():
    try:
        engine = get_db_engine()
        Base.metadata.create_all(engine)
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

@task(task_id='scrape_trading_date')
def scrape_trading_date(**context) -> List[Dict]:
    try:
        # 從 Airflow context 獲取執行時間
        execution_date = context['execution_date']
        year = execution_date.year
        
        logger.info(f"Processing trading date data for year: {year}")
        
        url = "https://www.tpex.org.tw/www/zh-tw/bulletin/tradingDate"
        payload = {
            "date": str(year),
            "id": "",
            "response": "json"
        }
        primary_keys = []
        
        with httpx.Client() as client:
            response = client.post(url, data=payload)
            response.raise_for_status()
            data = response.json()
            
            if data["stat"] != "ok":
                raise ValueError(f"API returned error status: {data['stat']}")
                
            trading_dates = []
            
            # 解析 HTML 表格
            html = data["data"]["html"]
            soup = BeautifulSoup(html, 'html.parser')
            
            # 找到所有表格行
            rows = soup.find_all('tr')[1:]  # 跳過表頭
            
            for row in rows:
                try:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) < 4:
                        continue
                        
                    name = cols[0].text.strip()
                    date_text = cols[1].text.strip()
                    description = cols[3].text.strip()
                    
                    # 判斷是否為休假日
                    is_holiday = (
                        any(keyword in description for keyword in ["放假", "休市", "無交易"]) or
                        "市場無交易，僅辦理給付結算" in description
                    ) and name != '農曆春節前國際債券交易系統最後交易日'
                    
                    # 處理日期格式
                    date_parts = date_text.split('<br />')
                    for date_part in date_parts:
                        date_part = date_part.strip()
                        if not date_part:
                            continue
                            
                        # 處理多個日期的情況（例如 "1月21日及1月22日市場無交易，僅辦理給付結算。"）
                        if '及' in description:
                            # 先分割出日期部分
                            date_parts = description.split('市場')[0].strip().replace('及', '、').split('、')
                            for date_str in date_parts:
                                try:
                                    # 處理 "月日" 格式
                                    if '月' in date_str and '日' in date_str:
                                        # 移除可能的空白
                                        date_str = date_str.strip()
                                        # 分割月份和日期
                                        month_day = date_str.split('月')
                                        month = int(month_day[0])
                                        day = int(month_day[1].replace('日', ''))
                                        date_obj = datetime(year, month, day).date()
                                        # 判斷是否為休假日
                                        is_holiday = (
                                            any(keyword in description for keyword in ["放假", "休市", "無交易"]) or
                                            "市場無交易，僅辦理給付結算" in description
                                        ) 
                                        if date_obj not in primary_keys:
                                            primary_keys.append(date_obj)
                                            trading_dates.append({
                                                "date": date_obj,
                                                "name": name,
                                                "is_holiday": is_holiday,
                                                "description": description,
                                                "country": "TW"
                                            })
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"Error parsing date {date_str}: {str(e)}")
                        # 處理其他格式的日期
                        elif ' ' in date_part or '\n' in date_part or '\r' in date_part or '\t' in date_part or '日' in date_part:
                            # 分割日期字串，處理所有可能的分隔符號
                            # 先將所有特殊字符替換為空格
                            date_part = date_part.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                            # 在每個"日"後面添加空格，以處理沒有空格分隔的連續日期
                            date_part = date_part.replace('日', '日 ')
                            # 分割日期字串
                            dates = date_part.split()
                            for single_date in dates:
                                try:
                                    # 處理 "月日" 格式
                                    if '月' in single_date and '日' in single_date:
                                        # 移除可能的空白
                                        single_date = single_date.strip()
                                        # 分割月份和日期
                                        month_day = single_date.split('月')
                                        month = int(month_day[0])
                                        day = int(month_day[1].replace('日', ''))
                                        date_obj = datetime(year, month, day).date()
                                        if date_obj not in primary_keys:
                                            primary_keys.append(date_obj)
                                            trading_dates.append({
                                                "date": date_obj,
                                                "name": name,
                                            "is_holiday": is_holiday,
                                            "description": description,
                                            "country": "TW"
                                            })
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"Error parsing date {single_date}: {str(e)}")
                            
                except Exception as e:
                    logger.warning(f"Error parsing row: {str(e)}")
                    continue
                    
            if not trading_dates:
                raise ValueError("No data was parsed from the response")
                
            logger.info(f"Successfully parsed {len(trading_dates)} trading date records")
            return {"trading_dates": trading_dates}
            
    except httpx.RequestError as e:
        logger.error(f"Error making request: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

@task(task_id='save_trading_date')
def save_trading_date(data: Dict):
    try:
        engine = get_db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # 批次處理所有記錄
            for record in data["trading_dates"]:
                try:
                    # 使用 merge 操作來處理重複的日期
                    existing_record = session.query(TradingDate).filter(
                        TradingDate.date == record["date"]
                    ).first()
                    
                    if existing_record:
                        # 更新現有記錄
                        for key, value in record.items():
                            setattr(existing_record, key, value)
                    else:
                        # 插入新記錄
                        new_record = TradingDate(**record)
                        session.add(new_record)
                        
                except Exception as e:
                    logger.error(f"Error processing record {record}: {str(e)}")
                    continue
                    
            session.commit()
            logger.info(f"Successfully saved {len(data['trading_dates'])} trading date records to database")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving trading date data to database: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise
    finally:
        session.close()

with DAG(
    'trading_date_dag',
    default_args=default_args,
    description='爬取台灣證券市場休市日資料',
    schedule='0 0 1 1 *',  # 每年1月1日執行
    start_date=datetime(2024, 1, 1, tzinfo=TAIPEI_TZ),
    catchup=True,
    tags=['trading_date'],
) as dag:

    create_tables_task = create_tables()
    scrape_data_task = scrape_trading_date()
    save_data_task = save_trading_date(scrape_data_task)

    create_tables_task >> scrape_data_task >> save_data_task 