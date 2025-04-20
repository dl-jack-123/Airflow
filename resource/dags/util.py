from datetime import datetime, date
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import holidays
import pytz
from sqlalchemy.orm import sessionmaker
from models.twse_models import TradingDate

logger = logging.getLogger(__name__)

# 設定台北時區
TAIPEI_TZ = pytz.timezone('Asia/Taipei')

# 設定台灣的假日
tw_holidays = holidays.TW()

class DataConverter:
    @staticmethod
    def convert_value(value, is_int=False):
        """
        將數值轉換為整數或浮點數
        :param value: 要轉換的值
        :param is_int: 是否轉換為整數
        :return: 轉換後的數值或 None
        """
        if isinstance(value, str):
            if value.replace("-", "").strip() in ["", "N/A"]:
                return None
            else:
                return int(value.replace(",", "")) if is_int else float(value.replace(",", ""))
        elif isinstance(value, (int, float)):
            return int(value) if is_int else float(value)
        return None

def is_trading_day(execution_date: datetime) -> bool:
    """
    檢查是否為交易日
    1. 不是週末（週六、週日）
    2. 不是國定假日
    3. 不是休市日
    """
    # 檢查是否為週末
    if execution_date.weekday() >= 5:  # 5 是週六，6 是週日
        return False
    
    # 檢查是否為國定假日
    if execution_date.date() in tw_holidays:
        return False
        
    # 檢查是否為休市日
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        trading_date = session.query(TradingDate).filter(
            TradingDate.date == execution_date.date()
        ).first()
        
        if trading_date and trading_date.is_holiday:
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error checking trading date: {str(e)}")
        return False
    finally:
        session.close()

def check_trading_day(scrape_task_id: str, **context):
    """
    檢查是否為交易日，並返回對應的分支
    :param scrape_task_id: 爬取資料的任務 ID
    :param context: Airflow context
    :return: 要執行的任務 ID
    """
    execution_date = context['execution_date']
    if is_trading_day(execution_date):
        return scrape_task_id
    else:
        return 'skip_execution'

def get_db_engine():
    """
    建立資料庫連線引擎
    """
    try:
        conn = BaseHook.get_connection('postgres_default')
        connection_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {str(e)}")
        raise

def create_trading_day_branch(dag, scrape_task_id: str):
    """
    建立交易日檢查的分支任務
    :param dag: DAG 物件
    :param scrape_task_id: 爬取資料的任務 ID
    :return: (check_trading_day_task, skip_execution_task)
    """
    check_trading_day_task = BranchPythonOperator(
        task_id='check_trading_day',
        python_callable=check_trading_day,
        op_kwargs={'scrape_task_id': scrape_task_id},  # 傳入爬取任務 ID
        provide_context=True,
        dag=dag
    )
    
    skip_execution_task = DummyOperator(
        task_id='skip_execution',
        dag=dag
    )
    
    return check_trading_day_task, skip_execution_task 