# Airflow 資料爬蟲專案

## 專案概述
本專案使用 Apache Airflow 建立自動化資料爬蟲系統，主要用於爬取台灣證券市場相關資料。

## 功能特點
- 自動化爬取台灣證券交易所（TWSE）和櫃買中心（TPEx）的市場資料
- 支援多種資料類型：指數、個股、盤後定價交易、融資融券等
- 自動判斷交易日和非交易日
- 完整的錯誤處理和重試機制
- 資料自動儲存到資料庫

## 系統需求
- Python 3.8+
- Apache Airflow 2.0+
- PostgreSQL 12+
- 其他依賴套件請參考 `requirements.txt`

## 專案結構
```
resource/
├── dags/
│   ├── models/          # 資料模型定義
│   ├── twse_index_dag.py        # 台股大盤指數 DAG
│   ├── twse_foreign_stock_dag.py # 外資買賣超 DAG
│   ├── trading_date_dag.py      # 交易日資料 DAG
│   ├── after_hours_trading_dag.py # 盤後定價交易 DAG
│   └── tpex_after_hours_trading_dag.py # 櫃買中心盤後定價交易 DAG
└── util/               # 工具函數
```

## 資料模型
### TradingDate
- 儲存交易日和非交易日資訊
- 主要欄位：日期、名稱、是否為休假日、說明、國別

### TwseIndex
- 儲存台股大盤指數資料
- 主要欄位：日期、指數名稱、收盤價、漲跌值、漲跌幅、成交量、成交金額

### DailyQuote
- 儲存個股每日交易資料
- 主要欄位：日期、股票代號、股票名稱、成交量、成交筆數、成交金額、開盤價、最高價、最低價、收盤價等

### ShortSelling
- 儲存融資融券交易資料
- 主要欄位：日期、股票代號、股票名稱、融券賣出成交量、融券賣出成交金額、借券賣出成交量等

### AfterHoursTrading
- 儲存盤後定價交易資料
- 主要欄位：日期、股票代號、股票名稱、成交量、成交筆數、成交金額、成交價等

## 排程設定
- 台股大盤指數：每日 14:30 執行
- 外資買賣超：每日 15:00 執行
- 交易日資料：每年 1 月 1 日執行
- 盤後定價交易：每日 16:00 執行
- 櫃買中心盤後定價交易：每日 16:00 執行

## 錯誤處理
- 所有 DAG 都包含完整的錯誤處理機制
- 預設重試次數：5 次
- 重試間隔：5 分鐘
- 詳細的錯誤日誌記錄

## 開發指南
1. 新增資料模型
   - 在 `models/twse_models.py` 中定義新的資料模型
   - 確保包含必要的欄位和主鍵設定

2. 建立新的 DAG
   - 參考現有 DAG 的結構
   - 包含必要的任務：建立表格、爬取資料、儲存資料
   - 實作錯誤處理和日誌記錄

3. 資料處理
   - 使用 `DataConverter` 進行資料類型轉換
   - 處理特殊情況（如空資料、格式錯誤等）
   - 確保資料的完整性和正確性

## 部署指南
1. 安裝依賴套件
   ```bash
   pip install -r requirements.txt
   ```

2. 設定資料庫連線
   - 在 Airflow 中設定 PostgreSQL 連線
   - 確保連線資訊正確

3. 啟動 Airflow
   ```bash
   airflow webserver
   airflow scheduler
   ```

## 注意事項
- 所有時間都使用台北時區（Asia/Taipei）
- 確保資料庫連線設定正確
- 定期檢查日誌以監控執行狀況
- 注意 API 的呼叫限制和頻率

## 貢獻指南
1. Fork 本專案
2. 建立新的分支
3. 提交修改
4. 發起 Pull Request

## 授權
MIT License
