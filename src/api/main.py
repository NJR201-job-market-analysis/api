# 匯入相關套件
import pandas as pd  # 用來處理資料表
from fastapi import FastAPI, Query  # 建立 API 用
from sqlalchemy import create_engine, engine  # 用來建立資料庫連線
from typing import List, Optional

# 匯入自定義的資料庫連線設定
from .config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
)


# 建立連接到 MySQL 資料庫的函式，回傳一個 SQLAlchemy 的連線物件
def get_mysql_conn() -> engine.base.Connection:
    # 組成資料庫連線字串，使用 pymysql 作為 driver
    address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(address)  # 建立 SQLAlchemy 引擎
    connect = engine.connect()  # 建立實際連線
    return connect  # 回傳連線物件


# 建立 FastAPI 應用實例
app = FastAPI()


# 定義根目錄路由（測試用）
@app.get("/")
def read_root():
    return {"Hello": "World"}  # 回傳基本測試訊息


@app.get("/jobs/hot")
def get_hot_jobs(limit: int = 10):
    """
    取得最熱門職缺
    """
    sql = f"""
    SELECT * FROM jobs
    ORDER BY views DESC
    LIMIT {limit}
    """
    mysql_conn = get_mysql_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}


@app.get("/jobs")
def get_jobs(
    category: Optional[str] = None,
    keyword: Optional[str] = None,
    location: Optional[str] = None,
):
    """
    透過分類、關鍵字或地區等欄位，查詢職缺
    """
    sql = "SELECT * FROM jobs WHERE 1=1"
    if category:
        sql += f" AND category = '{category}'"
    if keyword:
        sql += f" AND (title LIKE '%{keyword}%' OR description LIKE '%{keyword}%')"
    if location:
        sql += f" AND location = '{location}'"

    mysql_conn = get_mysql_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}


@app.get("/skills/hot")
def get_hot_skills(limit: int = 10):
    """
    取得最熱門技能
    """
    sql = f"""
    SELECT s.name, COUNT(js.skill_id) as skill_count
    FROM job_skills js
    JOIN skills s ON js.skill_id = s.id
    GROUP BY s.name
    ORDER BY skill_count DESC
    LIMIT {limit}
    """
    mysql_conn = get_mysql_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}