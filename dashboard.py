import streamlit as st
import snowflake.connector
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
   user=os.getenv('user'),
    password=os.getenv('password'),
    account=os.getenv('account'),
    warehouse=os.getenv('warehouse'),
    database=os.getenv('database'),
    schema=os.getenv('schema')
)

st.title("Real-Time Product Changes Dashboard")

placeholder = st.empty()

while True:
    cur = conn.cursor()
    cur.execute("""
        SELECT ID, NAME, PRICE, UPDATED_DATE
        FROM PRODUCTS
        ORDER BY UPDATED_DATE DESC
        LIMIT 20
    """)
    df = cur.fetch_pandas_all()
    cur.close()

    with placeholder.container():
        st.subheader("Latest CDC Events")
        st.dataframe(df)

        st.subheader("Events Over Time")
        st.line_chart(df.set_index("UPDATED_DATE")["ID"])

    time.sleep(2)  # Refresh every 2 sec
