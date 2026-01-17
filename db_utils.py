import psycopg2
import streamlit as st
import pandas as pd
from typing import List, Optional
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_connection(user_key: str):
    """
    Creates a raw psycopg2 connection to the database for a specific user.
    Uses credentials from Streamlit secrets.
    Used for write operations (insert/delete).
    """
    try:
        creds = st.secrets[user_key]
        
        # Check if a direct URL is provided
        if "url" in creds:
            return psycopg2.connect(creds["url"])
        
        # Otherwise use individual components
        conn = psycopg2.connect(
            host=creds["host"],
            database=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            port=creds["port"]
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to database for {user_key}: {e}")
        return None

def get_db_engine(user_key: str):
    """
    Creates a SQLAlchemy engine for a specific user.
    Used for reading data with pandas.
    """
    try:
        creds = st.secrets[user_key]
        
        if "url" in creds:
            # Assume URL is compatible or needs slight adjustment (e.g. postgres:// -> postgresql://)
            url = creds["url"].replace("postgres://", "postgresql://")
        else:
            # Construct URL safely with quoted password
            user = creds["user"]
            password = quote_plus(creds["password"])
            host = creds["host"]
            port = creds["port"]
            dbname = creds["dbname"]
            
            url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            
        return create_engine(url)
    except Exception as e:
        st.error(f"Error creating database engine for {user_key}: {e}")
        return None

def fetch_data(user_key: str, query: str = None, table_name: str = None) -> pd.DataFrame:
    """
    Fetches data from the database and returns a pandas DataFrame.
    If query is not provided, it builds one using table_name.
    Uses SQLAlchemy engine to avoid pandas UserWarnings.
    """
    engine = get_db_engine(user_key)
    if engine is None:
        return pd.DataFrame()
    
    try:
        if query is None and table_name is not None:
            query = f'SELECT * FROM "{table_name}"'
        elif query is None:
            raise ValueError("Either query or table_name must be provided")

        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        
        return df
    except Exception as e:
        st.error(f"Error fetching data for {user_key}: {e}")
        return pd.DataFrame()

def get_all_user_data(user_keys: List[str], table_name: str = "account_data") -> pd.DataFrame:
    """
    Fetches data for all users and combines them into a single DataFrame.
    Assumes each user has a similar table structure.
    """
    all_dfs = []
    for user in user_keys:
        query = f"SELECT * FROM {table_name}"
        df = fetch_data(user, query)
        if not df.empty:
            df['db_user'] = user # Track which DB it came from
            all_dfs.append(df)
    
    if not all_dfs:
        return pd.DataFrame()
        
    return pd.concat(all_dfs, ignore_index=True)

def get_latest_data(user_key: str, table_name: str = None) -> pd.DataFrame:
    """
    Fetches the most recent record for a user (based on date_world).
    """
    engine = get_db_engine(user_key)
    if engine is None:
        return pd.DataFrame()
    
    try:
        if table_name is None:
            # Try to guess or fail. Better to require it or fetch from secrets.
            # Usually passed in.
            return pd.DataFrame()

        query = f'SELECT * FROM "{table_name}" ORDER BY "date_world" DESC LIMIT 1'
        
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        
        return df
    except Exception as e:
        st.error(f"Error fetching latest data for {user_key}: {e}")
        return pd.DataFrame()

def insert_account_data(user_key: str, data_dict: dict, table_name: str = None):
    """
    Inserts a new record into the database.
    Deletes existing record for the same date and strategy to ensure uniqueness.
    """
    conn = get_connection(user_key)
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Extract fields
        date_world = data_dict.get('date_world')
        strategy = data_dict.get('strategy')
        
        if not date_world or not strategy:
            st.error("Missing date_world or strategy for insertion.")
            return False

        # Ensure date_world is a string to match DB TEXT column (prevents 'operator does not exist: text = date')
        if hasattr(date_world, 'isoformat'):
            date_world_str = date_world.strftime('%Y-%m-%d')
        else:
            date_world_str = str(date_world)

        # 1. Delete existing entry for this day and strategy
        delete_query = f'DELETE FROM "{table_name}" WHERE "date_world" = %s AND "strategy" = %s'
        cursor.execute(delete_query, (date_world_str, strategy))
        
        # 2. Insert new entry
        # columns: date_world, collateral, strategy, total_pnl, deposit, withdrawal, btc_pnl, eth_pnl, user_id, pos_size
        
        insert_query = f"""
            INSERT INTO "{table_name}" 
            (date_world, collateral, strategy, total_pnl, deposit, withdrawal, btc_pnl, eth_pnl, user_id, pos_size)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            date_world_str,
            data_dict.get('collateral', 0),
            strategy,
            data_dict.get('total_pnl', 0),
            data_dict.get('deposit', 0),
            data_dict.get('withdrawal', 0),
            data_dict.get('btc_pnl', 0),
            data_dict.get('eth_pnl', 0),
            data_dict.get('user_id'),
            data_dict.get('pos_size', 0)
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        st.error(f"Error inserting data for {user_key}: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False
