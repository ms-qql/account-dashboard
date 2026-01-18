import psycopg2
import streamlit as st
import pandas as pd
from typing import List, Optional
from sqlalchemy import create_engine
import bcrypt
from datetime import datetime

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

def initialize_user_table(user_key: str):
    """
    Creates the account_dashboard_users table and inserts initial hashed credentials.
    """
    conn = get_connection(user_key)
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Create table if not exists
        create_query = """
            CREATE TABLE IF NOT EXISTS account_dashboard_users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL
            )
        """
        cursor.execute(create_query)
        
        # Check if users already exist to avoid duplicates or overwriting
        cursor.execute("SELECT COUNT(*) FROM account_dashboard_users")
        if cursor.fetchone()[0] == 0:
            # Initial users and their current passwords
            initial_users = {
                "user1_ms": {"password": "password123", "role": "admin"},
                "user2_jf": {"password": "password456", "role": "user"}
            }
            
            for username, info in initial_users.items():
                password_hash = bcrypt.hashpw(info['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                cursor.execute(
                    "INSERT INTO account_dashboard_users (username, password_hash, role) VALUES (%s, %s, %s)",
                    (username, password_hash, info['role'])
                )
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error initializing user table for {user_key}: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def verify_user(user_key: str, username: str, password: str):
    """
    Verifies user credentials against the account_dashboard_users table.
    Returns (authenticated: bool, role: str or None)
    """
    conn = get_connection(user_key)
    if conn is None:
        return False, None
    
    try:
        cursor = conn.cursor()
        query = "SELECT password_hash, role FROM account_dashboard_users WHERE username = %s"
        cursor.execute(query, (username,))
        result = cursor.fetchone()
        
        if result:
            password_hash, role = result
            if bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
                return True, role
        
        cursor.close()
        conn.close()
        return False, None
    except Exception as e:
        # If table doesn't exist, initialize it and retry once
        if "does not exist" in str(e).lower():
            if initialize_user_table(user_key):
                return verify_user(user_key, username, password)
        
        st.error(f"Error verifying user for {user_key}: {e}")
        if conn:
            conn.close()
        return False, None

def update_user_password(user_key: str, username: str, new_password: str):
    """
    Hashes and updates the password for a user in the account_dashboard_users table.
    Returns bool (success/failure)
    """
    conn = get_connection(user_key)
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        query = "UPDATE account_dashboard_users SET password_hash = %s WHERE username = %s"
        cursor.execute(query, (password_hash, username))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error updating password for {username}: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False
