import os
import streamlit as st
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_env_var(key, default=None, user=None):
    """
    Retrieve environment variable with optional User prefix.
    If user is provided, looks for {USER}_{KEY}.
    Fallback to generic {KEY} if user-specific not found? Protocol says:
    Users have valid keys like 'USER1_DB_HOST'.
    """
    if user:
        # Try specific user key first: e.g. USER1_DB_HOST
        user_prefix = user.upper()
        # Handle cases like "user1_ms" -> maybe just use the full string or a mapping?
        # The existing code passes "user1" or "user2" as keys often.
        # But db_utils passes "user1" (from secrets keys).
        
        # We will assume the keys in the .env are patterned as:
        # USER1_DB_HOST, USER1_DB_USER, etc.
        # where 'user' argument is 'user1'
        
        # If the user string contains non-alphanumeric, might need cleaning, but let's assume it's clean for now.
        full_key = f"{user_prefix}_{key}"
        if full_key in os.environ:
            return os.environ[full_key]
    
    # Try generic key
    if key in os.environ:
        return os.environ[key]
        
    return default

def get_db_creds(user_key):
    """
    Retrieve database credentials for a specific user.
    Prioritizes Environment Variables.
    Falls back to st.secrets for backward compatibility (optional).
    """
    # 1. Try Environment Variables
    # keys needed: host, dbname, user, password, port
    # Mapping:
    # DB_HOST -> host
    # DB_NAME -> dbname
    # DB_USER -> user
    # DB_PASS -> password
    # DB_PORT -> port
    # DB_URL -> url (optional)
    
    host = get_env_var("DB_HOST", user=user_key)
    dbname = get_env_var("DB_NAME", user=user_key)
    user = get_env_var("DB_USER", user=user_key)
    password = get_env_var("DB_PASS", user=user_key)
    port = get_env_var("DB_PORT", user=user_key)
    url = get_env_var("DB_URL", user=user_key)

    if url:
        return {"url": url}
    
    if host and dbname and user and password and port:
        return {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port
        }

    # 2. Fallback to st.secrets
    if user_key in st.secrets:
        return st.secrets[user_key]
        
    return {}

def get_valid_users():
    """
    Returns list of valid users. 
    In .env, this could be a comma-separated list like USERS=user1,user2
    """
    users_str = os.getenv("VALID_USERS")
    if users_str:
        return [u.strip() for u in users_str.split(",")]
    
    # Fallback to secrets
    if "database" in st.secrets and "users" in st.secrets["database"]:
        return st.secrets["database"]["users"]
        
    # Default fallback
    return ["user1", "user2"]

def get_table_name(user_key):
    """
    Returns the table name for a user.
    Env Var: {USER}_TABLE_NAME
    """
    tbl = get_env_var("TABLE_NAME", user=user_key)
    if tbl:
        return tbl
        
    if user_key in st.secrets:
         return st.secrets[user_key].get("table_name", user_key)
         
    return user_key

def get_dashboard_users_key():
    """
    Returns the key/user used to access the centralized 'account_dashboard_users' table.
    """
    # Usually "user1" or defined in env
    return os.getenv("DASHBOARD_DB_USER", "user1")
