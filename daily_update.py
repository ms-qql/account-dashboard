import streamlit as st
from db_utils import fetch_data
from data_loading import run_data_loading
import sys

def main():
    print("Starting daily update for User2...")
    
    # Configuration
    user = "user2" # Corresponds to secrets key
    user_id_val = "user2_jf" # Database value
    
    if user not in st.secrets:
        print(f"Error: User '{user}' not found in secrets.")
        sys.exit(1)
        
    table_name = st.secrets[user].get("table_name", user)
    
    # 1. Load existing data (needed for PnL calc and content copying)
    print(f"Fetching existing data for {user} from {table_name}...")
    try:
        raw_df = fetch_data(user, table_name=table_name)
        if raw_df.empty:
            print("Warning: No existing data found. PnL calculations may be rough.")
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)
        
    exchanges_to_run = ["BitGet", "Deribit"]
    
    any_failure = False
    
    # 2. Run Updates
    for ex in exchanges_to_run:
        print(f"--- Running {ex} ---")
        try:
            success, msg = run_data_loading(ex, user, table_name, raw_df, user_id_val)
            if success:
                print(f"SUCCESS: {msg}")
            else:
                print(f"FAILURE: {msg}")
                any_failure = True
        except Exception as e:
            print(f"CRITICAL ERROR for {ex}: {e}")
            any_failure = True

    print("--- Daily Update Complete ---")
    
    if any_failure:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
