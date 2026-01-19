import streamlit as st
from db_utils import fetch_data
from data_loading import run_data_loading
import sys

def main():
    print("Starting daily update...")

    # Configuration: Define users and their target exchanges
    # User1: Hyperliquid (which automatically sums up main + secondary accounts)
    # User2: BitGet, Hyperliquid, Deribit
    user_configs = [
        {
            "user": "user1",
            "user_id_val": "user1_ms",
            "exchanges": ["Hyperliquid"] 
        },
        {
            "user": "user2",
            "user_id_val": "user2_jf",
            "exchanges": ["BitGet", "Hyperliquid", "Deribit"]
        }
    ]
    
    any_failure = False

    for config in user_configs:
        user = config["user"]
        user_id_val = config["user_id_val"]
        exchanges = config["exchanges"]
        
        print(f"\n=== Processing {user} ===")
        
        if user not in st.secrets:
            print(f"Error: User '{user}' not found in secrets. Skipping.")
            any_failure = True
            continue
            
        table_name = st.secrets[user].get("table_name", user)
        
        # 1. Load existing data
        print(f"Fetching existing data for {user} from {table_name}...")
        try:
            raw_df = fetch_data(user, table_name=table_name)
            if raw_df.empty:
                print(f"Warning: No existing data found for {user}. PnL calculations may be rough.")
        except Exception as e:
            print(f"Error fetching data for {user}: {e}")
            any_failure = True
            continue
            
        # 2. Run Updates
        for ex in exchanges:
            print(f"--- Running {ex} for {user} ---")
            try:
                success, msg = run_data_loading(ex, user, table_name, raw_df, user_id_val)
                if success:
                    print(f"SUCCESS: {msg}")
                else:
                    print(f"FAILURE: {msg}")
                    any_failure = True
            except Exception as e:
                print(f"CRITICAL ERROR for {ex} ({user}): {e}")
                any_failure = True

    print("--- Daily Update Complete ---")
    
    if any_failure:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
