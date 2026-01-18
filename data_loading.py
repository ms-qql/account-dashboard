import streamlit as st
from datetime import datetime
import pandas as pd
from bitget import trade_bitget
from hl import trade_hl
from db_utils import insert_account_data

def run_data_loading(exchange_name, user, table_name, raw_df, user_id_val):
    try:
        current_date = datetime.now().date()
        msgs = []
        
        if exchange_name == "Deribit":
            # Copy logic
            deribit_df = raw_df[raw_df['strategy'].str.contains('Deribit|Option', case=False, na=False)]
            
            if not deribit_df.empty:
                last_entry_row = deribit_df.sort_values('date_world').iloc[-1]
                last_entry = last_entry_row.to_dict()
                
                last_entry['date_world'] = current_date
                last_entry['user_id'] = user_id_val 
                
                # Handle numpy types if any
                for k, v in last_entry.items():
                    if hasattr(v, 'item'):
                        last_entry[k] = v.item()

                if insert_account_data(user, last_entry, table_name):
                    return True, f"Deribit: Copied data for {current_date}"
                else:
                    return False, "Deribit: Failed to save data."
            else:
                return False, "Deribit: No existing data found to copy."

        else: # BitGet or Hyperliquid
            strat_name = ""
            total_balance = 0.0
            fetched_count = 0
            
            if exchange_name == "BitGet":
                strat_name = "Bitget"
                client = trade_bitget(user, "main")
                try:
                    data = client.get_balance_collateral(user)
                    if isinstance(data, str):
                        return False, f"BitGet: {data}"
                    else:
                        total_balance = float(data.get('balance', 0))
                except Exception as e:
                     return False, f"BitGet Error: {e}"

            elif exchange_name == "Hyperliquid":
                strat_name = "HL"
                client = trade_hl(user, "main")
                
                # Dynamic Config Key Detection based on secrets
                config_keys = []
                # Always check for standard 'hyperliquid'
                if user in st.secrets and "hyperliquid" in st.secrets[user]:
                    config_keys.append("hyperliquid")
                
                # Check for secondary keys (hyperliquid2, etc.)
                if user in st.secrets and "hyperliquid2" in st.secrets[user]:
                    config_keys.append("hyperliquid2")
                
                # Fallback if no secrets found (e.g. env vars only)
                if not config_keys:
                     config_keys = ["hyperliquid"]

                for key in config_keys:
                    try:
                        data = client.get_balance_collateral(user, config_key=key)
                        
                        if data is None: continue 
                        
                        if isinstance(data, str):
                             if key == "hyperliquid": 
                                 msgs.append(f"HL ({key}): {data}")
                        else:
                             bal = float(data.get('balance', 0))
                             total_balance += bal
                             fetched_count += 1

                    except Exception as e:
                        msgs.append(f"HL Error ({key}): {e}")
            
            # PnL Calculation
            try:
                prev_df = raw_df[raw_df['strategy'] == strat_name].sort_values('date_world')
                if not prev_df.empty:
                    prev_collateral = float(prev_df.iloc[-1]['collateral'])
                    calc_total_pnl = total_balance - prev_collateral
                else:
                    calc_total_pnl = 0.0

                record = {
                    'date_world': current_date,
                    'strategy': strat_name, 
                    'collateral': total_balance,
                    'total_pnl': calc_total_pnl,
                    'deposit': 0,
                    'withdrawal': 0,
                    'btc_pnl': 0,
                    'eth_pnl': 0,
                    'user_id': user_id_val,
                    'pos_size': 0
                }
                
                if insert_account_data(user, record, table_name):
                    succ_msg = f"{strat_name}: Saved ${total_balance:,.2f} (PnL: {calc_total_pnl:,.2f})"
                    if exchange_name == "Hyperliquid" and fetched_count > 1:
                        succ_msg += f" [Sum of {fetched_count} accounts]"
                    return True, succ_msg
                else:
                    return False, f"{strat_name}: Failed to save data."
            except Exception as e:
                return False, f"{strat_name} Processing Error: {e}"

    except Exception as e:
        return False, f"{exchange_name} Unexpected Error: {e}"
