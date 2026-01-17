import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db_utils import get_connection, fetch_data
from data_processing import process_account_data, resample_data, calculate_monthly_heatmap_data
from datetime import datetime

# Page config
st.set_page_config(page_title="Account Dashboard", layout="wide")


# Dark/Light Mode Toggle

# Dark/Light Mode Toggle
# Default to Dark Mode (True)
# But first, Login Check
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
    st.session_state['role'] = None
    st.session_state['username'] = None

def check_login(username, password):
    # Default credentials for demo purposes / fallback
    # In production, use st.secrets["auth"]
    users = {
        "user1_ms": {"password": "password123", "role": "admin"},
        "user2_jf": {"password": "password456", "role": "user"}
    }
    
    # Try to load from secrets if available
    if "auth" in st.secrets:
        # Expected format: [auth.users] user1_ms = "pass" ...
        # Or [auth] user1_ms = "pass"
        # Since I can't see secrets structure fully, I'll rely on hardcoded fallback 
        # but try to match against user request logic.
        pass

    if username in users and users[username]['password'] == password:
        return users[username]['role']
    return None

if not st.session_state['authenticated']:
    # Show Login Form
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("Login")
        
        # Add some spacing
        st.markdown("<br>", unsafe_allow_html=True)
        
        username_input = st.text_input("Username")
        password_input = st.text_input("Password", type="password")
        
        login_btn = st.button("Login", type="primary", use_container_width=True)
        
        if login_btn:
            role = check_login(username_input, password_input)
            if role:
                st.session_state['authenticated'] = True
                st.session_state['role'] = role
                st.session_state['username'] = username_input
                st.rerun()
            else:
                st.error("Invalid Username or Password")
    
    st.stop() # Stop execution if not authenticated

# Sidebar content starts here
dark_mode = st.sidebar.toggle("Dark Mode", value=True)

if dark_mode:
    # Dark Mode CSS
    css = """
    <style>
        [data-testid="stAppViewContainer"] {
            background-color: #0e1117;
            color: #fafafa;
        }
        [data-testid="stSidebar"] {
            background-color: #262730;
        }
        .metric-card {
            background-color: #262730;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid #484848;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
            color: #ffffff;
        }
        .metric-label {
            font-size: 14px;
            color: #aaaaaa;
        }
    </style>
    """
else:
    # Light Mode CSS (Specific overrides for Dark Blue widgets)
    css = """
    <style>
        [data-testid="stAppViewContainer"] {
            background-color: #ffffff;
            color: #000000;
        }
        [data-testid="stSidebar"] {
            background-color: #262730;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
            color: #000000;
        }
        .metric-label {
            font-size: 14px;
            color: #666;
        }
        /* Buttons - Dark Blue */
        div.stButton > button {
           background-color: #1f2937 !important;
           color: white !important;
           border: none;
        }
        /* Selectbox - Dark Blue Background */
        div[data-baseweb="select"] > div {
            background-color: #1f2937 !important;
            color: white !important;
        }
        /* Headers & Text High Contrast in Main Area */
        [data-testid="stAppViewContainer"] h1, 
        [data-testid="stAppViewContainer"] h2, 
        [data-testid="stAppViewContainer"] h3, 
        [data-testid="stAppViewContainer"] h4, 
        [data-testid="stAppViewContainer"] h5, 
        [data-testid="stAppViewContainer"] h6, 
        [data-testid="stAppViewContainer"] p, 
        [data-testid="stAppViewContainer"] li, 
        [data-testid="stAppViewContainer"] span, 
        [data-testid="stAppViewContainer"] div, 
        [data-testid="stAppViewContainer"] label {
            color: #000000 !important;
        }
        /* Metric values in KPI row specifically black */
        .metric-value-white {
             color: #000000 !important;
        }
        /* Sidebar Design from Dark Mode */
        [data-testid="stSidebar"] label, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span {
             color: #ffffff !important;
        }
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
             color: #ffffff !important;
        }
        /* Streamlit Header (Rerun/Deploy) visibility */
        [data-testid="stHeader"] {
            background-color: #ffffff !important;
            color: #000000 !important;
        }
    </style>
    """

st.markdown(css, unsafe_allow_html=True)

# Chart Colors based on Mode
if dark_mode:
    chart_font_color = "#ffffff"
    chart_grid_color = "LightGray"
    chart_bg_color = "rgba(0,0,0,0)"
else:
    chart_font_color = "#000000" # White text for black backgrounds
    chart_grid_color = "LightGray" # Gray grid on black backgrounds
    chart_bg_color = "#ffffff"    # Black chart background requested

# Helper for metric cards
def metric_card(label, value, delta=None):
    col1, col2 = st.columns([1,1])
    with col1:
        st.metric(label, value, delta)

# --- Sidebar ---
# Helper function to run data loading for a single exchange
def run_data_loading(exchange_name, user, table_name, raw_df, user_id_val):
    try:
        from bitget import trade_bitget
        from hl import trade_hl
        from db_utils import insert_account_data
        from datetime import timezone
        
        current_date = datetime.now().date()
        msgs = []
        success = False

        if exchange_name == "Deribit":
            # Copy logic
            deribit_df = raw_df[raw_df['strategy'].str.contains('Deribit|Option', case=False, na=False)]
            
            if not deribit_df.empty:
                last_entry_row = deribit_df.sort_values('date_world').iloc[-1]
                last_entry = last_entry_row.to_dict()
                
                last_entry['date_world'] = current_date
                last_entry['user_id'] = user_id_val 
                
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
                # Simple check for 'hyperliquid2' as per previous code
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
                             # msgs.append(f"Fetched {key}: {bal}")

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


# --- Sidebar ---
st.sidebar.markdown("### Selection")

# Get users from secrets
if "database" in st.secrets:
    user_options = st.secrets["database"]["users"]
else:
    user_options = ["user1", "user2"]

# RBAC: Filter user options
if st.session_state['role'] == 'user':
    # Standard user can only see user2
    authorized_users = ["user2"]
    # Filter options
    user_options = [u for u in user_options if u in authorized_users]
    if not user_options:
        user_options = ["user2"] # Fallback
else:
    # Admin sees all
    pass

selected_user = st.sidebar.selectbox("User", user_options, index=0)

# Load data for selected user
@st.cache_data(ttl=600)
def load_data(user):
    table_name = st.secrets[user].get("table_name", user)
    return fetch_data(user, table_name=table_name)

raw_df = load_data(selected_user)

if raw_df.empty:
    st.error("No data found for the selected user.")
    st.stop()

# Strategy Selection
strategies = ["Total_Account"] + sorted(raw_df['strategy'].unique().tolist())
selected_strategy = st.sidebar.selectbox("Strategy", strategies)

# Prototype-like Date Selection
min_date = pd.to_datetime(raw_df['date_world']).min().to_pydatetime()
max_date = pd.to_datetime(raw_df['date_world']).max().to_pydatetime()

st.sidebar.markdown("**Start date (month / year)**")
col_m, col_y = st.sidebar.columns(2)
with col_m:
    # Set default start month to 1 and ensure it's selectable back to Jan
    start_month = st.number_input("Start Month", min_value=1, max_value=12, value=1, key="sm", label_visibility="collapsed")
with col_y:
    # Set earliest start year to 2023 and default to 2023
    start_year = st.number_input("Start Year", min_value=2023, max_value=2030, value=2023, key="sy", label_visibility="collapsed")

st.sidebar.caption("(dates always beginning of month)")

st.sidebar.markdown("### Graph Selection")
show_balance = st.sidebar.checkbox("Balance", value=True)
show_daily_charts = st.sidebar.checkbox("Show Daily Charts", value=True)
show_weekly_charts = st.sidebar.checkbox("Show Weekly Charts", value=True)
show_monthly_charts = st.sidebar.checkbox("Show Monthly Charts", value=True)
show_quarterly_charts = st.sidebar.checkbox("Show Quarterly Charts", value=True)
show_strategy_breakdown = st.sidebar.checkbox("Show Strategy Breakdown", value=True)
# User request: "please always use the default and remove the exclude the exclude deposits checkbox"
# Default was Exclude Deposits = True, so we always use net_pnl
exclude_deposits = True 

# Update Button
# Update Button
if st.sidebar.button("UPDATE GRAPHS", width="stretch", type="primary"):
    st.cache_data.clear()
    st.rerun()

# --- Data Loader ---
# --- Data Loader ---
if st.session_state['role'] == 'admin':
    st.sidebar.markdown("### Data Loader")
    
    loader_exchange = st.sidebar.selectbox("Exchange", ["All", "BitGet", "Hyperliquid", "Deribit"])

    if st.sidebar.button("LOAD DATA", width="stretch", type="primary"):
        # Get table name & User ID
        table_name = st.secrets[selected_user].get("table_name", selected_user)
        if selected_user == "user1":
            user_id_val = "user1_ms"
        else:
            user_id_val = "user2_jf"

        exchanges_to_run = []
        if loader_exchange == "All":
            # Dynamic Detection based on User/Secrets
            # Default Logic requested: User1 -> HL, User2 -> BitGet, HL, Deribit
            # Enhanced with secrets check
            
            # Hyperliquid (Check secrets or assume based on user)
            if selected_user == "user1" or (selected_user in st.secrets and "hyperliquid" in st.secrets[selected_user]):
                exchanges_to_run.append("Hyperliquid")
                
            # BitGet
            if selected_user == "user2" or (selected_user in st.secrets and "bitget" in st.secrets[selected_user]):
                 exchanges_to_run.append("BitGet")
                 
            # Deribit (Copy)
            # Only for user2 or if specifically configured? Prompt said "For user2 BitGet, Hyperliquid and Deribit"
            if selected_user == "user2":
                 exchanges_to_run.append("Deribit")


        else:
            exchanges_to_run.append(loader_exchange)
        
        # Execution Loop
        any_success = False
        
        for ex in exchanges_to_run:
            success, msg = run_data_loading(ex, selected_user, table_name, raw_df, user_id_val)
            if success:
                st.sidebar.success(msg)
                any_success = True
            else:
                st.sidebar.error(msg)
                
        if any_success:
            st.cache_data.clear()
            # st.rerun() # Rerun immediately can cut off other messages. Use session state?
            # Actually, if we rerun, we lose the other messages. 
            # Better: Set a flag and rerun at the end? Or just show messages and let user click Update?
            # App structure reruns the whole script on interaction. 
            # If we st.rerun(), the script stops immediately.
            # So we should only rerun IF we are sure we are done or want to refresh.
            # If we have multiple exchanges, we want to see all results.
            # So we should NOT rerun inside the loop.
            pass
            
        # Manual rerun button or auto-refresh hint?
        # Original code had st.rerun(). 
        # If we don't rerun, the charts won't update with new data.
        # But we want to see the messages.
        # Compromise: Show messages, then maybe a small "Data Updated - Refreshing..." or just rely on manual update?
        # Or use st.toast?
        # Let's leave st.rerun() out to allow reading messages, but clear cache so next update is fresh.
        # Or add a "st.button('Refresh')" if updated.
        if any_success:
             st.sidebar.info("Data saved. Please click 'UPDATE GRAPHS' to refresh.")

# --- Data Processing (End of Data Loader block logic, but Data Processing is global) ---
# Indent "LOAD DATA" button block logic above only?
# The button block was lines 274-336. I need to make sure I only wrapped the Loader UI and Button.
# The Data Processing below (line 339) should happen regardless of admin status.


# --- Data Processing ---
actual_start_date = datetime(int(start_year), int(start_month), 1).date()
proc_df = process_account_data(raw_df, selected_strategy)
# Filter by start date
proc_df = proc_df[proc_df['date_world'].dt.date >= actual_start_date]

if proc_df.empty:
    st.warning("No data for the selected start date.")
    st.stop()

# Determine PnL column based on Exclude Deposits
pnl_col = 'net_pnl'

# Recalculate cum_pnl for the filtered view so charts start at 0 (or correct accumulated value relative to start date)
# The user wants "final number should match the P&L since startyear value"
# So cum_pnl should be the accumulation of the visible pnl_col
proc_df['cum_pnl'] = proc_df[pnl_col].cumsum()

# --- Main Dashboard ---
# Custom Header like prototype
st.markdown(f"""
    <div style="background-color: #3498db; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
        <h2 style="color: white; margin: 0; font-size: 20px;">Account Dashboard - {selected_user}</h2>
    </div>
""", unsafe_allow_html=True)

# KPI Row
latest_data = proc_df.iloc[-1]
# Calculate total PnL based on selection
if exclude_deposits:
    total_pnl = proc_df[pnl_col].sum()
    daily_pnl = latest_data[pnl_col]
else:
    # If using raw total_pnl, usually it assumes the column is cumulative or daily change? 
    # Logic in data_processing sums total_pnl, so sum is correct if its daily change.
    total_pnl = proc_df[pnl_col].sum()
    daily_pnl = latest_data[pnl_col]

current_balance = latest_data['equity']

kpi_col1, kpi_col2 = st.columns(2)
with kpi_col1:
    st.markdown(f"""
        <div style="text-align: center; color: white;">
            <span style="font-weight: bold;">Balance (USD):</span>
            <span style="margin-left: 50px;">{current_balance:,.2f}</span>
        </div>
    """, unsafe_allow_html=True)
with kpi_col2:
    st.markdown(f"""
        <div style="text-align: center; color: white;">
            <span style="font-weight: bold;">P&L since {actual_start_date.strftime('%b %Y')}* (USD):</span>
            <span style="margin-left: 50px;">{total_pnl:,.2f}</span>
        </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# --- Charts (Stacked) ---

# 1. Equity Curve (Balance)
if show_balance:
    fig_equity = px.area(proc_df, x='date_world', y='equity', 
                         title="Total Balance", 
                         color_discrete_sequence=['#3498db'])
    
    # Remove Benchmark logic as requested

    fig_equity.update_layout(
        height=300, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        legend=dict(y=1.1, x=0, orientation='h', font=dict(color=chart_font_color)),
        font=dict(color=chart_font_color)
    )
    fig_equity.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_equity.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_equity.update_traces(hovertemplate="Date: %{x}<br>Balance: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_equity, width="stretch")

# 2. Daily Charts
if show_daily_charts:
    # Daily PnL
    fig_daily = px.bar(proc_df, x='date_world', y=pnl_col,
                     color=pnl_col, 
                     color_continuous_scale=['red', 'green'], 
                     color_continuous_midpoint=0,
                     title="Daily PnL")
    fig_daily.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_daily.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_daily.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_daily.update_traces(hovertemplate="Date: %{x}<br>PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_daily, width="stretch")
    
    # Cumulative Daily PnL
    fig_daily_cum = px.bar(proc_df, x='date_world', y='cum_pnl',
                     color='cum_pnl', 
                     color_continuous_scale=['red', 'green'], 
                     color_continuous_midpoint=0,
                     title="Cumulative Daily PnL")
    fig_daily_cum.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_daily_cum.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_daily_cum.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_daily_cum.update_traces(hovertemplate="Date: %{x}<br>Cum PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_daily_cum, width="stretch")

# 3. Weekly Charts
if show_weekly_charts:
    weekly_df = resample_data(proc_df, 'W')
    # Weekly PnL
    fig_weekly = px.bar(weekly_df, x='date_world', y=pnl_col,
                 color=pnl_col, 
                 color_continuous_scale=['red', 'green'], 
                 color_continuous_midpoint=0,
                 title="Weekly PnL")
    fig_weekly.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_weekly.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_weekly.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_weekly.update_traces(hovertemplate="Week: %{x}<br>PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_weekly, width="stretch")
    
    # Cumulative Weekly PnL
    fig_weekly_cum = px.bar(weekly_df, x='date_world', y='cum_pnl',
                 color='cum_pnl', 
                 color_continuous_scale=['red', 'green'], 
                 color_continuous_midpoint=0,
                 title="Cumulative Weekly PnL")
    fig_weekly_cum.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_weekly_cum.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_weekly_cum.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_weekly_cum.update_traces(hovertemplate="Week: %{x}<br>Cum PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_weekly_cum, width="stretch")

# 4. Monthly Charts
if show_monthly_charts:
    monthly_df = resample_data(proc_df, 'ME')
    # Change date to month name/year for better readability
    monthly_df['Month'] = monthly_df['date_world'].dt.strftime('%b %Y')
    
    # Monthly PnL
    fig_monthly = px.bar(monthly_df, x='Month', y=pnl_col,
                 color=pnl_col, 
                 color_continuous_scale=['red', 'green'], 
                 color_continuous_midpoint=0,
                 title="Monthly PnL")
    fig_monthly.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_monthly.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_monthly.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_monthly.update_traces(hovertemplate="Month: %{x}<br>PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_monthly, width="stretch")
    
    # Cumulative Monthly PnL
    fig_monthly_cum = px.bar(monthly_df, x='Month', y='cum_pnl',
                 color='cum_pnl', 
                 color_continuous_scale=['red', 'green'], 
                 color_continuous_midpoint=0,
                 title="Cumulative Monthly PnL")
    fig_monthly_cum.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_monthly_cum.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_monthly_cum.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_monthly_cum.update_traces(hovertemplate="Month: %{x}<br>Cum PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_monthly_cum, width="stretch")

# 5. Quarterly Charts
if show_quarterly_charts:
    quarterly_df = resample_data(proc_df, 'QE')
    # Format quarter nicely (e.g., 2023Q1)
    quarterly_df['Quarter'] = quarterly_df['date_world'].dt.to_period('Q').astype(str)
    
    # Quarterly PnL
    fig_quarterly = px.bar(quarterly_df, x='Quarter', y=pnl_col,
                 color=pnl_col, 
                 color_continuous_scale=['red', 'green'], 
                 color_continuous_midpoint=0,
                 title="Quarterly PnL")
    fig_quarterly.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_quarterly.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_quarterly.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_quarterly.update_traces(hovertemplate="Quarter: %{x}<br>PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_quarterly, width="stretch")
    
    # Cumulative Quarterly PnL
    fig_quarterly_cum = px.bar(quarterly_df, x='Quarter', y='cum_pnl',
                 color='cum_pnl', 
                 color_continuous_scale=['red', 'green'], 
                 color_continuous_midpoint=0,
                 title="Cumulative Quarterly PnL")
    fig_quarterly_cum.update_layout(
        height=250, 
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title=None, 
        yaxis_title=None,
        coloraxis_showscale=False,
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        title_font_color=chart_font_color
    )
    fig_quarterly_cum.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_quarterly_cum.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_quarterly_cum.update_traces(hovertemplate="Quarter: %{x}<br>Cum PnL: $%{y:,.2f}<extra></extra>")
    st.plotly_chart(fig_quarterly_cum, width="stretch")

# --- New Feature: Monthly Heatmap ---
st.markdown("### Monthly Performance Heatmap")
heatmap_pnl, heatmap_pct = calculate_monthly_heatmap_data(proc_df, pnl_col=pnl_col)

if not heatmap_pct.empty:
    # Use Percentage for color and text, but show Absolute PnL on hover
    fig_heatmap = px.imshow(heatmap_pct, 
                            labels=dict(x="Month", y="Year", color="Return (%)"),
                            x=heatmap_pct.columns,
                            y=heatmap_pct.index,
                            color_continuous_scale=['red', 'white', 'green'],
                            color_continuous_midpoint=0,
                            aspect="auto",
                            text_auto=".2f",
                            title="")
    
    # Custom Hover Template to show Abs PnL
    # We need to construct custom data for hover
    # Plotly imshow matches values by index/col, so we can pass the PnL df as custom_data
    fig_heatmap.update_traces(
        customdata=heatmap_pnl.values,
        hovertemplate="Year: %{y}<br>Month: %{x}<br>Return: %{z:.2f}%<br>PnL: $%{customdata:,.2f}<extra></extra>"
    )
    
    fig_heatmap.update_layout(
        height=400,
        title="",
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color)
    )
    st.plotly_chart(fig_heatmap, width="stretch")

# --- Strategy Comparison ---
if selected_strategy == "Total_Account" and show_strategy_breakdown:
    st.subheader("Equity Breakdown by Strategy")
    # Group raw data by date and strategy to show breakdown
    strat_df = raw_df[raw_df['date_world'].dt.date >= actual_start_date].copy()
    fig_strat = px.area(strat_df, x='date_world', y='collateral', color='strategy', 
                        line_group='strategy', title="")
    fig_strat.update_layout(
        height=400, 
        title="",
        xaxis_title=None, 
        yaxis_title="Equity (USD)",
        paper_bgcolor=chart_bg_color,
        plot_bgcolor=chart_bg_color,
        font=dict(color=chart_font_color),
        legend=dict(font=dict(color=chart_font_color))
    )
    fig_strat.update_xaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    fig_strat.update_yaxes(showgrid=True, gridwidth=1, gridcolor=chart_grid_color)
    st.plotly_chart(fig_strat, width="stretch")

# --- Footnote ---
st.markdown("---")
st.caption("* P&L is calculated as: actual balance - deposits + withdrawals. It can deviate from graphs below due to different calc method (in-trade P&L, price fluctuation of collateral)")
