import pandas as pd
import numpy as np

def process_account_data(df: pd.DataFrame, strategy: str = "Total_Account"):
    """
    Processes the raw data to calculate equity and PnL.
    Handles 'Total_Account' (all strategies combined) or specific strategies.
    """
    if df.empty:
        return df

    # Convert date to datetime
    df['date_world'] = pd.to_datetime(df['date_world'])
    
    # Filter by strategy if needed
    if strategy != "Total_Account":
        df = df[df['strategy'] == strategy].copy()
    else:
        # For Total_Account, sum values for each day
        df = df.groupby('date_world').agg({
            'collateral': 'sum',
            'total_pnl': 'sum',
            'deposit': 'sum',
            'withdrawal': 'sum',
            'btc_pnl': 'sum',
            'eth_pnl': 'sum'
        }).reset_index()

    df = df.sort_values('date_world')
    
    # Calculate Equity at End of Day
    # User feedback: "the correct balance should be column collateral only"
    df['equity'] = df['collateral']
    
    # User feedback: "daily pnl should be total_pnl - deposit"
    df['net_pnl'] = df['total_pnl'] - df['deposit']
    
    # Cumulative PnL (Net PnL accumulated)
    df['cum_pnl'] = df['net_pnl'].cumsum()
    
    return df

def resample_data(df: pd.DataFrame, freq: str = 'W'):
    """
    Resamples data to a different frequency (e.g., 'W' for weekly, 'M' for monthly).
    """
    if df.empty:
        return df
        
    df = df.set_index('date_world')
    
    resampled = df.resample(freq).agg({
        'equity': 'last',           # Equity at the end of the period
        'total_pnl': 'sum',         # Total realized pnl during the period
        'net_pnl': 'sum',           # Net PnL (excluding deposits)
        'deposit': 'sum',           # Total deposits during the period
        'withdrawal': 'sum',        # Total withdrawals during the period
        'btc_pnl': 'sum',
        'eth_pnl': 'sum',
        'cum_pnl': 'last'
    })
    
    return resampled.reset_index()

def calculate_monthly_heatmap_data(df: pd.DataFrame, pnl_col: str = 'total_pnl'):
    """
    Prepares data for the monthly returns heatmap.
    Returns a pivot table for PnL (absolute) and one for returns (percentage).
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
        
    # Ensure we have datetime index
    df = df.copy()
    df['date_world'] = pd.to_datetime(df['date_world'])
    df['year'] = df['date_world'].dt.year
    df['month_num'] = df['date_world'].dt.month
    df['month_name'] = df['date_world'].dt.month_name()
    
    # Group by Year-Month
    # To Calc %, we need PnL Sum / Starting Equity of that month
    # We take the standard approach: Sum PnL for month. 
    # For denominator, we take the equity at the END of the PREVIOUS month, 
    # or the Collateral at the START of the current month (first available record).
    
    monthly_stats = []
    
    for (year, month), group in df.groupby(['year', 'month_num']):
        total_pnl = group[pnl_col].sum()
        
        # Get starting equity (collateral + pnl at start basically, or just first record's collateral if it resets? 
        # Actually equity = collateral + total_pnl. 
        # But for ROI, usually it's PnL / Capital.
        # Let's use the first available 'collateral' of the month as a proxy for Capital base if 'equity' varies wildy
        # Or better: Previous month's end equity.
        
        # Simple approximation: Average collateral of the month or First collateral.
        # Let's use First Collateral of the month.
        start_equity = group.iloc[0]['collateral']
        
        # Avoid division by zero
        pct_return = (total_pnl / start_equity * 100) if start_equity != 0 else 0
        
        monthly_stats.append({
            'year': year,
            'month': group.iloc[0]['month_name'], # Use name from first record
            'total_pnl': total_pnl,
            'pct_return': pct_return
        })
        
    monthly_df = pd.DataFrame(monthly_stats)
    
    if monthly_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Pivot for Heatmaps
    pnl_pivot = monthly_df.pivot(index='year', columns='month', values='total_pnl')
    pct_pivot = monthly_df.pivot(index='year', columns='month', values='pct_return')
    
    # Sort months correctly
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    # Reindex checks if cols exist, ignore missing
    existing_months = [m for m in month_order if m in pnl_pivot.columns]
    pnl_pivot = pnl_pivot.reindex(columns=existing_months)
    pct_pivot = pct_pivot.reindex(columns=existing_months)
    
    return pnl_pivot, pct_pivot
