

import numpy as np
import pandas as pd
import streamlit as st

from dotenv import load_dotenv
load_dotenv()
import os
import traceback

import ccxt

#from . import datatable

 
######## HyperLiquid ############################################################################################################



# ----------------------------------- Init ----------------------------------------------------------------------------------------------------

# ------------------------ ADD USERS ------------------------------------
# 1) Add user to users list (function read_user_list)
# 2) add api_key (function read_api_key_list)
# 3) add api_secret (function read_api_secret_list)



class trade_bitget():
  
    ''' Class for trading the Binance Spot market for specific user.
    
    Attributes
    ============
    currently 3 different approaches for derivatives - USDC selected:
    a. USDT Perpetual: USDT, USDT as Collateral (One-way and Hedge mode)
    b. USDC Perpetual: USDC and Options, USDC as Collateral (Cross and isolated margin)
    c. Inverse Perpetual: BTC as collateral

    Fees:
    Maker: 0.01%, Taker: 0.06%
    
    Methods
    =======
    read_api_key_secret(self, user) - reads the api key and secret for specific user
    
    write_table_close_signal(self, user, asset, direction) - writes close order in trading table for specific user and asset
    write_table_open_signal(self, user, asset, direction, trade_multi) - writes open order in trading table for specific user ans asset
      directon: long or short, trade_multi = % of available collateral (acc. investment table) to be invested
    write_order_table(self, user, subaccount, asset, trade_direction, order_size, price_asset, close_order, trailing_stop)
      support function for write open and close table; performs actual wrting of table with add. trading parameters such as trailing stop
      
    auto_execute_orders(user) - execute orders listed in order table

    get_balance_collateral(user) - query balance & collateral of user
    get_asset_pos(user, asset) - query position of specific asset of a user
    get_price_asset(self, asset) - query asset price
        
    '''      
    
    def __init__(self, user, subaccount):              
        self.results = None
        #user = user[:8]

        # self.df_api removed - now using st.secrets or os.environ

        self.invest_cap = 1.0     

        # Initialize the singleton cache
        self._exchange_cache = {}      

    def read_api_key_secret_bitget(self, user):
        """
        Reads API credentials from streamlit secrets or environment variables.
        1. Checks st.secrets[user]["bitget"]
        2. Checks os.environ for BITGET_{USER}_API_KEY etc.
        """
        # 1. Try Secrets
        try:
            if user in st.secrets and "bitget" in st.secrets[user]:
                creds = st.secrets[user]["bitget"]
                return {
                    'api_key': creds.get('api_key'),
                    'api_secret': creds.get('api_secret'),
                    'passphrase': creds.get('passphrase')
                }
        except Exception:
            pass # Fallback to env

        # 2. Try Environment Variables
        try:
            # Construct keys based on user (e.g. user1 -> BITGET_USER1_API_KEY)
            user_env = user.upper()
            api_key = os.environ.get(f"BITGET_{user_env}_API_KEY")
            api_secret = os.environ.get(f"BITGET_{user_env}_API_SECRET")
            passphrase = os.environ.get(f"BITGET_{user_env}_PASSPHRASE")

            if api_key and api_secret and passphrase:
                return {
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'passphrase': passphrase
                }
        except Exception as e:
            print(f"Error checking env vars for {user}: {e}")

        raise ValueError(f"No Bitget credentials found for user {user} in st.secrets or environment variables")
    
    def init_exchange(self, user, subaccount=None):
        """
        Retrieves or creates a cached CCXT Bitget instance.
        Ensures load_markets() is called only once per user/subaccount.
        """
        # Create unique key for cache
        # Handle None or 'main' as equivalent for caching purposes
        sub_key = subaccount if subaccount and subaccount.lower() != 'main' else 'main'
        cache_key = f"{user}_{sub_key}"
        
        # Return existing instance if available
        if cache_key in self._exchange_cache:
            return self._exchange_cache[cache_key]
            
        print(f"Initializing new CCXT Bitget connection for {cache_key}...")
        
        try:
            # 1. Get Credentials
            api_data = self.read_api_key_secret_bitget(user)
            
            required_keys = ['api_key', 'api_secret', 'passphrase']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing {key} in API credentials")
            
            # 2. Initialize CCXT
            exchange = ccxt.bitget({
                'apiKey': api_data['api_key'],
                'secret': api_data['api_secret'],
                'password': api_data['passphrase'],
                'enableRateLimit': True
            })
            
            # 3. Configure Subaccount
            if subaccount and subaccount.lower() != 'main':
                exchange.options['defaultSubaccount'] = subaccount
                
            # 4. Load Markets (Expensive operation - done once)
            exchange.load_markets()
            
            # 5. Cache and Return
            self._exchange_cache[cache_key] = exchange
            print(f"Successfully cached connection for {cache_key}")
            return exchange
            
        except Exception as e:
            # Propagate error but don't cache failed instances
            print(f"Error initializing exchange for {user}: {str(e)}")
            raise e

    def highest_bid_order(self, df_open_orders):
      return 0.0
    
    def lowest_ask_order(self, df_open_orders):
      return 99999.0

    def order_price(self, direction, coin, limit_price, best_limit, subaccount, user):
        # identify the right order price dependant on selection of best limit order checkbox
        # direction = 'long' or 'short'
        # coin e.g. 'BTC-PERP'
        # limit_price: limit price input
        # best limit = use best limit: 'yes' or ''; otherwise usage of limit price input
        if best_limit:
          limit_price_f = 50000.0 #best_order_price(direction,coin,subaccount, user)
        else:
          if limit_price:
            limit_price_f = float(limit_price)
          else:
            limit_price_f = 0.0
        return limit_price_f


    def execute_order(self, direction, asset, market_order, no_coins, target_pos_f, batch_size_f, limit_price_f, subaccount, user, stopLoss=False, sl_price_f=0.0, takeProfit=False, tp_price_f=0.0, invest_share=0.0):
        """
        Execute a market or limit order on a cryptocurrency exchange.
        
        Parameters:
        -----------
        direction : str
            Trading direction, must be either 'long' or 'short'
        asset : str
            Trading pair/asset symbol (e.g., 'BTC-PERP')
        market_order : bool or str
            Whether to use market order (True/'TRUE') or limit order (False/'')
        no_coins : float
            Current number of coins in position
        target_pos_f : float
            Target position size
        batch_size_f : float
            Maximum size of individual order batch
        limit_price_f : float
            Limit price for the order (used only for limit orders)
        subaccount : str
            Trading subaccount name, 'main' for primary account
        user : str
            User identifier for the trade
        stopLoss : bool, optional
            Whether to set a stop loss, defaults to False
        sl_price_f : float, optional
            Stop loss price, defaults to 0.0
        takeProfit : bool, optional
            Whether to set a take profit, defaults to False
        tp_price_f : float, optional
            Take profit price, defaults to 0.0
        invest_share : float, optional
            Investment share percentage, defaults to 0.0
            
        Returns:
        --------
        str
            'success' if order execution attempted, regardless of outcome
        """

        print(f"Executing {direction} order for {asset} at {'market price' if market_order else limit_price_f} with target_pos {target_pos_f} and current {no_coins}")

        # Input validation
        if not isinstance(direction, str) or direction.lower() not in ['long', 'short']:
            raise ValueError("Direction must be either 'long' or 'short'")
        
        if not asset or not isinstance(asset, str):
            raise ValueError(f"Asset for {asset} must be a valid trading symbol string")
        
        if not isinstance(no_coins, (int, float)):
            raise ValueError(f"Current position size for {asset} must be a number")
        
        if not isinstance(target_pos_f, (int, float)):
            raise ValueError(f"Target position for {asset}must be a number")
        
        if not isinstance(batch_size_f, (int, float)) or batch_size_f <= 0:
            raise ValueError(f"Batch size for {asset} must be a positive number")
        
        if not isinstance(limit_price_f, (int, float)) or limit_price_f < 0:
            raise ValueError(f"Limit price for {asset} must be a positive number")
            
        # Convert market_order to boolean if it's a string
        if isinstance(market_order, str):
            market_order = (market_order.upper() == 'TRUE')
        
        # Normalize direction to lowercase for case-insensitive comparison
        direction = direction.lower()
        
        # Set subaccount name
        if subaccount.lower() == 'main':
            subname = None
        else:
            subname = subaccount
            
        # Get minimum size increment (ideally this should come from the exchange API)
        # TODO: Replace hardcoded value with dynamic fetch from exchange
        size_increment = 0.001  # for Bitcoin client.get_market(asset)['sizeIncrement']
        
        # Calculate trade size based on direction
        if direction == 'long':
            # For long positions, we're buying to increase our position
            # We either buy the batch size or what's left to reach our target (whichever is smaller)
            # But always at least the minimum size increment
            trade_size = max(min((target_pos_f - no_coins), batch_size_f), size_increment)
            side = 'buy'
        elif direction == 'short':
            # For short positions, we're selling to decrease our position
            # We either sell the batch size or what's left to reach our target (whichever is smaller)
            # But always at least the minimum size increment
            trade_size = max(min((no_coins - target_pos_f), batch_size_f), size_increment)
            side = 'sell'
        
        # Convert trade size to string with appropriate precision
        trade_coins = str(round(trade_size, 6))  # Using 6 decimal precision for most crypto assets
        
        # Set order type and price
        orderType = 'market' if market_order else 'limit'
        orderPrice = None if market_order else str(limit_price_f)
        
        # Execute the trade with error handling
        try:
            # Additional validation for stop loss and take profit
            if stopLoss and (not isinstance(sl_price_f, (int, float)) or sl_price_f <= 0):
                raise ValueError("Stop loss price must be a positive number")
            
            if takeProfit and (not isinstance(tp_price_f, (int, float)) or tp_price_f <= 0):
                raise ValueError("Take profit price must be a positive number")
            
            # Validate stop loss and take profit logic based on direction
            if stopLoss and direction == 'long' and sl_price_f >= limit_price_f:
                raise ValueError("Stop loss price must be below entry price for long positions")
            
            if stopLoss and direction == 'short' and sl_price_f <= limit_price_f:
                raise ValueError("Stop loss price must be above entry price for short positions")
            
            if takeProfit and direction == 'long' and tp_price_f <= limit_price_f:
                raise ValueError("Take profit price must be above entry price for long positions")
            
            if takeProfit and direction == 'short' and tp_price_f >= limit_price_f:
                raise ValueError("Take profit price must be below entry price for short positions")
                
            # Log trade information before execution
            print(f"Executing {orderType} {side} order for {trade_coins} {asset} at {'market price' if market_order else limit_price_f}")
            
            # Execute the trade
            result = self.execute_trade_bitget(
                user=user,
                subaccount=subaccount,
                symbol=asset,
                orderType=orderType,
                side=side,
                orderQty=trade_coins,
                orderPrice=orderPrice,
                stopLoss=stopLoss,
                sl_price_f=sl_price_f,
                takeProfit=takeProfit,
                tp_price_f=tp_price_f,
                invest_share=invest_share
            )
            print(f"{orderType} Order: {result}")
            return f"success: {orderType} order executed"
        except Exception as e:
            # Capture and log the specific error
            error_message = str(e)
            print(f"{orderType} Order failed: {error_message}")
            # Log the error with detailed information for debugging
            traceback.print_exc()
            return f"error: {error_message}"



    def execute_trade_bitget(self, user='user2_jf', subaccount='main', symbol='BTC/USDC:USDC', 
                            orderType='Market', side=None, orderQty='0.0', orderPrice='', 
                            stopLoss=False, sl_price_f='', takeProfit=False, tp_price_f='', 
                            invest_share=0.0):
        """
        Execute a trade on the Bitget exchange.
        
        Parameters:
        -----------
        user : str
            User identifier for API credentials, defaults to 'user1_ms'
        subaccount : str
            Trading subaccount name, 'main' for primary account
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT_UMCBL')
        orderType : str
            Type of order ('Market' or 'Limit')
        side : str
            Trade direction ('Buy' or 'Sell')
        orderQty : str
            Quantity to trade
        orderPrice : str
            Price for limit orders (ignored for market orders)
        stopLoss : bool
            Whether to set a stop loss
        sl_price_f : str or float
            Stop loss price
        takeProfit : bool
            Whether to set a take profit
        tp_price_f : str or float
            Take profit price
        invest_share : float
            Investment amount in USDT to determine position size
            
        Returns:
        --------
        dict or str
            API response or error message
        """

        min_size_usd = 15 # Ob HyperLiquid minimum trade size is 10 USD

        # Input validation
        if side is None:
            raise ValueError("Side parameter (Buy/Sell) is required")
        
        if not isinstance(side, str) or side.lower() not in ['buy', 'sell']:
            raise ValueError("Side must be either 'Buy' or 'Sell'")
        
        if not isinstance(orderType, str) or orderType.lower() not in ['market', 'limit']:
            raise ValueError("Order type must be either 'Market' or 'Limit'")
        
        # Validate quantity
        try:
            float_qty = float(orderQty)
            if float_qty <= 0:
                raise ValueError("Order quantity must be greater than zero")
        except ValueError:
            raise ValueError(f"Invalid order quantity: {orderQty}. Must be a valid number.")
        
        # Validate price for limit orders
        if orderType.lower() == 'limit':
            if not orderPrice:
                raise ValueError("Price is required for limit orders")
            try:
                float_price = float(orderPrice)
                if float_price <= 0:
                    raise ValueError("Order price must be greater than zero")
            except ValueError:
                raise ValueError(f"Invalid price: {orderPrice}. Must be a valid number.")
        
        # Validate stop loss and take profit prices if enabled
        if stopLoss:
            try:
                sl_price = float(sl_price_f)
                if sl_price <= 0:
                    raise ValueError("Stop loss price must be greater than zero")
                
                # Validate stop loss logic based on side (if price is provided)
                if orderType.lower() == 'limit' and orderPrice:
                    limit_price = float(orderPrice)
                    if side.lower() == 'buy' and sl_price >= limit_price:
                        raise ValueError("Stop loss price should be below entry price for long positions")
                    if side.lower() == 'sell' and sl_price <= limit_price:
                        raise ValueError("Stop loss price should be above entry price for short positions")
            except ValueError as e:
                if "Stop loss price should be" not in str(e):
                    raise ValueError(f"Invalid stop loss price: {sl_price_f}. Must be a valid number.")
                else:
                    raise e
        
        if takeProfit:
            try:
                tp_price = float(tp_price_f)
                if tp_price <= 0:
                    raise ValueError("Take profit price must be greater than zero")
                
                # Validate take profit logic based on side (if price is provided)
                if orderType.lower() == 'limit' and orderPrice:
                    limit_price = float(orderPrice)
                    if side.lower() == 'buy' and tp_price <= limit_price:
                        raise ValueError("Take profit price should be above entry price for long positions")
                    if side.lower() == 'sell' and tp_price >= limit_price:
                        raise ValueError("Take profit price should be below entry price for short positions")
            except ValueError as e:
                if "Take profit price should be" not in str(e):
                    raise ValueError(f"Invalid take profit price: {tp_price_f}. Must be a valid number.")
                else:
                    raise e
        
        # Validate invest_share
        if invest_share < 0:
            raise ValueError("Investment share cannot be negative")
        
        # Normalize symbol format
        symbol = symbol.upper()
        if '/USDT:USDT' not in symbol:
            symbol = symbol + '/USDT:USDT'

        if '/USDT:USDT' in symbol:
                symbol_short = symbol.replace('/USDT:USDT', '')
        else:
                symbol_short = symbol 

        # Convert order type and side to expected format
        orderTypeLower = orderType.lower()
        side_out = 'buy' if side.lower() == 'buy' else 'sell'
        reduce_only = False # if side.lower() == 'buy' else True ###################### TO BE CHECKED & UPDATED

        
        # Log trade information
        print(f'User {user} to {side_out} {orderQty} {symbol} with order type {orderTypeLower} at price {orderPrice}')
        
        """ ### Old Init CCXT client ####
        # Get API credentials based on user
        try:
            api_data = self.read_api_key_secret_bitget(user)
                
            # Validate API data
            required_keys = ['api_key', 'api_secret', 'passphrase']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
        except Exception as e:
            error_msg = f"Failed to retrieve API credentials: {str(e)}"
            print(error_msg)
            return error_msg
        
        # Initialize CCXT client with error handling
        try:
            
            # Create exchange instance with the API credentials
            exchange = ccxt.bitget({
                'apiKey': api_data['api_key'],
                'secret': api_data['api_secret'],
                'password': api_data['passphrase'],
                'enableRateLimit': True
            })
            
            # Set up subaccount if needed
            if subaccount:
                exchange.options['defaultSubaccount'] = subaccount
                
        except Exception as e:
            error_msg = f"Failed to initialize CCXT Bitget client: {str(e)}"
            print(error_msg)
            return error_msg """
        
        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg        
        
        # Prepare order parameters
        order_params = { "reduceOnly": reduce_only} # long only i.e. for shorts reduce only = True
        #order_params = {
        #    "symbol": symbol,
        #    "marginCoin": "USDT",  # Using USDT as margin currency
        #    "size": orderQty,
        #    "side": side_out,
        #    "orderType": orderTypeLower,
        #    "price": orderPrice
        #}
        
        index_price = float(self.get_price_asset(symbol_short))
        if index_price <= 0:
            raise ValueError(f"Invalid index price: {index_price}")

        # Add stop loss if enabled
        if stopLoss:
            params={"stopLossPrice": sl_price_f, "reduceOnly": True}
        
        # Add take profit if enabled
        if takeProfit:
            params={"takeProfitPrice": tp_price_f, "reduceOnly": True}
        
        # Calculate position size based on investment amount if specified
        if invest_share > 0.0:
            try:
                # Calculate and round position size
                position_size = invest_share / index_price
                
                # Apply minimum position size check (assuming 0.001 as minimum)
                min_size = 0.001
                if position_size < min_size:
                    raise ValueError(f"Calculated position size {position_size} is below minimum {min_size}")
                    
                # Round to 3 decimal places (or appropriate precision for the asset)
                order_size = round(position_size, 3)
                print(f"Using investment amount: {invest_share} USDT at price {index_price} → {order_size} {symbol}")

            except Exception as e:
                error_msg = f"Failed to calculate position size from investment amount: {str(e)}"
                print(error_msg)
                return error_msg
            
        else:
            # Check if order reaches min amount
            if (float_qty * index_price) < min_size_usd:
                order_size = round(min_size_usd/index_price, 3)
            else:
                order_size = round(float_qty, 3)

        print(f"Order size for {symbol}: {side_out} order with {order_size} of {orderTypeLower} at price {index_price} and params {order_params}")
        # Execute order with error handling
        try:
            # IMPORTANT: The line below is commented out for testing
            # Uncomment in production to actually place orders
            #order_response = 'Test'
            order_response = exchange.create_market_order(symbol=symbol, side=side_out, amount=order_size, params=order_params)
            
            # Log the response
            print(f"Order response: {order_response}")
            return order_response
        
        except Exception as e:
            # Detailed error handling
            error_msg = f"Order execution failed: {str(e)}"
            print(f'Order data - {symbol}, {orderTypeLower}, {side_out}, {order_size}, {index_price}, {order_params}')
            print(error_msg)
            
            # Print stack trace for debugging
            traceback.print_exc()
            
            return f"Error: {error_msg}"


    def get_balance_collateral(self, user, subaccount=None):
        """
        Retrieve the maximum available cross margin collateral from a Bitget account using CCXT.
        
        Parameters:
        -----------
        user : str
            User identifier for API credentials
        subaccount : str, optional
            Trading subaccount name, None for primary account
            
        Returns:
        --------
        str or float
            Available collateral amount or error message
        """
        # Input validation
        if not user:
            error_msg = "User parameter is required"
            print(error_msg)
            return error_msg
        
        """ ### Old Init CCXT client ####
        # Get API credentials based on user and subaccount
        try:
            api_data = self.read_api_key_secret_bitget(user)
                
            #print(f'API Data for {user}: {api_data}')
            # Validate API data structure
            required_keys = ['api_key', 'api_secret', 'passphrase']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
                    
        except Exception as e:
            error_msg = f"Failed to retrieve API credentials: {str(e)}"
            print(error_msg)
            return error_msg
            
        # Initialize CCXT client with error handling
        try:
            
            # Create Bitget exchange instance with the API credentials
            exchange = ccxt.bitget({
                'apiKey': api_data['api_key'],
                'secret': api_data['api_secret'],
                'password': api_data['passphrase'],
                'enableRateLimit': True
            })
            
            # Set up subaccount if needed
            if subaccount:
                exchange.options['defaultSubaccount'] = subaccount
                
        except Exception as e:
            error_msg = f"Failed to initialize CCXT Bitget client: {str(e)}"
            print(error_msg)
            return error_msg """
            
        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg               
            
        # Retrieve account collateral information
        try:
            # In CCXT, we need to fetch futures account balance
            # The exact method may vary depending on the exchange and CCXT version
            # For Bitget in CCXT, we'll use fetchBalance with the appropriate account type
            
            raw_data = {}
            # Fetch balance for USDT-margined futures account
            balance = exchange.fetch_balance({'type': 'swap'})
            #print('Balance: ', balance['info'])
            
            # Try to find the cross collateral in the raw response
            raw_data['balance'] = balance['info'][0]['available']
            raw_data['free_collateral'] = balance['info'][0]['crossedMaxAvailable']
            balance_print = raw_data['balance']
            collateral_print = raw_data['free_collateral']
            print(f'{user} with Balance: {balance_print} and Collateral: {collateral_print}')            
            
                
        except Exception as e:
            # Handle specific API errors
            error_msg = f"Failed to retrieve collateral: {str(e)}"
            print(error_msg)
            # Print detailed error information for debugging
            import traceback
            traceback.print_exc()
            return f"Error: {error_msg}"

        return raw_data

 

    def get_asset_pos(self, user, subaccount, asset):
        """
        Retrieve the current position size for a specific asset from a Bitget account using CCXT.
        
        Parameters:
        -----------
        user : str
            User identifier for API credentials
        subaccount : str
            Trading subaccount name, None or 'main' for primary account
        asset : str
            Asset symbol to query (e.g., 'BTC', 'BTC/USDT:USDT')
            
        Returns:
        --------
        float
            Current position size (positive for long, negative for short, 0 if no position or error)
        """
        # Input validation
        if not user:
            print("Error: User parameter is required")
            return 0.0
        #user = user[:8]

        if not asset:
            print("Error: Asset parameter is required")
            return 0.0
            
        # Normalize subaccount parameter
        if subaccount is None or subaccount.lower() == 'main':
            subaccount_name = None
        else:
            subaccount_name = subaccount
            
        # Extract base asset symbol from full name
        try:
            if '/USDT:USDT' in asset:
                asset_short = asset.replace('/USDT:USDT', '')
            else:
                asset_short = asset
                
            # Validate asset symbol (basic check)
            if not asset_short or len(asset_short.strip()) < 1:
                print(f"Error: Invalid asset symbol: {asset}")
                return 0.0
                
        except Exception as e:
            print(f"Error processing asset symbol: {str(e)}")
            return 0.0
            
        """ ### Old Init CCXT client ####            
        # Get API credentials based on user and subaccount
        try:
            api_data = self.read_api_key_secret_bitget(user)
                
            # Validate API data structure
            required_keys = ['api_key', 'api_secret', 'passphrase']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
                    
        except Exception as e:
            print(f"Failed to retrieve API credentials: {str(e)}")
            return 0.0
            
        # Initialize CCXT client with error handling
        try:
            #import ccxt
            
            # Create Bitget exchange instance with the API credentials
            exchange = ccxt.bitget({
                'apiKey': api_data['api_key'],
                'secret': api_data['api_secret'],
                'password': api_data['passphrase'],
                'enableRateLimit': True
            })
            
            # Set up subaccount if needed
            if subaccount_name:
                exchange.options['defaultSubaccount'] = subaccount_name
                
        except Exception as e:
            print(f"Failed to initialize CCXT Bitget client: {str(e)}")
            return 0.0 """
        
        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg           
            
        # Retrieve position data with comprehensive error handling
        try:
            ccxt_symbol = f"{asset_short}/USDT:USDT"

            # Load all positions from the futures account
            positions = exchange.fetchPositions()
            
            # Initialize position size
            pos_size = 0.0
            
            # Handle case when no positions exist
            if not positions or len(positions) == 0:
                print(f"No positions found for user {user} in subaccount {subaccount}")
                return pos_size
                
            # Iterate through positions to find matching asset
            for position in positions:
                # Check if position data has the required structure
                if not isinstance(position, dict) or 'symbol' not in position:
                    print(f"Warning: Invalid position data structure: {position}")
                    continue
                    
                symbol = position['symbol']
                
                # Check if this position matches our target asset
                if asset_short.lower() in symbol.lower():
                    try:
                        # In CCXT, contracts is the size and side determines direction
                        size = float(position.get('contracts', 0))
                        side = position.get('side', '').lower()
                        
                        # Calculate position size based on direction
                        if side == 'long':
                            pos_size += size
                        elif side == 'short':
                            pos_size -= size
                        else:
                            # Alternate approach if side is not provided
                            notional = float(position.get('notional', 0))
                            if notional > 0:
                                pos_size += size
                            elif notional < 0:
                                pos_size -= size
                                
                    except ValueError as e:
                        print(f"Error converting position size: {str(e)}")
                        continue
                        
            print(f'Position for asset: {asset_short} for user: {user} → {pos_size}')
            return pos_size
            
        except Exception as e:
            # Handle any unexpected errors
            print(f"Error retrieving position data: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return zero as fallback
            return 0.0


    def get_all_positions(self, user, subaccount):
        """
        Retrieve the current position size for a specific asset from a Bitget account using CCXT.
        
        Parameters:
        -----------
        user : str
            User identifier for API credentials
        subaccount : str
            Trading subaccount name, None or 'main' for primary account
            
        Returns:
        --------
        pandas df
            All current positions
        """
        # Input validation
        if not user:
            print("Error: User parameter is required")
            return 0.0
        #user = user[:8]
            
        # Normalize subaccount parameter
        if subaccount is None or subaccount.lower() == 'main':
            subaccount_name = None
        else:
            subaccount_name = subaccount
            
        """ ### Old Init CCXT client
        # Get API credentials based on user and subaccount
        try:
            #if user == 'user1_ms':
             #   api_data = self.read_api_key_secret_bitget_user1(subaccount_name)
            #else:
            api_data = self.read_api_key_secret_bitget(user)
                
            # Validate API data structure
            required_keys = ['api_key', 'api_secret', 'passphrase']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
                    
        except Exception as e:
            print(f"Failed to retrieve API credentials: {str(e)}")
            return 0.0
            
        # Initialize CCXT client with error handling
        try:
            #import ccxt
            
            # Create Bitget exchange instance with the API credentials
            exchange = ccxt.bitget({
                'apiKey': api_data['api_key'],
                'secret': api_data['api_secret'],
                'password': api_data['passphrase'],
                'enableRateLimit': True
            })
            
            # Set up subaccount if needed
            if subaccount_name:
                exchange.options['defaultSubaccount'] = subaccount_name
                
        except Exception as e:
            print(f"Failed to initialize CCXT Bitget client: {str(e)}")
            return 0.0 """
        
        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg           

        # Retrieve position data with comprehensive error handling

        try:      
            # Load all positions from the futures account
            positions = exchange.fetchPositions()

            # json_normalize to flatten nested structures
            df = pd.json_normalize(positions, sep='_')   # → Spalten wie info_marginCoin, info_unrealizedPL, …

            # convert numeric columns
            for col in df.select_dtypes(include='object').columns:
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass
                    
            # convert to base currency        
            df['symbol'] = df['symbol'].str.split('/', n=1).str[0].str.lower()
            df_pos = df[['symbol', 'contracts', 'side', 'notional', 'entryPrice', 'leverage', 'unrealizedPnl']].copy()

            side = df_pos['side']
            df_pos['contracts'] = np.where(side == 'short',  -df['contracts'], df['contracts'])            

            symbol = df_pos['symbol']

            #print(f'All positions for user: {user}, subaccount: {subaccount}\n', df_pos)
            return df_pos 
            
        except Exception as e:
            # Handle any unexpected errors
            print(f"Error retrieving position data: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return zero as fallback
            return 0.0

   
    def get_price_asset(self, asset, user='user2_jf', subaccount=''):
        """
        Retrieve the current index price for a cryptocurrency asset from Bitget.
        
        Parameters:
        -----------
        asset : str
            Trading symbol/asset (e.g., 'BTC', 'ETH', 'BTCUSDT_UMCBL')
            
        Returns:
        --------
        float or str
            Current index price of the asset or error message
        """
        # Input validation
        if not user:
            print("Error: User parameter is required")
            return 0.0
        if not asset:
            print("Error: Asset parameter is required")
            return 0.0
            
        # Normalize subaccount parameter
        if subaccount is None or subaccount.lower() == 'main':
            subaccount_name = None
        else:
            subaccount_name = subaccount

        # Normalize asset & handle special cases
        asset = asset.upper()   
        if 'LUNA' in asset:
            asset = asset.replace('LUNA', 'LUNA2')
            print(f"LUNA detected: converted to {asset}")

        # Extract base asset symbol from full name
        try:
            if 'USDT_UMCBL' in asset:
                asset_short = asset.replace('USDT_UMCBL', '')
            else:
                asset_short = asset
                
            # Validate asset symbol (basic check)
            if not asset_short or len(asset_short.strip()) < 1:
                print(f"Error: Invalid asset symbol: {asset}")
                return 0.0
                
        except Exception as e:
            print(f"Error processing asset symbol: {str(e)}")
            return 0.0

        print(f'Getting price for asset: {asset_short} for user: {user}, subaccount: {subaccount}')

        """ ### Old Init CCXT client ####
        # Get API credentials based on user and subaccount
        try:
            api_data = self.read_api_key_secret_bitget(user)
                
            # Validate API data structure
            required_keys = ['api_key', 'api_secret', 'passphrase']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
                    
        except Exception as e:
            print(f"Failed to retrieve API credentials: {str(e)}")
            return 0.0
            
        # Initialize CCXT client with error handling
        try:
            
            # Create exchange instance with the API credentials
            exchange = ccxt.bitget({
                'apiKey': api_data['api_key'],
                'secret': api_data['api_secret'],
                'password': api_data['passphrase'],
                'enableRateLimit': True
            })
            
            # Set up subaccount if needed
            if subaccount:
                exchange.options['defaultSubaccount'] = subaccount
                
        except Exception as e:
            error_msg = f"Failed to initialize CCXT Bitget client: {str(e)}"
            print(error_msg)
            return error_msg """

        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg   
        
        # Retrieve account collateral information
        try:
            # Format the symbol for CCXT (e.g., 'BTC/USDT:USDT' for futures)
            ccxt_symbol = f"{asset_short}/USDT:USDT"

            # Extract and validate index price
            ticker = exchange.fetch_ticker(ccxt_symbol)
            price_perp = ticker['last']
            
            # Convert to float for validation
            try:
                price_float = float(price_perp)
                if price_float <= 0:
                    print(f"Warning: Unusually low price detected: {price_float}")
            except ValueError:
                print(f"Warning: Price is not a valid number: {price_perp}")
            
            # Log the retrieved price
            print(f"Successfully retrieved price for {ccxt_symbol}: {price_perp}")
            return price_perp
                
        except Exception as e:
            # Handle specific API errors
            error_msg = f"Failed to retrieve collateral: {str(e)}"
            print(error_msg)
            # Print detailed error information for debugging
            traceback.print_exc()
            return f"Error: {error_msg}"


  
    def get_unrealized_pnl(self, user, subaccount=None):
        """
        Retrieve the current position size for a specific asset from a Bitget account using CCXT.
        
        Parameters:
        -----------
        user : str
            User identifier for API credentials
        subaccount : str
            Trading subaccount name, None or 'main' for primary account
            
        Returns:
        --------
        float
            Current total unrealized pnl  (sum of all individual pnl)
        """
     
        # Input validation
        if not user:
            print("Error: User parameter is required")
            return 0.0
            
        # Normalize subaccount parameter
        if subaccount is None or subaccount.lower() == 'main':
            subaccount_name = None
        else:
            subaccount_name = subaccount
    
        print(f'Getting unrealized pnl for user: {user}, subaccount: {subaccount}')

        """ ### Old Init CCXT client ####
        # Get API credentials based on user and subaccount
        try:
            #if user == 'user1_ms':
            #api_data = self.read_api_key_secret_hl_user1(subaccount_name)
            #else:
            api_data = self.read_api_key_secret_hl(user)
                
            # Validate API data structure
            required_keys = ['api_key', 'api_secret']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
                    
        except Exception as e:
            print(f"Failed to retrieve API credentials: {str(e)}")
            return 0.0
            
        # Initialize CCXT client with error handling
        try:
            #import ccxt
            
            # Create exchange instance with the API credentials
            exchange = ccxt.hyperliquid({
                "walletAddress": api_data['api_key'],
                "privateKey": api_data['api_secret']
            })
            
            # Set up subaccount if needed
            if subaccount:
                exchange.options['defaultSubaccount'] = subaccount
                
        except Exception as e:
            print(f"Failed to initialize CCXT HyperLiquid client: {str(e)}")
            return 0.0 """
        
        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg           
            
        # Retrieve position data with comprehensive error handling
        try:
   
            # Set initial pnl to 0
            unrealized_pnl = 0.0

            # Load all positions from the futures account
            positions = exchange.fetchPositions()

            #positions_data = exchange.fetch_positions()[0]['info']['position']
            #asset = positions_data['coin']
            #position = positions_data['szi']     
            
            # Handle case when no positions exist
            if not positions or len(positions) == 0:
                print(f"No positions found for user {user} in subaccount {subaccount}")
                return 0.0
                
            # Iterate through positions to find matching asset
            for position in positions:
                # Check if position data has the required structure
                if not isinstance(position, dict) or 'symbol' not in position:
                    print(f"Warning: Invalid position data structure: {position}")
                    continue

                # Read unrealized pnl per asset and sum up
                pnl_asset = position['info']['position']['unrealizedPnl']    
                unrealized_pnl += float(pnl_asset)
                        
            return unrealized_pnl
            
        except Exception as e:
            # Handle any unexpected errors
            print(f"Error retrieving position data: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return zero as fallback
            return 0.0
 
  


    def change_leverage(self, user, subaccount=None, asset='btc', leverage=10):
        """
        Retrieve the current position size for a specific asset from a Bitget account using CCXT.
        
        Parameters:
        -----------
        user : str
            User identifier for API credentials
        subaccount : str
            Trading subaccount name, None or 'main' for primary account
        asset : str
            Asset symbol to query (e.g., 'BTC')
        leverage : int
            leverage factor for specific asset (typically 1..40)
            
        Returns:
        --------
        float
            Current position size (positive for long, negative for short, 0 if no position or error)
        """
        # Input validation
        if not user:
            print("Error: User parameter is required")
            return 0.0
        if not asset:
            print("Error: Asset parameter is required")
            return 0.0
            
        # Normalize subaccount parameter
        if subaccount is None or subaccount.lower() == 'main':
            subaccount_name = None
        else:
            subaccount_name = subaccount
            
        # Extract base asset symbol from full name
        try:
            if 'USDT_UMCBL' in asset:
                asset_short = asset.replace('USDT_UMCBL', '')
            elif '/USDC:USDC' in asset:
                asset_short = asset.replace('/USDC:USDC', '')
            else:
                asset_short = asset
                
            # Validate asset symbol (basic check)
            if not asset_short or len(asset_short.strip()) < 1:
                print(f"Error: Invalid asset symbol: {asset}")
                return 0.0
                
        except Exception as e:
            print(f"Error processing asset symbol: {str(e)}")
            return 0.0

        print(f'Changing leverage for user: {user}, subaccount: {subaccount}, asset: {asset_short} to {leverage}')    
        
        """ ### Old Init CCXT client ####
        # Get API credentials based on user and subaccount
        try:
            #if user == 'user1_ms':
            #api_data = self.read_api_key_secret_hl_user1(subaccount_name)
            #else:
            api_data = self.read_api_key_secret_hl(user)
                
            # Validate API data structure
            required_keys = ['api_key', 'api_secret']
            for key in required_keys:
                if key not in api_data or not api_data[key]:
                    raise ValueError(f"Missing or empty {key} in API credentials")
                    
        except Exception as e:
            print(f"Failed to retrieve API credentials: {str(e)}")
            return 0.0
            
        # Initialize CCXT client with error handling
        try:
            
            # Create exchange instance with the API credentials
            exchange = ccxt.hyperliquid({
                "walletAddress": api_data['api_key'],
                "privateKey": api_data['api_secret']
            })
            
            # Set up subaccount if needed
            if subaccount:
                exchange.options['defaultSubaccount'] = subaccount
                
        except Exception as e:
            error_msg = f"Failed to initialize CCXT HyperLiquid client: {str(e)}"
            print(error_msg)
            return error_msg """
        
        try:
            # Retrieve the cached exchange instance (Instant!)
            exchange = self.init_exchange(user, subaccount)
            
        except Exception as e:
            error_msg = f"Failed to get exchange connection: {str(e)}"
            print(error_msg)
            return error_msg           
            
        # Retrieve position data with comprehensive error handling
        try:
            # Format the symbol for CCXT (e.g., 'BTC/USDC:USDC' for futures)
            # This may need adjustment based on the exact format expected by CCXT for Bitget
            ccxt_symbol = f"{asset_short.upper()}/USDC:USDC"
                
            # Load all positions from the futures account
            response = exchange.set_leverage(leverage, ccxt_symbol)
            return response['status']

        except Exception as e:
            error_msg = f"Failed to initialize CCXT HyperLiquid client: {str(e)}"
            print(error_msg)
            return error_msg

