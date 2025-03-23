import json
import psycopg2
import os

# --- Database Configuration (PostgreSQL) ---
DB_HOST = "localhost"  # Replace with your DB host
DB_NAME = "stock_analytics"  # Replace with your DB name
DB_USER = "root"  # Replace with your DB user
DB_PASSWORD = "arka1256"  # Replace with your DB password

conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()

# Define the correct folder path containing JSON files
folder_path = os.path.join(os.getcwd(), 'selected_stocks')

# Check if folder exists
if not os.path.exists(folder_path):
    raise FileNotFoundError(f"Folder '{folder_path}' does not exist.")

# Define SQL table creation statements
create_table_statements = [
    """
    CREATE TABLE IF NOT EXISTS equity_info (
        symbol VARCHAR(10) PRIMARY KEY,
        company_name VARCHAR(255),
        industry VARCHAR(255),
        isin VARCHAR(255),
        slb_isin VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS equity_metadata (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        series VARCHAR(255),
        isin VARCHAR(255),
        status VARCHAR(255),
        listing_date DATE,
        industry VARCHAR(255),
        last_update_time TIMESTAMP,
        pd_sector_pe NUMERIC,
        pd_symbol_pe NUMERIC,
        pd_sector_ind VARCHAR(255),
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS equity_price_info (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        last_price NUMERIC,
        change NUMERIC,
        p_change NUMERIC,
        previous_close NUMERIC,
        open NUMERIC,
        close NUMERIC,
        vwap NUMERIC,
        stock_ind_close_price NUMERIC,
        lower_cp VARCHAR(255),
        upper_cp VARCHAR(255),
        p_price_band VARCHAR(255),
        base_price NUMERIC,
        min NUMERIC,
        max NUMERIC,
        intraday_high_low_value NUMERIC,
        week_high_low_min NUMERIC,
        week_high_low_min_date DATE,
        week_high_low_max NUMERIC,
        week_high_low_max_date DATE,
        week_high_low_value NUMERIC,
        i_nav_value NUMERIC,
        check_i_nav BOOLEAN,
        tick_size NUMERIC,
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS equity_industry_info (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        macro VARCHAR(255),
        sector VARCHAR(255),
        industry VARCHAR(255),
        basic_industry VARCHAR(255),
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS trade_info (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        no_block_deals BOOLEAN,
        bulk_block_deals_name VARCHAR(255) ARRAY,
        total_buy_quantity BIGINT,
        total_sell_quantity BIGINT,
        trade_info_total_traded_volume NUMERIC,
        trade_info_total_traded_value NUMERIC,
        trade_info_total_market_cap NUMERIC,
        ffmc NUMERIC,
        impact_cost NUMERIC,
        cm_daily_volatility VARCHAR(255),
        cm_annual_volatility VARCHAR(255),
        market_lot VARCHAR(255),
        active_series VARCHAR(255),
        security_var NUMERIC,
        index_var NUMERIC,
        var_margin NUMERIC,
        extreme_loss_margin NUMERIC,
        adhoc_margin NUMERIC,
        applicable_margin NUMERIC,
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS security_wise_dp (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        quantity_traded BIGINT,
        delivery_quantity BIGINT,
        delivery_to_traded_quantity NUMERIC,
        series_remarks VARCHAR(255),
        sec_wise_del_pos_date DATE,
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS corporate_actions (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        exdate DATE,
        purpose VARCHAR(255),
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS shareholdings_patterns (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        date DATE,
        promoter_and_promoter_group NUMERIC,
        public NUMERIC,
        shares_held_by_employee_trusts NUMERIC,
        total NUMERIC,
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_results (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        from_date DATE,
        to_date DATE,
        expenditure NUMERIC,
        income NUMERIC,
        audited VARCHAR(255),
        cumulative VARCHAR(255),
        consolidated VARCHAR(255),
        re_dil_eps NUMERIC,
        re_pro_loss_bef_tax NUMERIC,
        pro_loss_aft_tax NUMERIC,
        re_broadcast_timestamp DATE,
        xbrl_attachment VARCHAR(255),
        na_attachment VARCHAR(255),
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS board_meeting (
        symbol VARCHAR(10) REFERENCES equity_info(symbol),
        purpose TEXT,
        meeting_date DATE,
        UNIQUE(symbol)  -- Adding UNIQUE constraint here
    );
    """
]

# Create tables if they don't exist
for statement in create_table_statements:
    cur.execute(statement)
    conn.commit()

# Iterate through all JSON files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.json'):
        file_path = os.path.join(folder_path, filename)
        
        try:
            # Load JSON data
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Extract data from the JSON
            equity_details = data['equityDetails']
            equity_info = equity_details['info']
            equity_metadata = equity_details['metadata']
            equity_price_info = equity_details['priceInfo']
            equity_industry_info = equity_details['industryInfo']

            trade_info = data['tradeInfo']
            trade_info_market_dept_order_book = trade_info['marketDeptOrderBook']
            trade_info_security_wise_dp = trade_info['securityWiseDP']

            corporate_info = data['corporateInfo']
            corporate_actions = corporate_info['corporate_actions']['data']
            shareholdings_patterns = corporate_info['shareholdings_patterns']['data']
            financial_results = corporate_info['financial_results']['data']
            board_meeting = corporate_info['borad_meeting']['data']

            symbol = equity_info['symbol']

            # Insert data into tables using parameterized queries
            # Example: Inserting into equity_details
            cur.execute(
                """
                INSERT INTO equity_info (
                    symbol, company_name, industry, isin, slb_isin
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    industry = EXCLUDED.industry,
                    isin = EXCLUDED.isin,
                    slb_isin = EXCLUDED.isin
                """,
                (
                    equity_info['symbol'], 
                    equity_info['companyName'], 
                    equity_info['industry'], 
                    equity_info['isin'], 
                    equity_info['isin']
                )
            )
            conn.commit()

            # Insert into equity_metadata
            cur.execute(
                """
                INSERT INTO equity_metadata (
                    series, symbol, isin, status, listing_date, industry, last_update_time, pd_sector_pe, pd_symbol_pe,
                    pd_sector_ind
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
                """,
                (
                    equity_metadata['series'], equity_metadata['symbol'], equity_metadata['isin'], equity_metadata['status'],
                    equity_metadata['listingDate'] if equity_metadata['listingDate'] != 'NA' else None,
                    equity_metadata['industry'],
                    equity_metadata['lastUpdateTime'] if equity_metadata['lastUpdateTime'] != 'NA' else None,
                    float(equity_metadata['pdSectorPe']) if equity_metadata['pdSectorPe'] != 'NA' else None,
                    float(equity_metadata['pdSymbolPe']) if equity_metadata['pdSymbolPe'] != 'NA' else None,
                    equity_metadata['pdSectorInd'] if equity_metadata['pdSectorInd'] != 'NA' else None
                )
            )
            conn.commit()

            # Insert into equity_price_info
            cur.execute(
                """
                INSERT INTO equity_price_info (
                    symbol, last_price, change, p_change, previous_close, open, close, vwap, stock_ind_close_price, lower_cp, upper_cp,
                    p_price_band, base_price, min, max, intraday_high_low_value, week_high_low_min, week_high_low_min_date,
                    week_high_low_max, week_high_low_max_date, week_high_low_value, i_nav_value, check_i_nav, tick_size
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    last_price = EXCLUDED.last_price,
                    change = EXCLUDED.change,
                    p_change = EXCLUDED.p_change,
                    previous_close = EXCLUDED.previous_close,
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    vwap = EXCLUDED.vwap,
                    stock_ind_close_price = EXCLUDED.stock_ind_close_price,
                    lower_cp = EXCLUDED.lower_cp,
                    upper_cp = EXCLUDED.upper_cp,
                    p_price_band = EXCLUDED.p_price_band,
                    base_price = EXCLUDED.base_price,
                    min = EXCLUDED.min,
                    max = EXCLUDED.max,
                    intraday_high_low_value = EXCLUDED.intraday_high_low_value,
                    week_high_low_min = EXCLUDED.week_high_low_min,
                    week_high_low_min_date = EXCLUDED.week_high_low_min_date,
                    week_high_low_max = EXCLUDED.week_high_low_max,
                    week_high_low_max_date = EXCLUDED.week_high_low_max_date,
                    week_high_low_value = EXCLUDED.week_high_low_value,
                    i_nav_value = EXCLUDED.i_nav_value,
                    check_i_nav = EXCLUDED.check_i_nav,
                    tick_size = EXCLUDED.tick_size
                """,
                (
                    equity_info['symbol'], equity_price_info['lastPrice'], equity_price_info['change'],
                    equity_price_info['pChange'], equity_price_info['previousClose'], equity_price_info['open'],
                    equity_price_info['close'], equity_price_info['vwap'], equity_price_info['stockIndClosePrice'],
                    equity_price_info['lowerCP'], equity_price_info['upperCP'], equity_price_info['pPriceBand'],
                    equity_price_info['basePrice'], equity_price_info['intraDayHighLow']['min'],
                    equity_price_info['intraDayHighLow']['max'], equity_price_info['intraDayHighLow']['value'],
                    equity_price_info['weekHighLow']['min'], equity_price_info['weekHighLow']['minDate'],
                    equity_price_info['weekHighLow']['max'], equity_price_info['weekHighLow']['maxDate'],
                    equity_price_info['weekHighLow']['value'], equity_price_info['iNavValue'], equity_price_info['checkINAV'],
                    equity_price_info['tickSize']
                )
            )
            conn.commit()

            # Insert into equity_industry_info
            cur.execute(
                """
                INSERT INTO equity_industry_info (
                    symbol, macro, sector, industry, basic_industry
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    macro = EXCLUDED.macro,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    basic_industry = EXCLUDED.basic_industry
                """,
                (
                    equity_info['symbol'], equity_industry_info['macro'], equity_industry_info['sector'],
                    equity_industry_info['industry'], equity_industry_info['basicIndustry']
                )
            )
            conn.commit()

            # Insert into trade_info
            cur.execute(
                """
                INSERT INTO trade_info (
                    symbol, no_block_deals, bulk_block_deals_name, total_buy_quantity, total_sell_quantity,
                    trade_info_total_traded_volume, trade_info_total_traded_value, trade_info_total_market_cap, ffmc,
                    impact_cost, cm_daily_volatility, cm_annual_volatility, market_lot, active_series, security_var, index_var,
                    var_margin, extreme_loss_margin, adhoc_margin, applicable_margin
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    no_block_deals = EXCLUDED.no_block_deals,
                    bulk_block_deals_name = EXCLUDED.bulk_block_deals_name,
                    total_buy_quantity = EXCLUDED.total_buy_quantity,
                    total_sell_quantity = EXCLUDED.total_sell_quantity,
                    trade_info_total_traded_volume = EXCLUDED.trade_info_total_traded_volume,
                    trade_info_total_traded_value = EXCLUDED.trade_info_total_traded_value,
                    trade_info_total_market_cap = EXCLUDED.trade_info_total_market_cap,
                    ffmc = EXCLUDED.ffmc,
                    impact_cost = EXCLUDED.impact_cost,
                    cm_daily_volatility = EXCLUDED.cm_daily_volatility,
                    cm_annual_volatility = EXCLUDED.cm_annual_volatility,
                    market_lot = EXCLUDED.market_lot,
                    active_series = EXCLUDED.active_series,
                    security_var = EXCLUDED.security_var,
                    index_var = EXCLUDED.index_var,
                    var_margin = EXCLUDED.var_margin,
                    extreme_loss_margin = EXCLUDED.extreme_loss_margin,
                    adhoc_margin = EXCLUDED.adhoc_margin,
                    applicable_margin = EXCLUDED.applicable_margin
                """,
                (
                    equity_info['symbol'], trade_info['noBlockDeals'],
                    [block_deal['name'] for block_deal in trade_info['bulkBlockDeals']],
                    trade_info_market_dept_order_book['totalBuyQuantity'],
                    trade_info_market_dept_order_book['totalSellQuantity'],
                    trade_info_market_dept_order_book['tradeInfo']['totalTradedVolume'],
                    trade_info_market_dept_order_book['tradeInfo']['totalTradedValue'],
                    trade_info_market_dept_order_book['tradeInfo']['totalMarketCap'],
                    trade_info_market_dept_order_book['tradeInfo']['ffmc'],
                    trade_info_market_dept_order_book['tradeInfo']['impactCost'],
                    trade_info_market_dept_order_book['tradeInfo']['cmDailyVolatility'],
                    trade_info_market_dept_order_book['tradeInfo']['cmAnnualVolatility'],
                    trade_info_market_dept_order_book['tradeInfo']['marketLot'],
                    trade_info_market_dept_order_book['tradeInfo']['activeSeries'],
                    trade_info_market_dept_order_book['valueAtRisk']['securityVar'],
                    trade_info_market_dept_order_book['valueAtRisk']['indexVar'],
                    trade_info_market_dept_order_book['valueAtRisk']['varMargin'],
                    trade_info_market_dept_order_book['valueAtRisk']['extremeLossMargin'],
                    trade_info_market_dept_order_book['valueAtRisk']['adhocMargin'],
                    trade_info_market_dept_order_book['valueAtRisk']['applicableMargin']
                )
            )
            conn.commit()

            # Insert into security_wise_dp
            def safe_date(value):
                """Return the date if valid, else return None."""
                return value.replace(' EOD', '') if value not in ['-', 'NA', None] else None

            def safe_string(value):
                """Return the string if valid, else return None."""
                return value if value not in ['-', 'NA', None] else None

            def safe_numeric(value):
                """Return the numeric value if valid, else return None."""
                return float(value) if value not in ['-', 'NA', None] else None

            cur.execute(
                """
                INSERT INTO security_wise_dp (
                    symbol, quantity_traded, delivery_quantity, delivery_to_traded_quantity, series_remarks, sec_wise_del_pos_date
                )
                VALUES (%s, %s, %s, %s, %s, TO_DATE(%s, 'DD-MON-YYYY'))
                ON CONFLICT (symbol) DO NOTHING
                """,
                (
                    equity_info['symbol'],
                    safe_numeric(trade_info_security_wise_dp['quantityTraded']),
                    safe_numeric(trade_info_security_wise_dp['deliveryQuantity']),
                    safe_numeric(trade_info_security_wise_dp['deliveryToTradedQuantity']),
                    safe_string(trade_info_security_wise_dp['seriesRemarks']),
                    safe_date(trade_info_security_wise_dp['secWiseDelPosDate'])
                )
            )
            conn.commit()

            # Insert into corporate_actions
            for action in corporate_actions:
                cur.execute(
                    """
                    INSERT INTO corporate_actions (
                        symbol, exdate, purpose
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol) DO NOTHING
                    """,
                    (action['symbol'], action['exdate'], action['purpose'])
                )
            conn.commit()

            # Insert into shareholdings_patterns
            for date, patterns in shareholdings_patterns.items():
                promoter_and_promoter_group = None
                public = None
                shares_held_by_employee_trusts = None
                total = None
                
                for pattern in patterns:
                    for holder_type, percentage in pattern.items():
                        holder_type = holder_type.strip()
                        percentage = percentage.strip().replace('%', '')  # Remove '%' symbol
                        
                        if holder_type == "Promoter & Promoter Group":
                            promoter_and_promoter_group = percentage
                        elif holder_type == "Public":
                            public = percentage
                        elif holder_type == "Shares held by Employee Trusts":
                            shares_held_by_employee_trusts = percentage
                        elif holder_type == "Total":
                            total = percentage
                
                cur.execute(
                    """
                    INSERT INTO shareholdings_patterns (
                        symbol, date, promoter_and_promoter_group, public, shares_held_by_employee_trusts, total
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO NOTHING
                    """,
                    (symbol, date, promoter_and_promoter_group, public, shares_held_by_employee_trusts, total)
                )
            conn.commit()

            # Insert into financial_results
            for result in financial_results:
                cur.execute(
                    """
                    INSERT INTO financial_results (
                        symbol, from_date, to_date, expenditure, income, audited, cumulative, consolidated, re_dil_eps,
                        re_pro_loss_bef_tax, pro_loss_aft_tax, re_broadcast_timestamp, xbrl_attachment, na_attachment
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO NOTHING
                    """,
                    (
                        symbol, result['from_date'], result['to_date'], result['expenditure'], result['income'], result['audited'],
                        result['cumulative'], result['consolidated'], result['reDilEPS'], result['reProLossBefTax'],
                        result['proLossAftTax'], result['re_broadcast_timestamp'], result['xbrl_attachment'],
                        result['na_attachment']
                    )
                )
            conn.commit()

            # Insert into board_meeting
            for meeting in board_meeting:
                cur.execute(
                    """
                    INSERT INTO board_meeting (
                        symbol, purpose, meeting_date
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol) DO NOTHING
                    """,
                    (meeting['symbol'], meeting['purpose'], meeting['meetingdate'])
                )
            conn.commit()

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            conn.rollback()  # Rollback the transaction in case of error
            continue  # Move to the next file

# Close database connection
cur.close()
conn.close()