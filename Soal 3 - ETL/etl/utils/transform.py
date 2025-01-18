import pandas as pd

def general_clean(df):
    df = df.str.strip()
    df = df.str.replace(r'_NOISE.*', '', regex=True)
    return df

def clean_products(products_df: pd.DataFrame) -> pd.DataFrame:
    #clean products data
    products_df = products_df.apply(general_clean) #cleaning empty space and noise string

    #removing data with invalid product_id because it is unusable
    products_df = products_df[products_df['product_id'].str.match(r'^PROD-[a-zA-Z0-9]+$', na=False)]
    products_df = products_df[~products_df['product_id'].str.match(r'^PROD-.$')]


    category_list = ['Books','Sports & Outdoors','Home & Kitchen','Beauty & Personal Care','Electronics','Clothing'] #list of category, needs to be updated for new category or taken from another table that contains full list of category
    category_fix_map = {
        'Bo': 'Books',
        'Home &': 'Home & Kitchen',
        'Clot': 'Clothing',
        'Beauty & Pe': 'Beauty & Personal Care',
        'Sports &': 'Sports & Outdoors',
        'Elect': 'Electronics'
    } #fix map for unfinished category value
    products_df['category'] = products_df['category'].replace(category_fix_map) #fixing unfinished value
    products_df['category'] = products_df['category'].where(products_df['category'].isin(category_list)) #removing value that is not in the category list


    products_df['product_name'] = products_df['product_name'].str.replace('-','') #removing - from product_name for easier processing
    products_df['product_name'] = products_df['product_name'].fillna('MISSING') #replacing null value with 'MISSING for easier processing
    products_df['product_name'] = products_df['product_name'].where(~products_df['product_name'].str[0].isin(list("-0123456789")), other=products_df['category'].astype('str') + ' Item ' + products_df['product_name'].astype('str')) #replacing missing product_name value using category value + Itme + number from original product_name value
    products_df['product_name'] = products_df['product_name'].where(~products_df['product_name'].str.startswith('MISSING')) #removing value that starts with missing


    manufacturer_list = ['EcoTech','GlobalBrands','FashionHouse','TechGiant','SportsPro','LuxuryGoods','BookWorlds'] #list of manufacturer, needs to be updated for new manufacturer or taken from another table that contains full list of manufacturer
    manufacturer_fix_map = {
        'Eco': 'EcoTech',
        'Global': 'GlobalBrands',
        'Fashio': 'FashionHouse',
        'Tech': 'TechGiant',
        'Luxur': 'LuxuryGoods',
        'Book': 'BookWorlds'
    } #fix map for unfinished manufacturer value
    products_df['manufacturer'] = products_df['manufacturer'].replace(manufacturer_fix_map) #fixing unfinished value
    products_df['manufacturer'] = products_df['manufacturer'].where(products_df['manufacturer'].isin(manufacturer_list)) #removing value that is not in the manufacturer list


    #normalize products data
    products_df['base_price'] = pd.to_numeric(products_df['base_price'], errors='coerce') #changing price data type to numeric, if not a number change to null
    return products_df

def clean_transactions(transactions_df: pd.DataFrame) -> pd.DataFrame:
    #clean transactions data
    transactions_df = transactions_df.apply(general_clean)  #cleaning empty space and noise string

    #removing data with invalid transaction_id because it is unusable
    transactions_df = transactions_df[transactions_df['transaction_id'].str.match(r'^TRX-[a-zA-Z0-9]+$', na=False)]
    transactions_df = transactions_df[~transactions_df['transaction_id'].str.match(r'^TRX-..$', na=False)]

    transactions_df['customer_id'] = transactions_df['customer_id'].where(transactions_df['customer_id'].str.match(r'^TRX-[a-zA-Z0-9]+$', na=False)) #removing invalid customer_id

    transactions_df['product_id'] = transactions_df['product_id'].where(transactions_df['product_id'].str.match(r'^PROD-[a-zA-z0-9]+$', na=False)) #removing invalid product_id

    sales_channel_list = ['Mobile App','In-Store','Online'] #list of sales channel, needs to be updated for new sales channel or taken from another table that contains full list of sales channel
    sales_channel_fix_map = {
        'Mobil': 'Mobile App',
        'In-S': 'In-Store',
        'Onl': 'Online'
    } #fix map for unfinished sales channel value
    transactions_df['sales_channel'] = transactions_df['sales_channel'].replace(sales_channel_fix_map) #fixing unfinished value
    transactions_df['sales_channel'] = transactions_df['sales_channel'].where(transactions_df['sales_channel'].isin(sales_channel_list)) #removing value that is not in the sales channel list


    #normalize transactions data
    transactions_df['sale_date'] = pd.to_datetime(transactions_df['sale_date'], errors='coerce') #changing sale_date to date format, if not a date then change to null
    transactions_df['quantity'] = pd.to_numeric(transactions_df['quantity'], errors='coerce') #changing quantity to numeric format, if not a number then change to null
    transactions_df['total_price'] = pd.to_numeric(transactions_df['total_price'], errors='coerce') #changing total_price to numeric format, if not a number then change to null
    return transactions_df

#calculate new metrics
def products_revenue(transactions_df: pd.DataFrame) -> pd.DataFrame:
    temp_df = transactions_df.groupby('product_id', as_index=False)['total_price'].sum()
    return temp_df

def sales_channel_performance(transactions_df: pd.DataFrame) -> pd.DataFrame:
    temp_df = transactions_df.groupby('sales_channel', as_index=False)[['quantity','total_price']].sum()
    return temp_df

def customer_frequency(transactions_df: pd.DataFrame) -> pd.DataFrame:
    temp_df = transactions_df.groupby('customer_id', as_index=False)['transaction_id'].count()
    return temp_df