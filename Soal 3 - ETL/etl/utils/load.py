from sqlalchemy import create_engine
import pandas as pd

def load_to_postgres(df: pd.DataFrame, table_name: str, connection_string: str):
    #load into postgresql
    engine = create_engine(connection_string)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Data loaded into {table_name}.")