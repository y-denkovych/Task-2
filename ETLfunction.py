#Importing libraries
import pandas as pd
import json
from password import password
from sqlalchemy import create_engine, MetaData ,text,inspect,table
from sqlalchemy_utils import create_database, database_exists

def ETL(file_name, database_name):

    #opening the file and loading it into a variable
    with open(file_name,"r") as file:
        data = json.load(file)

    Stats= data["Statistics"]
    Statsdf = pd.DataFrame.from_dict(Stats,orient="index").transpose()

    RunTimeStats = data["RuntimeStatistics"]
    RunTimeStatsdf = pd.DataFrame.from_dict(RunTimeStats,orient="index").transpose()

    #aquiring sub-dictionaries and loading them into a database
    Rolw = data['RollingWindow'].items()
    df = []
    for key1 , value1 in Rolw:
        for key2, value2 in value1.items(): 
            sub_df = pd.DataFrame.from_dict(value2,orient="index").transpose()
            sub_df["date"] = key1
            df.append(sub_df.set_index(["date"]))
    finaldf = pd.concat(df,axis = 0)

    # Copying every other row into a new df, and deleting the row for a second df.
    portfoliodf = finaldf.iloc[1::2]
    tradingdf = finaldf.iloc[::2]

    #removing unused columns
    portfoliodf = portfoliodf.iloc[:, 41:]

    #removing unused columns
    tradingdf = tradingdf.iloc[:,:41]

    #separating Portfolio df based on months
    m1portfoliodfmask = portfoliodf.index.get_level_values('date').str.contains('M1_')
    m3portfoliodfmask = portfoliodf.index.get_level_values('date').str.contains('M3_')
    m12portfoliodfmask = portfoliodf.index.get_level_values('date').str.contains('M12_')

    m1portfoliodf = portfoliodf.loc[m1portfoliodfmask]
    m3portfoliodf = portfoliodf.loc[m3portfoliodfmask]
    m12portfoliodf = portfoliodf.loc[m12portfoliodfmask]

    m1portfoliodf = m1portfoliodf.reset_index(drop=False)
    m3portfoliodf = m3portfoliodf.reset_index(drop=False)
    m12portfoliodf = m12portfoliodf.reset_index(drop=False)

    #separating Trading df based on months
    m1tradingdfmask = tradingdf.index.get_level_values('date').str.contains('M1_')
    m3tradingdfmask = tradingdf.index.get_level_values('date').str.contains('M3_')
    m12tradingdfmask = tradingdf.index.get_level_values('date').str.contains('M12_')

    m1tradingdf = tradingdf.loc[m1tradingdfmask]
    m3tradingdf = tradingdf.loc[m3tradingdfmask]
    m12tradingdf = tradingdf.loc[m12tradingdfmask]

    m1tradingdf = m1tradingdf.reset_index(drop=False)
    m3tradingdf = m3tradingdf.reset_index(drop=False)
    m12tradingdf = m12tradingdf.reset_index(drop=False)

    #Initialise connection
    protocol = 'postgresql'
    username = 'postgres'
    db_password = password
    host = 'localhost'
    port = 5432
    database_name = database_name
    rds_connection_string = f'{protocol}+psycopg2://{username}:{db_password}@{host}:{port}/{database_name}'
    engine = create_engine(rds_connection_string)

    # Check if the database exists and then create it if there are none
    if not database_exists(engine.url):
        create_database(engine.url)

    #Checking connection
    with engine.connect() as conn:
        sql = text('SELECT current_database()')
        result = conn.execute(sql)
        print(result.fetchone()[0]) 

    #Loading dfs to SQL
    m1portfoliodf.to_sql(name='PortfolioStatsM1', con=engine, if_exists='replace', index=False)
    m3portfoliodf.to_sql(name='PortfolioStatsM3', con=engine, if_exists='replace', index=False)
    m12portfoliodf.to_sql(name='PortfolioStatsM12', con=engine, if_exists='replace', index=False)
    m1tradingdf.to_sql(name='TradingStatsM1', con=engine, if_exists='replace', index=False)
    m3tradingdf.to_sql(name='TradingStatsM3', con=engine, if_exists='replace', index=False)
    m12tradingdf.to_sql(name='TradingStatsM12', con=engine, if_exists='replace', index=False)

    Statsdf.to_sql(name='Stats', con=engine, if_exists='replace', index=False)
    RunTimeStatsdf.to_sql(name='RunTimeStats', con=engine, if_exists='replace', index=False)

    #Checking tables names
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    print(table_names)