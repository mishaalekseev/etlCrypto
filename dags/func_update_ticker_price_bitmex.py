def update_ticker_price_bitmex(symbolTicker = 'XBTUSD'):
    # Системные библиотеки
    import os
    from utils import logger
    import pandas as pd
    from datetime import datetime
    dirname = os.path.dirname(__file__)

    # Библиотеки подключений
    import psycopg2 # Драйвер PostgreSQL
    from sqlalchemy import create_engine # Библиотека для подключений к БД в pandas
    import bitmex # API Bitmex
    import configdags as c # Конфигурации подключений 

    # Строки подключения
    client = bitmex.bitmex(test=True, api_key=c.bitmex_api_key, api_secret=c.bitmex_api_secret)
    engine_psql = create_engine(c.postgresql)
    logger.warning(f"Подключение настроено {datetime.now()}")

    # Получаем данные по последним сделкам по API Bitmex
    BitMex_df = pd.DataFrame(client.Instrument.Instrument_get(symbol = 'XBT').result()[0])
    logger.warning(f"Получил данные из API Bitmex {datetime.now()}")
    print(BitMex_df.columns)
    BitMex_df = BitMex_df[['askPrice','bidPrice','lastPrice','timestamp']]
    BitMex_df = BitMex_df.rename(columns = {
        'timestamp' : 'datetime',
    })
    BitMex_df['symbol'] = symbolTicker
    # # Отправляем сразу в БД
    BitMex_df.to_sql('prices_all_bitmex', engine_psql, if_exists='append',index=False)
    logger.warning(f"Успешно залил данные в таблицу prices_all_bitmex {datetime.now()}")