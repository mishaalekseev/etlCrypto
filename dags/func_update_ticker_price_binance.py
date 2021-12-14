def update_ticker_price_binance(symbolTicker = 'BTCUSDT'):
    # Системные библиотеки
    import os
    from utils import logger
    import pandas as pd
    from datetime import datetime
    dirname = os.path.dirname(__file__)

    # Библиотеки подключений
    import psycopg2 # Драйвер PostgreSQL
    from sqlalchemy import create_engine # Библиотека для подключений к БД в pandas
    from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager # API Binance
    import configdags as c # Конфигурации подключений 

    # Строки подключения
    client = Client(c.binance_api_key, c.binance_api_secret)
    engine_psql = create_engine(c.postgresql)
    logger.warning(f"Подключение настроено {datetime.now()}")

    # Получаем данные по последним сделкам по API Binance
    Price = pd.DataFrame([client.get_orderbook_ticker(symbol=symbolTicker)])
    Price = Price[['askPrice','bidPrice']]
    lastPrice = pd.DataFrame(client.get_recent_trades(symbol=symbolTicker, limit = 1))
    Price['lastPrice'] = lastPrice['price']
    Price['datetime'] = pd.to_datetime(lastPrice['time'],unit='ms')
    Price['symbol'] = symbolTicker
    logger.warning(f"Получил данные из API Binance {datetime.now()}")

    # Отправляем сразу в БД
    Price.to_sql('prices_all_binance', engine_psql, if_exists='append',index=False)
    logger.warning(f"Успешно залил данные в таблицу prices_all_binance {datetime.now()}")