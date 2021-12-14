def update_hourly_data():

    # Системные библиотеки
    import os
    from utils import logger
    import pandas as pd
    from datetime import datetime
    dirname = os.path.dirname(__file__)

    # Библиотеки подключений
    import psycopg2 # Драйвер PostgreSQL
    import sqlalchemy as sa
    from sqlalchemy import create_engine # Библиотека для подключений к БД в pandas
    import configdags as c # Конфигурации подключений 

    # Строки подключения
    engine_psql = create_engine(c.postgresql)
    logger.warning(f"Подключение настроено {datetime.now()}")

    ##########################################
    ## Функция для обработки больших таблиц ##
    def load_in_bunches(sql, engine, items_list, step=2000, obj_name='', string_values=False, execute_sql = False):
            df = pd.DataFrame()
            if string_values:
                joint = "','"
            else:
                joint = ','
            for bunch in range(0, len(items_list), step):
                if execute_sql:
                    ob.execute_sql_safe(
                        sql.format(joint.join(items_list[bunch:bunch + step])),
                        engine,
                        f'{obj_name} с {bunch} по {bunch + step}'
                    )
                    return
                else:
                    df = df.append(
                        ob.read_sql_safe(
                            sql.format(joint.join(items_list[bunch:bunch + step])),
                            engine,
                            f'{obj_name} с {bunch} по {bunch + step}'
                        )
                    )
            return df

    def execute_in_bunches(sql, engine, items_list, step=2000, obj_name='', string_values=False):
            if string_values:
                joint = "','"
            else:
                joint = ','
            for bunch in range(0, len(items_list), step):
                f_string = joint.join(items_list[bunch:bunch + step])
                ob.execute_sql_safe(
                    sql.format(f_string, f_string),
                    engine,
                    f'{obj_name} с {bunch} по {bunch + step}'
                )
            return 0

    def read_sql_safe(query, engine, query_name, tries=1, sleep_seconds=10, enable_logs=True):
        """
        query (str) - sql текст запроса
        engine - объект sa.create_engine()
        query_name (str) - название запроса (для азписи в логи)
        tries (int) - кол-во повторных попыток выполнения запроса
        sleep_seconds (int) - сколько времени ждать между попытками выполнить запрос

        Функция dispose'ит коннекшн после выполнения запроса или после того, как запрос падает
        Кроме того, может повторять попытку выполнить запрос в случае ошибки
        """
        sql_flag = False
        q = 0
        df = None
        while not sql_flag:
            try:
                logger.warning(f'Начинаю запрос {query_name}')
                df = pd.read_sql(sql=query, con=engine)
                logger.warning(f'Закончил запрос {query_name}')
                sql_flag = True
                engine.dispose()
            except sa.exc.SQLAlchemyError:
                engine.dispose()
                sql_flag = False
                q += 1
                if q >= tries:
                    if enable_logs:
                        logger.warning(
                            f"Выполнение sql запроса выполнено с ошибкой. Попытки закончились, rais'им ошибку")
                    raise
                else:
                    if enable_logs:
                        logger.warning(
                            f'Выполнение sql запроса выполнено с ошибкой. жду {sleep_seconds} сек. и пробую снова')
                    sleep(sleep_seconds)
            # если ошибка не sql, то диспоузим коннекшн и рейзим ошибку
            except Exception as e:
                logger.error(e)
                engine.dispose()
                raise
        return df

    def execute_sql_safe(query, engine, query_name, tries=1, sleep_seconds=10, enable_logs=True):
        """
        Функция для безопасного sql execute. Аргументы аналогичны функции read_sql_safe
        """
        sql_flag = False
        q = 0
        # df = None
        while not sql_flag:
            try:
                logger.warning(f'Начинаю execute {query_name}')
                engine.execution_options(autocommit=True).execute(query)
                logger.warning(f'Закончил execute {query_name}')
                sql_flag = True
                engine.dispose()
            except sa.exc.SQLAlchemyError:
                engine.dispose()
                sql_flag = False
                q += 1
                if q >= tries:
                    if enable_logs:
                        logger.warning(
                            f"Выполнение sql запроса выполнено с ошибкой. Попытки закончились, rais'им ошибку")
                    raise
                else:
                    if enable_logs:
                        logger.warning(
                            f'Выполнение sql запроса выполнено с ошибкой. жду {sleep_seconds} сек. и пробую снова')
                    sleep(sleep_seconds)
            # если ошибка не sql, то диспоузим коннекшн и рейзим ошибку
            except Exception as e:
                logger.error(e)
                engine.dispose()
                raise
    # Функция интерполяции
    def interpolate_columns(df, columns):
        for col in columns:
            df[col] = df[col].interpolate()
        return df
    ################################
    ## Обработка таблицы с Bitmex ##
    query_distinct_symbols_bitmex = '''
    select distinct p.symbol as SymbolTicker
    from db1.public.prices_all_bitmex p;
    '''
    symbols_bitmex = list(read_sql_safe(query = query_distinct_symbols_bitmex, engine = engine_psql, query_name = '- забираем тикеры')['symbolticker'])

    for symbol in symbols_bitmex:
        start = 1
        end = 1000
        step = 1000
        while 1>0:
            query_hourly = f'''
            drop table if exists generatedHours;
            SELECT date_trunc('hour',generate_series(min(p.datetime), max(p.datetime), '1h')) AS order_date
            into TEMPORARY TABLE generatedHours
            from db1.public.prices_all_bitmex p
            where p.symbol = '{symbol}';

            select order_date, row_number() over (order by order_date) as rkn
            from generatedHours;

            select base.order_date, COALESCE(avg(data."askPrice"),0) as askPrice, COALESCE(avg(data."bidPrice"),0) as bidPrice, COALESCE(avg(data."lastPrice"),0) as lastPrice, base.rkn
            from(
                select order_date, row_number() over (order by order_date) as rkn
                from generatedHours p
                ) base
                left join (
                select date_trunc('hour',p.datetime) order_date, p."askPrice", p."bidPrice", p."lastPrice"
                from db1.public.prices_all_bitmex p
                where p.symbol = '{symbol}'
                ) data on data.order_date = base.order_date
            group by base.order_date,base.rkn
            having base.rkn between {start} and {end}
            order by base.order_date
            '''
            readed_df = read_sql_safe(query = query_hourly, engine = engine_psql, query_name = f'- забираем часовые данные {symbol}')
            if len(readed_df) == 0:
                break
            
            readed_df = interpolate_columns(
                readed_df,
                ['askprice', 'bidprice', 'lastprice']
            )
            readed_df.to_sql(f'hourly_prices_bitmex_{symbol}', engine_psql, if_exists='append',index=False)
            start = start+step
            end = end+step
        query_delete_duplicate_rows = f'''
            delete from db1.public."hourly_prices_bitmex_{symbol}"
            where exists (select 1
                        from db1.public."hourly_prices_bitmex_{symbol}" t2
                        where t2.order_date = db1.public."hourly_prices_bitmex_{symbol}".order_date and
                                t2.ctid > db1.public."hourly_prices_bitmex_{symbol}".ctid
                        );
        '''
        execute_sql_safe(query_delete_duplicate_rows, engine_psql, 'удаляем дупликаты')
    logger.warning(f"Загрузка завершена bitmex {datetime.now()}")

    ################################
    ## Обработка таблицы с Binance ##
    query_distinct_symbols_binance = '''
    select distinct p.symbol as SymbolTicker
    from db1.public.prices_all_binance p;
    '''
    symbols_binance = list(read_sql_safe(query = query_distinct_symbols_binance, engine = engine_psql, query_name = '- забираем тикеры')['symbolticker'])

    for symbol in symbols_binance:
        start = 1
        end = 1000
        step = 1000

        while 1>0:
            query_hourly = f'''
            drop table if exists generatedHours;
            SELECT date_trunc('hour',generate_series(min(p.datetime), max(p.datetime), '1h')) AS order_date
            into TEMPORARY TABLE generatedHours
            from db1.public.prices_all_binance p
            where p.symbol = '{symbol}';

            select order_date, row_number() over (order by order_date) as rkn
            from generatedHours;

            select base.order_date, avg(data."askPrice") as askPrice, avg(data."bidPrice") as bidPrice, avg(data."lastPrice") as lastPrice, base.rkn
            from(
                select order_date, row_number() over (order by order_date) as rkn
                from generatedHours p
                ) base
                left join (
                select date_trunc('hour',p.datetime) order_date, p."askPrice", p."bidPrice", p."lastPrice"
                from db1.public.prices_all_binance p
                where p.symbol = '{symbol}'
                ) data on data.order_date = base.order_date
            group by base.order_date,base.rkn
            having (base.rkn between {start} and {end})
            order by base.order_date
            '''
            readed_df = read_sql_safe(query = query_hourly, engine = engine_psql, query_name = f'- забираем часовые данные {symbol}')
            if len(readed_df) == 0:
                break
            
            readed_df = interpolate_columns(
                readed_df,
                ['askprice', 'bidprice', 'lastprice']
            )

            readed_df.to_sql(f'hourly_prices_binance_{symbol}', engine_psql, if_exists='append',index=False)
            start = start+step
            end = end+step
        
        query_delete_duplicate_rows = f'''
            delete from db1.public."hourly_prices_binance_{symbol}"
            where exists (select 1
                        from db1.public."hourly_prices_binance_{symbol}" t2
                        where t2.order_date = db1.public."hourly_prices_binance_{symbol}".order_date and
                                t2.ctid > db1.public."hourly_prices_binance_{symbol}".ctid
                        );
        '''
        execute_sql_safe(query_delete_duplicate_rows, engine_psql, 'удаляем дупликаты')

    logger.warning(f"Загрузка завершена binance {datetime.now()}")


update_hourly_data()