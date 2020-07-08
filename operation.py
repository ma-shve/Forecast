import pymssql
import numpy as np
import pandas as pd
import datetime
import service


# =============================================================================
# Смещение столбцов в целевом фрейме данных
# df - Целевой фрейм, в котором производим перестановку
# col1 - Первый столбец
# col2 - Второй столбец
# step - Шаг смещения
# cycle - Циклическое перемещение всех столбцов между целевыми или еденичная смена
# =============================================================================
def column_shift(df, col1, col2, step, cycle):
    # Перестановка столбцов
    cols = df.columns.tolist()
    if cycle:
        for i in range (col1,col2+1):
            col = cols[i]
            cols[i] = cols[i+step]
            cols[i+step] = col          
    else:
        col = cols[col1]
        cols[col1] = cols[col2]
        cols[col2] = col
    df = df[cols]
    return df

# =============================================================================
# Расчет количества уникальных клиентов в день
# request_path - Путь к папке запросов
# forecast_path - Путь к папке хранилищу данных
# config - Массив строк для подключения к sql серверу
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# =============================================================================
def client_qty_by_day(request_path, forecast_path, config, start_date, end_date, f_log):        
    service.log('Расчет количества уникальных клиентов в день', f_log, False)
    service.log('Подключение к SQL серверу', f_log, True)
    con = pymssql.connect(host=config[0], user=config[1], password=config[2], database=config[3])
    service.log('Подключение установлено', f_log, True)
    # Загружаем текст запроса
    f = open(request_path + 'AvgSale.txt')    
    set_clqnty_request = f.read()
    f.close()
    # Загружаем фрейм
    service.log('Чтение из БД', f_log, True)
    df_clqnty = pd.read_sql(set_clqnty_request.format(start_date, end_date), con)
    service.log('Чтение данных завершено', f_log, True)
    con.close()
    df_clqnty = df_clqnty.astype({'Period': 'datetime64'})
    service.log('Обработка данных', f_log, True)
    df_clqnty = pd.pivot_table(df_clqnty, values='СреднийОтпуск', index=['Артикул','Склад'] , aggfunc=np.mean, fill_value=0)
    df_clqnty['СреднийОтпуск'] = np.around(df_clqnty['СреднийОтпуск'], decimals=2)
    df_clqnty = df_clqnty.reset_index()
    start_date = datetime.datetime.strptime(start_date.replace(",","-"), '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date.replace(",","-"), '%Y-%m-%d').date()
    # Имя файла для записи
    file_name = service.naming(start_date, end_date, 'Day')
    service.log('Запись данных в файл: ' + file_name, f_log, True)
    df_clqnty.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del df_clqnty, file_name
    
# =============================================================================
# Расчет продаж в день и агрегирование по месяцам
# path - Путь к корневой папке ресурсов
# request_path - Путь к папке запросов
# forecast_path - Путь к папке хранилищу данных
# config - Массив строк для подключения к sql серверу
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# =============================================================================
def aggr_sales(path, request_path, forecast_path, config, start_date, end_date, f_log):
    service.log('Расчет деневной статистики продаж и агрегирование по месяцам', f_log, False)
    service.log('Подключение к SQL серверу', f_log, True)
    con = pymssql.connect(host=config[0], user=config[1], password=config[2], database=config[3])
    service.log('Подключение установлено', f_log, True)
    # Загружаем текст запроса
    f = open(request_path + 'requestSell.txt')
    set_sell_request = f.read()
    f.close()
    # Загружаем фрейм
    service.log('Чтение из БД', f_log, True)
    df_sell = pd.read_sql(set_sell_request.format(start_date, end_date), con)
    service.log('Чтение данных завершено', f_log, True)
    df_sell = df_sell.astype({'Period': 'datetime64'})
    con.close()
    # Формирование даты из строки для запроса
    start_date = datetime.datetime.strptime(start_date.replace(",","-"), '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date.replace(",","-"), '%Y-%m-%d').date()
    # Список складов с сопоставлением для агрегации
    df_whouse = pd.read_excel(path + 'infWhouse.xlsx')
    service.log('Обработка данных по дням:', f_log, True) 
    df_sell = df_sell.merge(df_whouse, 'left', on='WH')
    df_sell = pd.pivot_table(df_sell, values='Qnty', index=['Art','Period','Whouse'] , aggfunc=np.sum, fill_value=0)
    df_sell = df_sell.reset_index()
    # Имя файла для записи
    file_name = service.naming(start_date, end_date, 'Day')
    service.log('Запись данных продаж по дням: ' + file_name, f_log, True)
    df_sell.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    service.log('Формирование сводной по месяцам:', f_log, True) 
    df_date = pd.to_datetime(start_date) + pd.to_timedelta(np.arange(((end_date - start_date).days + 1)), 'D')
    df_date = df_date.to_frame()
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    df_date.rename(columns={0: 'Period'}, inplace=True)
    df_date = df_date.astype({'Period': 'datetime64'})
    for index, row in df_date.iterrows():
        df_date.at[index, 'Year_Month'] = datetime.datetime.strftime(df_date.at[index, 'Period'], "%Y_%m")
    df_sell = df_date.merge(df_sell, 'left', on='Period')
    df_sell = pd.pivot_table(df_sell, values='Qnty', index=['Art','Year_Month','Whouse'] , aggfunc=np.sum, fill_value=0)
    df_sell = df_sell.reset_index()
    # Имя файла для записи
    file_name = service.naming(start_date, end_date, period_name='Month')
    service.log('Запись данных: ' + file_name, f_log, True)
    df_sell.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del df_sell, df_whouse, df_date, index, row, file_name, request_path, forecast_path

# =============================================================================
# Группировка остатка по дням и по месяцам в отдельные файлы
# path - Путь к корневой папке ресурсов
# rest_path - Путь к папке остатков
# forecast_path - Путь к папке хранилищу данных
# num_files - Количество файлов с данными по остаткам. Фактически, количество месяцев с расчитанным остатком
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# =============================================================================
def aggr_rest(path, rest_path, forecast_path, num_files, start_date, end_date, f_log):
    service.log('Группировка остатка по дням и по месяцам в отдельные файлы', f_log, False)    
    service.log('Загрузка данных из таблиц:', f_log, True)
    # Загружаем фрейм-список складов
    df_whouse = pd.read_excel(path + 'infWhouse.xlsx')
    df_whouse.rename(columns={"WH": "Склад"}, inplace=True)
    # df_rest - Period, Остаток, Артикул, Склад
    df_rest = pd.DataFrame()
    for i in range(1,num_files+1):
        # Дата файла
        date = service.get_date(start_date.year+((i-1)//12), i - 12 * ((i-1)//12))        
        # Имя файла
        file_name = service.naming(date, period_name='Day')
        # Чтение даных
        df_rest = df_rest.append(pd.read_csv(rest_path + file_name, sep=',', encoding='cp1251'))
        # Запись в лог
        if i > 1:
            service.log(file_name + ' Загружен. Операций соеденения произведено: ' + str(i-1), f_log, True)
        else:
            service.log(file_name + ' Загружен', f_log, True)
    del date, i
    df_rest = df_rest.astype({'Period': 'datetime64'})
    service.log('Данные загружены', f_log, True)
    service.log('Агрегация складов', f_log, True)
    df_rest = df_rest.merge(df_whouse, 'left', on='Склад')
    df_rest = df_rest.drop(columns=['Склад'])
    df_rest.rename(columns={'Whouse': 'Склад'}, inplace=True)
    df_rest = pd.pivot_table(df_rest, values='Остаток', index=['Артикул','Period','Склад'] , aggfunc=np.sum, fill_value=0)
    df_rest['Остаток'] = np.around(df_rest['Остаток'], decimals=2)
    df_rest = df_rest.reset_index()
    service.log('Загрузка уникальных артикулов', f_log, True)
    # Артикул
    df_art = pd.DataFrame(pd.unique(df_rest['Артикул']), columns=['Артикул'])
    df_art = df_art.reset_index()
    df_art = df_art.drop(columns=['index'])
    service.log('Загрузка уникальных складов', f_log, True)
    # Склад
    df_whouse = pd.DataFrame(pd.unique(df_rest['Склад']), columns=['Склад'])
    df_whouse = df_whouse.reset_index()
    df_whouse = df_whouse.drop(columns=['index'])
    service.log('Группировка по дням', f_log, True)
    # Запись файла по дням
    file_name = service.naming(start_date, end_date, period_name='Day', count=2)
    service.log('Запись данных: ' + file_name, f_log, True)
    df_rest.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Данные записаны', f_log, True)    
    service.log('Группировка по месяцам', f_log, True)
    df_rest['Year_Month'] = df_rest['Period'].map(lambda x: str(x.year) + '_' + service.fill_month(x.month))
    df_rest = pd.pivot_table(df_rest, values='Остаток', index=['Артикул','Year_Month','Склад'] , aggfunc=np.mean, fill_value=0)
    df_rest = df_rest.reset_index()
    df_rest['Остаток'] = np.around(df_rest['Остаток'], decimals=2)
    service.log('Соединение с календарем', f_log, True)
    # Календарь
    df_date = pd.to_datetime(start_date) + pd.to_timedelta(np.arange(((end_date - start_date).days + 1)), 'D')
    df_date = df_date.to_frame()
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    df_date.rename(columns={0: 'Period'}, inplace=True)
    df_date = df_date.astype({'Period': 'datetime64'})
    df_date['Year_Month'] = df_date['Period'].map(lambda x: str(x.year) + '_' + service.fill_month(x.month))
    df_date = df_date.groupby('Year_Month').count()
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['Period'])
    index = pd.MultiIndex.from_product([df_date['Year_Month'], df_whouse['Склад'], df_art['Артикул']], names = ['Year_Month', 'Склад', 'Артикул'])
    df_date = pd.DataFrame(index = index).reset_index()
    df_rest = df_date.merge(df_rest, 'left', on=['Year_Month','Склад','Артикул'])
    df_rest = df_rest.fillna(0)
    # Запись файла по имесяцам
    file_name = service.naming(start_date, end_date, period_name='Month', count=2)
    service.log('Запись данных: ' + file_name, f_log, True)
    df_rest.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del df_date, df_rest, df_art, df_whouse, end_date, index, path, start_date, file_name, rest_path, forecast_path,

# =============================================================================
# Расчет количества дней на остатке по месяцам
# forecast_path - Путь к папке хранилищу данных
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# =============================================================================  
def count_day_in_stock(forecast_path, start_date, end_date, f_log):
    service.log('Расчет количества дней на остатке по месяцам', f_log, False)
    # Календарь
    df_date = pd.to_datetime(start_date) + pd.to_timedelta(np.arange(((end_date - start_date).days + 1)), 'D')
    df_date = df_date.to_frame()
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    df_date.rename(columns={0: 'Period'}, inplace=True)
    df_date = df_date.astype({'Period': 'datetime64'})
    df_date['Year_Month'] = df_date['Period'].map(lambda x: str(x.year) + '_' + service.fill_month(x.month))
    file_name = service.naming(start_date, end_date, period_name='Day')
    service.log('Загрузка данных из таблицы остатков: ' + file_name, f_log, True)
    # Таблица остатков по дням - Артикул,Period,Склад,Остаток
    df_rest = pd.read_csv(forecast_path + file_name, sep=',',
                      dtype={'Артикул': 'object', 'Period': 'object','Склад': 'object','Остаток': 'int64'}, encoding='cp1251')
    df_rest = df_rest.astype({'Period': 'datetime64'})
    # Таблица среднего отпуска товара по дням - Period,Артикул,Склад,СреднийОтпуск
    file_name = service.naming(start_date, end_date, period_name='Day', count=2)
    service.log('Загрузка данных из таблицы среднего отпуска товара по дням: ' + file_name, f_log, True)
    df_clqnty = pd.read_csv(forecast_path + file_name, sep=',',
                      dtype={'Period': 'object', 'Артикул': 'object','Склад': 'object','СреднийОтпуск': 'float64'}, encoding='cp1251')
    df_rest = df_rest.merge(df_clqnty, 'left', on=['Артикул','Склад'])
    df_rest = df_rest.astype({'Period': 'datetime64'})
    df_rest = df_date.merge(df_rest, 'left', on='Period')
    service.log('Данные загружены', f_log, True)
    del df_clqnty, df_date
    # Артикул,Склад,Year_Month,ДнейНаОстатке
    df_rest['ДнейНаОстатке'] = np.where(df_rest['Остаток'] > df_rest['СреднийОтпуск'], 1, 0)
    df_rest = pd.pivot_table(df_rest, values='ДнейНаОстатке', index=['Year_Month','Артикул', 'Склад'] , aggfunc=sum, fill_value=0)
    df_rest = df_rest.reset_index()
    cols = df_rest.columns.tolist()
    #Перестановка столбцов
    col = cols[0]
    cols[0] = cols[1]
    cols[1] = cols[2]
    cols[2] = col
    df_rest = df_rest[cols]
    #Запись
    file_name = service.naming(start_date, end_date, period_name='Month', count=3)
    service.log('Запись данных: ' + file_name, f_log, True)
    df_rest.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del col, cols, df_rest, end_date, file_name, forecast_path, start_date
    
# =============================================================================
# Соединение всх таблиц, группированных по месяцам в одну
# forecast_path - Путь к папке хранилищу данных
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# =============================================================================  
def pivot_data(forecast_path, start_date, end_date, f_log):
    service.log('Соединение всх таблиц, группированных по месяцам в одну', f_log, False)
    file_name = service.naming(start_date, end_date, period_name='Month')
    service.log('Загрузка данных из таблицы остатков: ' + file_name, f_log, True)
    df_rest = pd.read_csv(forecast_path + file_name, sep=',', encoding='cp1251')
    df_rest.rename(columns={'Остаток': 'СреднийОстаток'}, inplace=True)
    file_name = service.naming(start_date, end_date, period_name='Month', count=2)
    service.log('Загрузка данных из таблицы продаж: ' + file_name, f_log, True)
    df_sell = pd.read_csv(forecast_path + file_name, sep=',', encoding='cp1251')
    df_sell.rename(columns={'Whouse': 'Склад'}, inplace=True)
    df_sell.rename(columns={'Art': 'Артикул'}, inplace=True)
    df_sell.rename(columns={'Qnty': 'Продано'}, inplace=True)
    service.log('Соединение остатков и продаж', f_log, True)
    result = pd.merge(df_rest, df_sell, how='left', on=['Артикул','Склад','Year_Month'])
    del df_sell, df_rest
    result = result.fillna(0)
    file_name = service.naming(start_date, end_date, period_name='Month', count=3)
    service.log('Загрузка данных из таблицы кол-ва дней на остатке: ' + file_name, f_log, True)
    df_count = pd.read_csv(forecast_path + file_name, sep=',', encoding='cp1251')
    service.log('Соединение остатков и продаж с количеством дней на остатке', f_log, True)
    result = pd.merge(result, df_count, how='left', on=['Артикул','Склад','Year_Month'])
    result = result.fillna(0)    
    result['Продано'] = np.around(result['Продано'], decimals=0)
    result = result.astype({'Продано': 'int64'})
    result['ДнейНаОстатке'] = np.around(result['ДнейНаОстатке'], decimals=0)
    result = result.astype({'ДнейНаОстатке': 'int64'})
    file_name = service.naming(start_date, end_date, count=4)
    service.log('Запись данных: ' + file_name, f_log, True)
    result.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del df_count, result, start_date, end_date, file_name, forecast_path
    
# =============================================================================
# Назначение статусов артикулам: были продажи, был остаток, есть разрыв в статистике остатков, новинка
# request_path - Путь к папке запросов
# forecast_path - Путь к папке хранилищу данных
# threshhold - количество месяцев, которые считаем "хвостом" для определеия новинок        
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# =============================================================================  
def set_status(request_path, forecast_path, threshhold, config, start_date, end_date, f_log):            
    service.log('Расчет статусов номенклатуры', f_log, False)
    service.log('Подключение к SQL серверу', f_log, True)
    con = pymssql.connect(host=config[0], user=config[1], password=config[2], database=config[3])
    service.log('Подключение установлено', f_log, True)
    # Загружаем текст запроса списка артикулов
    f = open(request_path + 'requestArt.txt')
    sql_art = f.read()
    f.close()
    # Загружаем список артикулов
    service.log('Чтение из БД', f_log, True)
    df_art_all = pd.read_sql(sql_art, con)
    con.close()
    del con, sql_art
    service.log('Чтение данных завершено', f_log, True)
    # Календарь
    df_date = pd.to_datetime(start_date) + pd.to_timedelta(np.arange(((end_date - start_date).days + 1)), 'D')
    df_date = df_date.to_frame()
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    df_date.rename(columns={0: 'Period'}, inplace=True)
    df_date = df_date.astype({'Period': 'datetime64'})
    df_date['Year_Month'] = df_date['Period'].map(lambda x: str(x.year) + '_' + service.fill_month(x.month))
    df_date = df_date.groupby('Year_Month').count()
    df_date = df_date.reset_index()
    df_date.rename(columns={'Period': 'DayCount'}, inplace=True) 
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    file_name = service.naming(start_date, end_date)
    service.log('Загрузка данных из сводной таблицы: ' + file_name, f_log, True)
    # Year_Month,Склад,Артикул,СреднийОстаток,Продано,ДнейНаОстатке
    df_rest = pd.read_csv(forecast_path + file_name, sep=',', 
                          dtype={'Year_Month': 'object', 'Склад': 'object','Артикул': 'object','СреднийОстаток': 'float64','Продано': 'int64','ДнейНаОстатке': 'int64'}, encoding='cp1251')
    df_rest = df_date.merge(df_rest, 'left', on='Year_Month')
    df_rest['Отличие'] = np.where(df_rest['ДнейНаОстатке'] < (df_rest['DayCount'] - threshhold), 1, 0)
    service.log('Загрузка уникальных артикулов', f_log, True)
    # Артикул
    df_art = pd.DataFrame(pd.unique(df_rest['Артикул']), columns=['Артикул'])
    df_art = df_art.reset_index()
    df_art = df_art.drop(columns=['index'])
    service.log('Загрузка уникальных складов', f_log, True)
    # Склад
    df_whouse = pd.DataFrame(pd.unique(df_rest['Склад']), columns=['Склад'])
    df_whouse = df_whouse.reset_index()
    df_whouse = df_whouse.drop(columns=['index'])
    full_set = set(df_art_all['Артикул'])
    del df_art_all, df_date
    # Присваиваем статус - Для тех артикулов у которых не было статистики продаж или остатков, назначаем статусы вручную
    present_set = set(df_art['Артикул'])
    df_art_all = pd.DataFrame(full_set.difference(present_set))
    del full_set, present_set
    df_art_all.rename(columns={0: 'Артикул'}, inplace=True) 
    #Артикул,Склад,IsEmpty,IsNew,IsGap
    index = pd.MultiIndex.from_product([df_art_all['Артикул'],df_whouse['Склад']], names = ['Артикул', 'Склад'])
    df_art_all = pd.DataFrame(index = index).reset_index()
    df_art_all['IsEmptySale'] = True
    df_art_all['IsEmpty'] = True
    df_art_all['IsGap'] = False
    df_art_all['IsNew'] = False
    del df_whouse
    # Расчет признаков для тех артикулов, у которых есть статистика
    service.log('Загрузка завершена', f_log, True)
    service.log('Расчт признаков: Наличие продаж; Остатка; Разрыв в остатке', f_log, True)
    df_rest = df_rest.sort_values(by=['Артикул','Склад','Year_Month'])
    #df_rest = df_rest.query("Склад == 'Новосибирск' and (Артикул == '2750' or Артикул == '2696')")
    #df_rest = df_rest.query("Склад == 'Питер'")
    #df_rest = df_rest.query("Склад == 'Ботаково' and (Артикул == '11' or Артикул == '4382' or Артикул == 'фк380')")
    # Расчет сводных признаков до артикула за месяц на складе
    df_rest['ID'] = df_rest['Артикул'] + df_rest['Склад']
    df_rest1 = pd.pivot_table(df_rest, values=['Продано', 'СреднийОстаток', 'Отличие', 'DayCount'], 
                              index=['Артикул','Склад'] , 
                              aggfunc={'Продано': np.sum, 'СреднийОстаток': np.sum, 'Отличие': np.sum, 'DayCount': 'count'}, 
                              fill_value=0)
    df_rest1 = df_rest1.reset_index()
    df_rest1['ID'] = df_rest1['Артикул'] + df_rest1['Склад']
    df_rest1 = df_rest1.drop(columns=['Артикул','Склад'])
    df_rest1.rename(columns={'DayCount': 'ВсегоМес', 
                             'Продано': 'ВсегоПродано', 
                             'СреднийОстаток': 'СрОстЗапериод', 
                             'Отличие': 'ВсегоОтличие'}, 
                    inplace=True) 
    result = df_rest.merge(df_rest1, 'left', on='ID')
    del df_rest1
    #Year_Month DayCount Склад Артикул СреднийОстаток Продано ДнейНаОстатке Отличие ВсегоМес ВсегоОтличие ВсегоПродано СрОстЗапериод
    df_rest = df_rest.drop(columns=['ID'])
    # Отбрасываем по месячные данные, оставляем только агрегированные по артикулу, по складу
    result = result.drop(columns=['ID', 'Year_Month','DayCount','СреднийОстаток','Продано','ДнейНаОстатке','Отличие']).drop_duplicates()
    #Склад	Артикул	ВсегоМес	ВсегоОтличие	ВсегоПродано	СрОстЗапериод
    result['IsEmptySale'] = np.where(result['ВсегоПродано'] == 0 , True, False)
    result['IsEmpty'] = np.where(result['СрОстЗапериод'] == 0 , True, False)
    result['IsGap'] = np.where(result['ВсегоОтличие'] != 0 , True, False)
    result = result.drop(columns=['ВсегоМес','ВсегоОтличие','ВсегоПродано','СрОстЗапериод'])
    # Ищем новинки только среди тех артикулов, которые имели разрыв в остатках
    service.log('Определение новинок', f_log, True)
    df_art = pd.DataFrame(pd.unique(result.query("IsGap == True")['Артикул']), columns=['Артикул'])
    df_art = df_art.reset_index()
    df_art = df_art.drop(columns=['index'])
    df_art['1'] = 1
    # Формируем исходный фрейм со статистикой по тем артикулам, для которых будем расчитывать статус новинки
    df_res = df_rest.drop(columns=['Продано', 'DayCount','Отличие','СреднийОстаток'])
    df_res = df_res.merge(df_art, 'left', on='Артикул')
    del df_art, df_rest
    df_res = df_res.dropna(how='any')
    df_res = df_res.drop(columns=['1'])
    df_res = df_res.reset_index()
    df_res = df_res.drop(columns=['index'])
    df_res['ID'] = df_res['Артикул'] + df_res['Склад']
    # date_count - количество месяцев всего
    # head_count - количество месяцев, остатка в которых не должно быть, для того, чтобы считать позицию новой
    date_count = pd.DataFrame(pd.unique(df_res['Year_Month']), columns=['Year_Month'])['Year_Month'].count()
    head_count = date_count - threshhold
    # Разделяем исходный фрейм на два, в первом те месяцы, в которых не должно быть остатка
    # во втором, месяцы с остатком
    df_head = df_res.groupby('ID').head(head_count).reset_index(drop=True)
    df_tail = df_res.groupby('ID').tail(threshhold).reset_index(drop=True)
    del date_count, head_count, threshhold
    # Создаем соответствующие сводные за период по складу и артикулу
    df_head = pd.pivot_table(df_head, values=['ДнейНаОстатке'], 
                             index=['Артикул','Склад'] , 
                             aggfunc={'ДнейНаОстатке': np.sum}, 
                             fill_value=0)
    df_tail = pd.pivot_table(df_tail, values=['ДнейНаОстатке'], 
                             index=['Артикул','Склад'] , 
                             aggfunc={'ДнейНаОстатке': np.sum}, 
                             fill_value=0)
    # Присваиваем статус новинки
    df_head['IsNew'] = np.where(df_head['ДнейНаОстатке'] == 0 , True, False)
    df_tail['IsNew'] = np.where(df_tail['ДнейНаОстатке'] > 0 , True, False)
    # Если оба условия True присваиваем окончательны статус
    df_res = df_head.merge(df_tail, 'left', on=['Артикул','Склад'])
    # Соединяем с результирующим фреймом и проставляем в нем, False, для остальных позиций
    del df_head, df_tail
    df_res['IsNew'] = np.where((df_res['IsNew_x'] == True) & (df_res['IsNew_y'] == True) , True, False)
    df_res = df_res.drop(columns=['ДнейНаОстатке_x','ДнейНаОстатке_y', 'IsNew_x', 'IsNew_y'])
    df_res = df_res.reset_index()
    result = result.merge(df_res, 'left', on=['Артикул','Склад'])
    del df_res
    result = result.fillna(False)
    # Добавляем номенклатуру без продаж и остатков
    service.log('Добавление статусов номенклатуры без статистики', f_log, True)
    result = result.append(df_art_all, sort=False)
    result = result.reset_index()
    result = result.drop(columns=['index'])
    file_name = service.naming(start_date, end_date, count=2)
    service.log('Запись данных: ' + file_name, f_log, True)
    result.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del df_art_all, end_date, forecast_path, request_path, result, start_date, file_name

# =============================================================================
# Формирование Базовой Аналитической Таблицы
# path - Путь к корневой папке ресурсов
# forecast_path - Путь к папке хранилищу данных
# start_date - начало периода
# end_date - конец периода
# f_log - файл лога
# ============================================================================= 
def set_ABT(path, forecast_path, start_date, end_date, f_log):            
    service.log('Создание базовой аналитической таблицы', f_log, False)
    # Календарь
    df_date = pd.to_datetime(start_date) + pd.to_timedelta(np.arange(((end_date - start_date).days + 1)), 'D')
    df_date = df_date.to_frame()
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    df_date.rename(columns={0: 'Period'}, inplace=True)
    df_date = df_date.astype({'Period': 'datetime64'})
    df_date['Year_Month'] = df_date['Period'].map(lambda x: str(x.year) + '_' + service.fill_month(x.month))
    # Определение максимальной даты в календаре
    period_count = df_date['Period'].count() - 1    
    max_date = df_date.at[period_count, 'Period']
    # Дата начала прогнозируемого месяца
    max_date = max_date + datetime.timedelta(days=1)
    # Количество дней в прогнозируемом месяце
    #days = calendar.monthrange(max_date.year, max_date.month)[1]
    df_date = df_date.groupby('Year_Month').count()
    df_date = df_date.reset_index()
    df_date.rename(columns={'Period': 'ДнейВМесяце'}, inplace=True) 
    df_date = df_date.reset_index()
    df_date = df_date.drop(columns=['index'])
    file_name = service.naming(start_date, end_date)
    service.log('Загрузка данных из сводной таблицы: ' + file_name, f_log, True)
    df_rest = pd.read_csv(forecast_path + file_name, sep=',', 
                          dtype={'Year_Month': 'object', 'Склад': 'object','Артикул': 'object','СреднийОстаток': 'float64','Продано': 'int64','ДнейНаОстатке': 'int64'}, encoding='cp1251')
    service.log('Данные загружены', f_log, True)
    df_rest = df_date.merge(df_rest, 'left', on='Year_Month')     
    service.log('Добавление количества дней в каждом месяце', f_log, True)
    df_rest = df_rest.sort_values(by=['Артикул','Склад','Year_Month'])
    #df_rest = df_rest.query("Склад == 'Новосибирск' and (Артикул == '2750' or Артикул == '2696')")
    # Перестановка столбцов
    df_rest = column_shift(df_rest, 1, 3, 0, False)
    df_rest = column_shift(df_rest, 3, 5, 1, True)
    file_name = service.naming(start_date, end_date, count=2)
    service.log('Загрузка данных из сводной таблицы: ' + file_name, f_log, True)
    df_status = pd.read_csv(forecast_path + file_name, sep=',', encoding='cp1251')
    service.log('Данные загружены', f_log, True)
    df_rest = df_rest.merge(df_status, 'left', on=['Артикул','Склад'])
    service.log('Соединение таблиц', f_log, True)
    del df_status
    # Перестановка столбцов
    df_rest = column_shift(df_rest, 3, 6, 4, True)
    df_rest['IsImputation'] = False
    df_rest['Forecasted'] = False
    # Перестановка столбцов
    df_rest = column_shift(df_rest, 7, 8, 4, True)
    # Соединяем никальные значениz Год_месяц из df_date со всеми артикулами и складами
    # Декартово произведение трех таблиц, соединяем с df_rest. Получим строку прогноза
    # Уникальные значениz артикулов
    #df_art = pd.DataFrame(pd.unique(df_rest['Артикул']), columns=['Артикул'])
    # Уникальные значениz складов
    #df_whouse = pd.DataFrame(pd.unique(df_rest['Склад']), columns=['Склад'])
    file_name = service.naming(start_date, end_date, count=3)
    service.log('Запись данных: ' + file_name, f_log, True)
    df_rest.to_csv(forecast_path + file_name, encoding='cp1251',index=False)
    service.log('Запись завершена', f_log, True)
    del df_rest, file_name, df_date, max_date, period_count 