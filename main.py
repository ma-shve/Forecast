# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:38:08 2020

@author: Швецов
"""
import service
import operation
import datetime
import configparser

# Считывание файла конфигурации
config = configparser.ConfigParser()
config.read('config.ini')
# Конфигурация подключения к БД
con_cfg = []
for key in config['connection']:  
    con_cfg.append(config['connection'][key])
# Проверка существования путей
for key in config['dir']:
    service.exists_path(config['dir'][key])
# Очистка папки прогноза
service.clean_path(config['dir']['forecast_dir'])
# Путь к папке ресурсов
res_dir = config['dir']['res_dir']
# Год начала расчета
year = int(config['start_date']['year'])
# Месяц начала расчета
month = int(config['start_date']['month'])
# Определение границ периода статистики
start_date = service.get_date(year, month)
get_end_date =service.get_end_date(config['dir']['rest_dir'], year)
end_date = get_end_date[0]
# Количество месяцев статистики 
num_files = get_end_date[1]
now = datetime.datetime.now()
# Создаем файл лога
log_file_name = 'Forecast_log_' +str(now.year) + str(service.fill_month(now.month)) + str(service.fill_month(now.day)) + '.txt' 
f_log = open(config['dir']['log_dir'] + log_file_name, 'w')
service.log('НАЧАЛО', f_log, False)
service.log('Начало периода: ' + str(start_date), f_log, False)
service.log('Конец периода:  ' + str(end_date), f_log, False) 
# Расчет количества уникальных клиентов в день и группировка продаж по дням и по месяцам
operation.client_qty_by_day(config['dir']['request_dir'], config['dir']['forecast_dir'], con_cfg, str(start_date).replace('-',','), str(end_date).replace('-',','), f_log)
# Расчет продаж в день и агрегирование по месяцам
operation.aggr_sales(res_dir, config['dir']['request_dir'], config['dir']['forecast_dir'], con_cfg, str(start_date).replace('-',','), str(end_date).replace('-',','), f_log)
# Группировка продаж по дням и по месяцам
operation.aggr_rest(res_dir, config['dir']['rest_dir'], config['dir']['forecast_dir'], num_files, start_date, end_date, f_log)
# Расчет количества дней на остатке по месяцам
operation.count_day_in_stock(config['dir']['forecast_dir'], start_date, end_date, f_log)
# Соединение всх таблиц, группированных по месяцам в одну
operation.pivot_data(config['dir']['forecast_dir'], start_date, end_date, f_log)
# Назначение статусов артикулам: были продажи, был остаток, есть разрыв в статистике остатков, новинка
operation.set_status(config['dir']['request_dir'], config['dir']['forecast_dir'], int(config['boards']['threshhold']), con_cfg, start_date, end_date, f_log)    
# Формирование Базовой Аналитической Таблицы
operation.set_ABT(res_dir, config['dir']['forecast_dir'], start_date, end_date, f_log)
service.log('КОНЕЦ', f_log, False)
f_log.close()
del res_dir, start_date, end_date, year, month, config, con_cfg, num_files, now, log_file_name, key, get_end_date