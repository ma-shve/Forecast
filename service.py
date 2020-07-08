import datetime
import calendar
import traceback
import os

# Политика имен файлов для хранилища
def naming(start_date=None, end_date=None, period_name='', count=1):
    # Имя вызывающей функции
    func_name = traceback.extract_stack()[-3][3]
    func_name = func_name[(func_name.find('.')+1):func_name.find('('):]
    # Расчет количества уникальных клиентов в день
    # Имя хранилища
    if func_name == 'client_qty_by_day':
        if start_date.year != end_date.year:
            file_name = 'AvgSaleBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
        else:
            file_name = 'AvgSaleBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
    # Расчет продаж в день и агрегирование по месяцам
    # Имя хранилища
    if func_name == 'aggr_sales':
        if start_date.year != end_date.year:
            file_name = 'SellBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
        else:
            file_name = 'SellBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'            
    # Группировка остатка по дням и по месяцам в отдельные файлы
    # Имя файла с остатком целевого месяца
    if func_name == 'aggr_rest' and count == 1:
        if start_date.month < 10:
            file_name = 'RestBy' + period_name + '_' + str(start_date.year) + '_0' + str(start_date.month) + '.csv'            
        else:
            file_name = 'RestBy' + period_name + '_' + str(start_date.year) + '_' + str(start_date.month) + '.csv'
    # Имя хранилища
    elif func_name == 'aggr_rest' and count == 2:
        if start_date.year != end_date.year:
            file_name = 'RestBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
        else:
            file_name = 'RestBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
    # Расчет количества дней на остатке по месяцам
    # Имя таблицы остатков по дням
    if func_name == 'count_day_in_stock' and count == 1:
        file_name = 'RestBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
    # Имя таблицы среднего отпуска товара по дням    
    elif func_name == 'count_day_in_stock' and count == 2:
        file_name = 'AvgSaleBy' +  period_name + '_' + date_name(start_date, end_date) + '.csv'
    # Имя хранилища
    elif func_name == 'count_day_in_stock' and count == 3:
        if start_date.year != end_date.year:
            file_name = 'DaysInStockBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
        else:
            file_name = 'DaysInStockBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
    # Соединение всх таблиц, группированных по месяцам в одну       
    if func_name == 'pivot_data' and count == 1:
        file_name = 'RestBy' + period_name + '_' + date_name(start_date, end_date) + '.csv'
    elif func_name == 'pivot_data' and count == 2:
        file_name = 'SellBy' +  period_name + '_' + date_name(start_date, end_date) + '.csv'
    elif func_name == 'pivot_data' and count == 3:
        file_name = 'DaysInStockBy' +  period_name + '_' + date_name(start_date, end_date) + '.csv'
    elif func_name == 'pivot_data' and count == 4:
        file_name = 'PivotData_' + date_name(start_date, end_date) + '.csv'
    # Назначение статусов артикулам: были продажи, был остаток, есть разрыв в статистике остатков, новинка
    if func_name == 'set_status' and count == 1:
        file_name = 'PivotData_' + date_name(start_date, end_date) + '.csv'
    elif func_name == 'set_status' and count == 2:
        file_name = 'IsNew_' + date_name(start_date, end_date) + '.csv'
    # Перестановка столбцов во фрейме в произвольном порядке
    if func_name == 'set_ABT' and count == 1:
        file_name = 'PivotData_' + date_name(start_date, end_date) + '.csv'
    elif func_name == 'set_ABT' and count == 2:
        file_name = 'IsNew_' + date_name(start_date, end_date) + '.csv'
    elif func_name == 'set_ABT' and count == 3:
        file_name = 'ABT_' + date_name(start_date, end_date) + '.csv'
 
    return file_name
# Проверка на существование пути
def exists_path(testpath):
    if os.path.exists(testpath):
        print ("Папка %s существует" % testpath)
        return True
    else:
        try:
            os.makedirs(testpath)
        except OSError:
            print ("Создать папку %s не удалось" % testpath)
        else:
            print ("Успешно создана папка %s" % testpath)
# Очистка папки прогноза
def clean_path(testpath):
    for file in os.listdir(testpath):
        file_path = os.path.join(testpath, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)            
        except Exception as e:
            print(e)
    print ("Папка %s очищена" % testpath)
# Добавление лидирующего 0 к номеру месяца
def fill_month(month):
    if month < 10:
        res = '0' + str(month)
    else:
        res = str(month)
    return res

def get_end_date(path, year):
    # Количество файлов с данными по остаткам. Фактически, количество месяцев с расчитанным остатком
    num_files = sum(os.path.isfile(os.path.join(path, f)) for f in os.listdir(path))
    if num_files%12 == 0:
        end_date = get_next_date(year+(int(num_files/12)-1), 12)
    else:
        end_date = get_next_date(year+(int(num_files/12)), num_files%12)
    return end_date, num_files

# Функция ведения лога
def log(message, file, adddate):         
    # Имя вызывающей функции
    name = traceback.extract_stack()[-3][3]
    name = name[(name.find('.')+1):name.find('('):]
    if adddate:
        now = datetime.datetime.now()
        # Выравнивание текста
        msg = now.strftime("%d-%m-%Y %H:%M:%S") + ' -- ' + name + ' -- ' + message
        msg = msg.ljust(150, '-')
        # Сообщентие
        print(msg, file=file) 
        print(msg)
    else:
        # Выравнивание текста
        msg = message
        msg = msg.center(150, '*')
        # Сообщентие
        print(msg, file=file)
        print(msg)    
# Функция текущей даты
def get_date(year, month):
    return datetime.date(year, month, 1)
# Функция последнего дня текущего месяца
def get_next_date(year, month):
    return datetime.date(year, month, calendar.monthrange(year, month)[1])
# Функция следующего месяца текущей даты
def get_next_month(year, month):
    if month == 12:
        return datetime.date(year + 1, 1, 1)
    else:
        return datetime.date(year, month + 1, 1)
# Последний день месяца
def get_last_day(year, month):
    start_date = datetime.date(year, month, 1)
    day_list = (start_date, calendar.mdays[month] + (month == 2 and calendar.isleap(year)))
    return day_list[1]
#Формирование короткой даты для наименования файлов
def date_name(start_date, end_date):
    if start_date.month < 10: 
        if end_date.month < 10: 
            return str(start_date.year) + '0' + str(start_date.month) + '_' + str(end_date.year) + '0' + str(end_date.month)
        else:
            return str(start_date.year) + '0' + str(start_date.month) + '_' + str(end_date.year) + str(end_date.month)
    else:
        if end_date.month < 10: 
            return str(start_date.year) + str(start_date.month) + '_' + str(end_date.year) + '0' + str(end_date.month)
        else:
            return str(start_date.year) + str(start_date.month) + '_' + str(end_date.year) + str(end_date.month)