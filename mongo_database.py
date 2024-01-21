""" Для работы Базы данных скачать ее и установить все по дефолту
Ссылка на скачивание https://drive.google.com/file/d/1kvmRCtW4ogS0Hx41Ol_fZKImo08yzquf/view?usp=sharing """

import pymongo
from config import table_name


class MongoDB:
    def __init__(self):
        # Подключение к базе данных MongoDB
        self.client = pymongo.MongoClient("mongodb://localhost:27017")
        # Выбор коллекции "Data" в базе данных "Offices"
        self.Offices = self.client[table_name]['Data']

    def check_table_exists(self):
        """
        Проверяет наличие базы данных по имени.

        Аргументы:
        - table_mame (str): Имя базы данных.

        Возвращает:
        - bool: True, если база данных с указанным именем существует, и False в противном случае.
        """
        database_names = self.client.list_database_names()
        return table_name in database_names

    def create_table(self, data):
        """
        Создает базу данных и заполняет ее данными.

        Аргументы:
        - data (list): Список данных для заполнения базы данных.
        """
        self.Offices.insert_many(data)

    def find_employee_by_employee_id(self, employee_id):
        """
        Ищет сотрудника по его идентификатору.

        Аргументы:
        - employee_id (int): Идентификатор сотрудника.

        Возвращает:
        - dict: Словарь с информацией о сотруднике, или None, если сотрудник не найден.
        """
        employee = self.Offices.find_one({"id": employee_id})
        return employee

    def get_all_data(self):
        """
        Получает все данные из коллекции.

        Возвращает:
        - list: Список словарей с данными из коллекции.
        """
        return list(self.Offices.find({}, {'_id': 0}))

    def roommate_search(self, parent_id):
        """
        Ищет соседей по офису.

        Аргументы:
        - parent_id (int): Идентификатор офиса.

        Возвращает:
        - list: Список словарей с информацией о соседних офисах.
        """
        return list(self.Offices.find({"ParentId": parent_id}, {'_id': 0}))

    def employee_search(self, parent_id):
        """
        Поиск в списке данных data_list всех сотрудников с заданным идентификатором города.

        Параметры:
        - employee (dict): Словарь с информацией о сотруднике.

        Возвращает:
        - tuple: Кортеж с названием города и списком сотрудников, работающих в этом городе.
        """
        # Узнаем из какого он отдела
        depart = self.Offices.find_one({"ParentId": parent_id})

        # Получим информацию по городу
        city = self.Offices.find_one(
            {"id": depart["ParentId"]}, {'_id': 0})

        # Если полученный объект city не является офисом, то прибавим id родительского отдела
        while city["Type"] != 1:
            city = self.Offices.find_one(
                {"id": city["ParentId"]}, {'_id': 0})


        # Получим список сотрудников в городе
        emploees_list = list(self.Offices.find(
            {"CityId": city["id"]}, {'_id': 0}))

        return city["Name"], emploees_list
    

db = MongoDB()
