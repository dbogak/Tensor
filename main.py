"""
Главный модуль программы.

Этот модуль отвечает за инициализацию базы данных PostgreSQL, проверку её состояния
и выполнение основных операций. Включает в себя функции для работы с базой данных,
ввода данных пользователем и их обработки.

Для корректной работы с базой данных PostgreSQL, необходимо установить библиотеку psycopg2,
которая используется для подключения и работы с базой данных.

Установка библиотек может быть выполнена с помощью команды:
pip install -r requirements.txt

База данных PostgreSQL должна быть настроена согласно конфигурации, указанной в файле config.py.
"""

import os
import json



def add_city_id_to_employees(data):
    """ Добавляет ID города к сотрудникам """
    # Словарь для быстрого доступа к элементам по их ID
    lookup = {item['id']: item for item in data}

    # Рекурсивная функция для нахождения city_id
    def find_city_id(current_id):
        parent_id = lookup[current_id]['ParentId']
        if parent_id is None:
            return None  # Достигли корня иерархии
        if lookup[parent_id]['Type'] == 1:
            return parent_id  # Если родитель - офис, возвращаем его ID
        else:
            # Иначе рекурсивно идем выше по иерархии
            return find_city_id(parent_id)

    # Обновление элементов списка
    for item in data:
        if item['Type'] == 1 or item['Type'] == 2:  # Для офисов и отделов
            item['CityId'] = None
        else:  # Для сотрудников
            item['CityId'] = find_city_id(item['id'])

    return data


def database_check():
    """ Проверка существования базы данных """
    # Проверим существует ли база данных 
    data = db.check_table_exists()
    if not data:
        # Если база данных еще не создана, то создадим ее
        # Получаем данные из файла `data.json` с кодировкой UTF-8
        with open('data.json', encoding='utf-8') as file:
            data = json.load(file)
        # Заполняем базу `Organizations` данными из файла `data.json`
        new_data = add_city_id_to_employees(data)
        db.create_table(new_data)


def input_employee_id():
    """Ввод идентификатора сотрудника и проверка на цифру или 0 для завершения"""
    print("\033[0m\033[32m")
    employeer_id = input("\nВведите id Сотрудника (введите 0 для завершения): ")
    # Проверка на 0 для завершения
    if employeer_id == '0':
        print("\033[0mВыход из программы.")
        exit()
    # Проверка на цифру
    elif employeer_id.isdigit():
        return employeer_id
    else:
        print("Вам необходимо ввести цифру.")
        return input_employee_id()


def main_screen():
    """ Выведем всю информацию по базе данных """
    clear_command_line()
    cities = []
    departs = []
    employees = []
    data = db.get_all_data()
    for d in data:
        if d["Type"] == 1:
            cities.append(d)
        elif d["Type"] == 2:
            departs.append(d)
        else:
            employees.append(d)

    print(
        f"\033[1mЧисло городов: {len(cities)}. Всего отделов: {len(departs)}. Сотрудников:  {len(employees)}.\033[0m")


def additional_info(parent_id):
    """Вывод дополнительной информации
    поиск соседей по офису"""
    roommates = db.roommate_search(parent_id)
    room_string = ""
    for men in roommates:
        room_string += f' {men["Name"]}'
    print(f"\033[35mСписок всех сотрудников в отделе: {room_string}")
    city, employee_list = db.employee_search(parent_id)
    employee_string = ""
    for men in employee_list:
        employee_string += f'{men["Name"]}, '
    employee_string = employee_string[:-2] + "."
    print(f"\033[34mВ городе {city} находятся следующие сотрудники: \033[35m\033[3m{employee_string}")


def main():
    """ Главная функция. Получаем айди и выводим имя сотрудника """
    employee_id = input_employee_id()
    # Поиск сотрудника по его айди из базы данных
    employee = db.find_employee_by_employee_id(int(employee_id))

    if employee is not None:
        if employee["Type"] == 3:
            # Выводим имя сотрудника
            print(f"\033[1m\033[35m{employee['Name']}\033[0m")
            # Выводим дополнительную информацию по офисам
            additional_info(employee["ParentId"])
        elif employee["Type"] == 2:
            # Выводим название отдела
            print(
                f"\033[33mДанный идентификатор принадлежит отделу \033[34m{employee['Name']}")
        elif employee["Type"] == 1:
            # Выводим название города
            print(
                f"\033[33mДанный идентификатор принадлежит городу \033[34m{employee['Name']}")
    else:
        # Выводим сообщение об ошибке
        print("\033[31m\033[3mСотрудник не найден\033[0m")


def select_database():
    """Выбор базы данных"""
    clear_command_line()
    database_choice = input("\033[94m1. MongoDB\n2. Postgres\n0. Выход\nВыберите базу данных : ")
    
    if database_choice == "1":
        # Код для подключения к MongoDB
        from mongo_database import db
        return db
    elif database_choice == "2":
        # Код для подключения к Postgres
        from pg_db import db
        return db
    else:
        print("Неверный выбор базы данных")
        select_database()

def clear_command_line():
    os.system('cls' if os.name == 'nt' else 'clear')

if __name__ == "__main__":
    # Запросим выбор базы данных синим цветом
    db = select_database()
    database_check()
    main_screen()
    while True:
        main()
