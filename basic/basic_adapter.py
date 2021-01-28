"""
Модуль содержит класс BasicAdapter.

:author: Andrei Ursaki.
"""
import json

from basic.request import Request
from basic.sql_helper import SqlHelper


class BasicAdapter:
    def __init__(self, telemetry_system_id: int, endpoint: str, content_type: str):
        """
        Конструктор класса.

        :param telemetry_system_id: идентификационный номер телеметрической системы
        :param endpoint: url/endpoint адаптера
        :param content_type: тип данных, используемых в сообщении
        """
        self.telemetry_system_id = telemetry_system_id
        self.endpoint = endpoint
        # создание объекта класса Request, для отправки сообщений адаптеру
        self.r = Request(endpoint, content_type)
        # создание объекта класса SqlHelper, для работы с базой данных
        self.sh = SqlHelper(telemetry_system_id=telemetry_system_id)

    def __check_data(self, sensor_code: str, info_dict: dict, route: str, header: dict = None) -> dict:
        """
        Метод для отправки запроса в CoordCom CardChecker.

        :param sensor_code: код объекта/датчика
        :param info_dict: словарь с проверяемыми значениями
        :param route: адрес метода проверки
        :param header: заголовки для запроса
        :return: словарь с результатами проверки, см. https://gitlab.sphaera.ru/coordcom/testers-projects/coordcom-card-checker
        """
        # добавляем в словарь с проверяемыми значениями информацию об объекте/датчике
        info_dict.update({'telemetry_system_id': self.telemetry_system_id, 'sensor_code': sensor_code})
        # с помощью класса Request выполняем запрос
        result = Request.send_request(json.dumps(info_dict, ensure_ascii=False), f'http://10.100.122.5:5001/{route}',
                                      'application/json', headers=header, print_msg=True)
        # преобразуем результаты в словарь
        result = json.loads(result).get("response")
        if result:
            # если есть результат, возвращаем его
            return result

    def check_card(self, sensor_code: str, card_info: dict) -> dict:
        """
        Метод для проверки карточки.

        :param sensor_code: код объекта/датчика
        :param card_info: словарь с проверяемыми значениями
        :return: словарь с результатами проверки
        """
        return self.__check_data(sensor_code, card_info, "checkByExternalSystemReference")

    def check_co(self, sensor_code: str, co_info: dict, header: dict = None) -> dict:
        """
        Метод для проверки атрибутов кастомного объекта.

        :param sensor_code: код объекта/датчика
        :param co_info: словарь с проверяемыми значениями
        :param header: заголовки для запроса, нужен для указания базы данных кастомных объектов
        :return: словарь с результатами проверки
        """
        return self.__check_data(sensor_code, co_info, "checkCustomObject", header)

    def get_sensors(self, state: int) -> list:
        """
        Метод для получения списка объектов/датчиков.

        :param state: необходимое состояние объектов/датчиков (0 - датчики из t_sensor, 1 - с открытыми карточками, 2 - без открытых карточек)
        :return: список объектов/датчиков
        """
        sensors = self.sh.get_all_sensor_codes()
        sensors_with_open_card = self.sh.get_all_sensors_with_open_card()
        if state == 0:
            return sensors
        elif state == 1:
            return sensors_with_open_card
        elif state == 2:
            return list(set(sensors) - set(sensors_with_open_card))

    def check_card_for_notification(self, sensor_code):
        """
        Метод для проверки напоминаний в КК.

        :param sensor_code: код объекта/датчика
        :return: True/False
        """
        return self.sh.is_notification_in_card(sensor_code)
