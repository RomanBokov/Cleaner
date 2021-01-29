"""
Модуль содержит класс LogReader.

:author: Andrei Ursaki.
"""
import json
from datetime import datetime

import allure

from basic.config import Config
from basic.request import Request


class LogReader:

    def __init__(self, server: str, start_datetime: datetime, end_datetime: datetime):
        """
        Конструктор класса.

        :param server: ip адрес сервера, на котором расположены логи.
        :param start_datetime: дата-время для начала поиска в логах
        :param end_datetime: дата-время для окончания поиска в логах
        """
        self.file_path_integration = fr"\\{server}{Config.integration_logs_path}"
        self.file_path_layer_object = fr"\\{server}{Config.layer_objects_logs_path}"
        self.start_dt_iso_str = start_datetime.isoformat(timespec="seconds")
        self.end_dt_iso_str = end_datetime.isoformat(timespec="seconds")

    def get_logs(self, file_path: str, find_dict: dict, pretty_print: bool = False, log_count: int = None,
                 full_file_search: bool = False) -> list:
        """
        Метод для получения списка логов.

        :param file_path: путь к файлу в котором необходимо произвести поиск
        :param find_dict: словарь с данными, по которым будеи произведен поиск
        :param pretty_print: возвращать ли данные в более читабельном виде
        :param log_count: ограничение количества возвращаемых логов
        :param full_file_search: производить ли поиск по всему файлу
        :return: список найденных логов
        """
        # составляем словарь с параметрами поиска
        msg = {"file_path": file_path, "find": find_dict, "pretty": pretty_print}
        if not full_file_search:
            # если поиск будем производить не во всем файле, добавляем время начала и конца
            msg.update({"from": self.start_dt_iso_str, "to": self.end_dt_iso_str})
        if log_count:
            # если необходимо добавляем ограничение по количеству логов
            msg.update({"log_count": log_count})
        # формируем json
        msg = json.dumps(msg, ensure_ascii=False)
        # отправляем запрос в LogChecker
        result = Request.send_request(msg, "http://10.100.122.5:5002/findLogs", "application/json", print_msg=True)
        try:
            # пытаемся получить данные из пришедшего json'а
            result = json.loads(result)
            if result.get("error"):
                return [result]
            else:
                return result.get("found_lоgs")
        except json.decoder.JSONDecodeError:
            # если ответ не содержит json возвращаем ошибку
            return [{"error": "Необработанная серверная ошибка"}]

    def get_log_for_rule(self, rule_name: str) -> list:
        """
        Метод для получения первого из логов по выбранному правилу.

        :param rule_name: название правила
        :return: список с первым найденым логом
        """
        # формируем поисковой запрос
        find_dict = {"sphaera_process": "Sphaera.Telemetry.Cep",
                     "sphaera_data": [{"data": f"<statementName>{rule_name}</statementName>"}]}
        return self.get_logs(self.file_path_integration, find_dict, log_count=1)

    def get_chain_logs(self, file_path: str, chain_id: str) -> list:
        """
        Метод для получения логов по "чейну"(уникальному идентификатору цепочки логов).

        :param file_path: путь к файлу в котором необходимо произвести поиск
        :param chain_id: уникальный идентификаторуцепочки логов
        :return: список найденных логов
        """
        # формируем поисковой запрос
        find_dict = {"sphaera_x_operation_id": chain_id}
        return self.get_logs(file_path, find_dict, full_file_search=True)

    @allure.step("Получение цепочки логов для правила {1}")
    def get_chain_for_rule(self, rule_name: str) -> list:
        """
        Метод для получения цепочки логов по выбранному правилу. Используется комбинация из 2 предыдущих методов.

        :param rule_name: название правила
        :return: список найденных логов
        """
        # получаем список с первым логом по правилу
        rule_log = self.get_log_for_rule(rule_name)
        if rule_log:
            logs = rule_log[0]
            if "error" not in list(logs.keys()):
                # если получили лог, получаем его "чейн"
                log_id = logs.get("sphaera_x_operation_id")
                # получаем логи по данному "чейну"
                logs = self.get_chain_logs(self.file_path_integration, log_id)
            # прикрепляем полученные логи для отчетности
            allure.attach(json.dumps(logs, ensure_ascii=False, indent=4), f"Логи для правила {rule_name}",
                          allure.attachment_type.JSON)
            return logs

    def get_log_for_layer_object(self, layer_obj_id: str) -> list:
        """
        Метод для получения первого из логов по выбранному кастомному объекту.

        :param layer_obj_id: уникальный идентификатор кастомного объекта
        :return: список с первым найденым логом
        """
        # формируем поисковой запрос
        find_dict = {"sphaera_operation": "CreateOrUpdateElement", "sphaera_data": [{"data": layer_obj_id}]}
        return self.get_logs(self.file_path_layer_object, find_dict, log_count=1)

    @allure.step("Получение логов для кастомного объекта с id {1}")
    def get_chain_for_layer_object(self, layer_obj_id):
        """
        Метод для получения цепочки логов по выбранному кастомному объекту.

        :param layer_obj_id: уникальный идентификатор кастомного объекта
        :return: список найденных логов
        """
        # получаем список с первым логом по кастомному объекту
        layer_object_log = self.get_log_for_layer_object(layer_obj_id)
        if layer_object_log:
            logs = layer_object_log[0]
            if "error" not in list(logs.keys()):
                # если получили лог, получаем его "чейн"
                log_id = logs.get("sphaera_x_operation_id")
                # получаем логи по данному "чейну"
                logs = self.get_chain_logs(self.file_path_layer_object, log_id)
            # прикрепляем полученные логи для отчетности
            allure.attach(json.dumps(logs, ensure_ascii=False, indent=4),
                          f"Логи для для кастомного объекта с id {layer_obj_id}", allure.attachment_type.JSON)
            return logs
