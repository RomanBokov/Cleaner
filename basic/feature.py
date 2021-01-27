"""
Модуль содержит класс Feature, содержащий методы для "расширенной" печати/вывода.

:author: Andrei Ursaki.
"""
import json


class Feature:
    @staticmethod
    def print_list(input_list: list, sep: str = None):
        """
        Метод для печати/вывода списка.

        :param input_list: входящий список
        :param sep: разделитель для элементов списка
        """
        if sep:
            # печать/вывод списка с разделителем
            print(f"{sep}".join(input_list))
        else:
            # обычная печать/вывод списка
            print(input_list)
        # печать/вывод длины списка
        print(len(input_list))

    @staticmethod
    def print_pretty(s):
        """
        Метод для болле читабельной печати/вывода json'а.

        :param s: входящая строка
        """
        try:
            print(json.dumps(s, ensure_ascii=False, indent=4))
        except ValueError:
            print(s)
