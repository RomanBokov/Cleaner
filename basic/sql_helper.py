"""
Модуль содержит класс SqlHelper, который содержит методы для работы с БД.

:author: Andrei Ursaki.
"""
import re

import pyodbc

from basic import db_config


class SqlHelper(object):
    def __init__(self, telemetry_system_id=None, sensors_conn=db_config.db_sensors_conn_122,
                 layer_obj_conn=db_config.db_layer_obj_conn_122, omnidata_conn=db_config.db_coordcom_conn):
        """
        Конструктор класса.

        :param telemetry_system_id: идентификатор телеметрической системы
        """
        self.telemetry_system_id = telemetry_system_id
        self.sensors_conn = sensors_conn
        self.layer_obj_conn = layer_obj_conn
        self.omnidata_conn = omnidata_conn

    @staticmethod
    def execute_query(conn_str, query, is_commit_needed=False):
        """
        Метод отвечающий за выполнение запроса к БД.

        :param conn_str: строка с параметрами подключения к БД
        :param query: запрос
        :param is_commit_needed: параметр необходимый в случае типа запроса "insert" или "delete", по умолчанию False
        :return:
        """
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        if is_commit_needed:
            conn.commit()
        return cursor

    def get_all_sensor_codes(self):
        """
        Метод для получения всех идентификаторов объектов.

        :return: список идентификаторов объектов
        """
        sensor_code_list = []
        query = f"""SELECT sensor_code      
                    FROM [SphaeraTelemetryReference02].[dbo].[t_sensor]
                    where telemetry_system_id = {self.telemetry_system_id} and sensor_code != 'SensorCodeDefault' 
                    and removed_dt is NULL """
        # print(query)
        cursor = self.execute_query(self.sensors_conn, query)
        for row in cursor.fetchall():
            sensor_code_list.append(row[0])
        return sensor_code_list

    def get_all_sensors_with_open_card(self):
        """
        Метод для получения дентификаторов объектов, для которых есть открытая карточка.

        :return: список идентификаторов объектов
        """
        sensor_code_list = []
        query = f"""select ExternalSystemReference from [cse_CaseExternalSystemReference_tab]
                    where ExternalSystemReference like '{self.telemetry_system_id}-%'"""
        cursor = self.execute_query(self.omnidata_conn, query)
        for row in cursor.fetchall():
            sensor_code = re.findall(r"<[^{}]+>", row[0])[0][1:-1]
            sensor_code_list.append(sensor_code)
        return list(set(sensor_code_list))

    def get_sensor_attributes(self, sensor_code):
        """
        Метод для получения атрибутов КО.

        :param sensor_code: идентификатор объекта
        :return: словарь атрибутов КО
        """
        sensor_attribute_dict = {}
        query = f"""SELECT TOP 1000  a.Code,av.Value,
                    t_s.layerobject_caption as caption,t_s.address as t_address, t_s.location_lat, t_s.location_long,
                    t_s.call_center_id,t_s.case_type_area,e.id,t_s.municipality_name
                    FROM [LayerObjectRostov].[dbo].[Element] e
                    join ElementType et on e.ElementTypeId = et.Id 
                    join ElementTypeAttribute eta on eta.ElementTypeId = et.id
                    join Attribute a on a.Id = eta.AttributeId 
                    join AttributeValue av on e.id = av.ElementId  and av.AttributeId = a.Id
                    join [SphaeraTelemetryReference02].[dbo].[t_sensor] t_s on t_s.layerobject_id = e.Id
                    where t_s.sensor_code = '{sensor_code}' and t_s.telemetry_system_id = {self.telemetry_system_id}"""
        cursor = self.execute_query(self.layer_obj_conn, query)
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        if data:
            for i in list(range(2, len(columns))):
                # с 3 столбца начинаются атрибуты, которых нет в КО
                if data[0][i]:
                    sensor_attribute_dict[columns[i]] = data[0][i]
                    # добавляем атрибуты из БД справочников в словарь
            for row in data:
                if row[1]:
                    sensor_attribute_dict[row[0]] = row[1]
                    # добавляем атрибуты КО в словарь
            sensor_attribute_dict['sensor_code'] = sensor_code
        return sensor_attribute_dict

    def get_card_data(self, sensor_code):
        """
        Метод для получения информации из карточки.

        :param sensor_code: идентификатор объекта
        :return: словарь атрибутов карточки
        """
        query = f"""SELECT TOP 1000 cf.MunicipalityName, ces.CallCenterId, ces.CaseFolderId, ces.CaseId, cf.CaseTypeId, 
                    ces.ExternalSystemName, ces.ExternalSystemReference, cf.Created as CardCreated,
                    cf.XCoordinate,cf.YCoordinate,cf.CaseIndex1,cf.CaseIndex2,cf.CaseIndex3,cf.CaseIndex1Name,cf.CaseIndex2Name,cf.CaseIndex3Name,
                    cf.CaseIndexComment,cf.RouteDirections
                    FROM [OmniData].[dbo].[cse_Case_tab] cf
                    join [OmniData].[dbo].[cse_CaseExternalSystemReference_tab] ces on cf.CallCenterId = ces.CallCenterId 
                    and cf.CaseFolderId = ces.CaseFolderId                
                    join [OmniData].[dbo].[geo_Municipality_tab] mun on mun.CallCenterId = ces.CallCenterId
                    where ces.ExternalSystemReference like '{self.telemetry_system_id}-<{sensor_code}>%'
                    order by cf.Created desc"""
        # print(query)
        cursor = self.execute_query(self.omnidata_conn, query)
        columns = [column[0] for column in cursor.description]
        results = {}
        for row in cursor.fetchall():
            card_dict = dict(zip(columns, row))
            card_dict['Notices'] = self.get_card_notices(card_dict.get('CallCenterId'), card_dict.get('CaseFolderId'))
            results.update(card_dict)

        return results

    def get_card_notices(self, call_center, case_folder_id):
        query = f"""select OrderNo, CaseNoteTypeId, ImportanceId, Created, Creator, Canceled, CaseId, NoteText 
                    from [OmniData].[dbo].[cse_Note_tab]
                    where CallCenterId = {call_center} and CaseFolderId = {case_folder_id}
                    order by OrderNo"""
        cursor = self.execute_query(self.omnidata_conn, query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results

    def delete_notify(self, param_list):
        """
        Метод для удаления напоминаний для карточек.

        :param param_list: список с информацией о карточке
        """
        call_center_id = param_list[0]
        case_folder_id = param_list[1]
        case_id = param_list[2]
        case_type_id = param_list[3]
        query = f"""delete from [OmniData].[dbo].[cse_TimeActivatedCase_tab]
                    where CallCenterId={call_center_id} and CaseFolderId={case_folder_id} and CaseId = {case_id}
                    and CaseTypeId={case_type_id}"""
        cursor = self.execute_query(self.omnidata_conn, query, is_commit_needed=True)

    def change_index_to_test(self, param_list):
        """
        Метод для изменения индексов 1 уровня на 64.

        :param param_list: список с информацией о карточке
        """
        call_center_id = param_list[0]
        case_folder_id = param_list[1]
        case_id = param_list[2]
        case_type_id = param_list[3]
        query = f"""update [OmniData].[dbo].[cse_Case_tab] set CaseIndex1=64,CaseIndex2=NULL,CaseIndex3=NULL
                    ,CaseIndex1Name='Тестирование Системы',CaseIndex2Name=NULL,CaseIndex3Name=NULL
                    where CallCenterId={call_center_id} and CaseFolderId={case_folder_id} and CaseId = {case_id}
                    and CaseTypeId={case_type_id}"""
        print(query)
        cursor = self.execute_query(self.omnidata_conn, query, is_commit_needed=True)

    def get_card_for_close(self):
        # query = """select CallCenterId,CaseFolderId,CaseId from cse_Case_tab
        #         where CallCenterId = 160 and Created > '2020-09-28'
        #         order by created desc"""
        query = f""" SELECT TOP 1000 ces.CallCenterId, cf.CaseFolderId, cf.CaseId,cf.CaseTypeId
                FROM [OmniData].[dbo].[cse_Case_tab] cf
                join [OmniData].[dbo].[cse_CaseExternalSystemReference_tab] ces on cf.CallCenterId = ces.CallCenterId 
                and cf.CaseFolderId = ces.CaseFolderId
                where ces.ExternalSystemReference like '{self.telemetry_system_id}-%'
                order by cf.Created desc
                """
        cursor = self.execute_query(self.omnidata_conn, query)
        return cursor.fetchall()

    def is_notification_in_card(self, sensor_code):
        query = f"""select * from [OmniData].[dbo].[cse_CaseExternalSystemReference_tab] ces
                    join [OmniData].[dbo].[cse_TimeActivatedCase_tab] tac on ces.CallCenterId = tac.CallCenterId 
                    and ces.CaseFolderId = tac.CaseFolderId and ces.CaseFolderId= tac.CaseFolderId
                    where ces.ExternalSystemReference like '{self.telemetry_system_id}-<{sensor_code}>%'"""
        cursor = self.execute_query(self.omnidata_conn, query)
        if cursor.fetchall():
            return True
        else:
            return False
