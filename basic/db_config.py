"""
Модуль содержит параметры подключения к базам данных.

:author: Andrei Ursaki.
"""
driver = 'DRIVER={SQL Server}'
server_layerobj_42 = 'SERVER=10.100.42.5'
server_omnidata = 'SERVER=10.100.42.2'
server_layerobj_122 = 'SERVER=10.100.122.5'
port = 'PORT=1433'
db_layer_obj = 'DATABASE=LayerObjectRostov'
db_sensors = 'DATABASE=SphaeraTelemetryReference02'
db_omnidata = 'DATABASE=OmniData'
user = 'UID=sa'
pw = 'PWD=Sph@era92'

db_layer_obj_conn_42 = ';'.join([driver, server_layerobj_42, port, db_layer_obj, user, pw])
db_sensors_conn_42 = ';'.join([driver, server_layerobj_42, port, db_sensors, user, pw])

db_layer_obj_conn_122 = ';'.join([driver, server_layerobj_122, port, db_layer_obj, user, pw])
db_sensors_conn_122 = ';'.join([driver, server_layerobj_122, port, db_sensors, user, pw])

db_coordcom_conn = ';'.join([driver, server_omnidata, port, db_omnidata, user, pw])
