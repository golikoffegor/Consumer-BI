import zlib
import base64
import codecs
from phpserialize import *
import queries as q
from ets.ets_mysql_lib import MysqlConnection as mc
import pymongo
import hashlib
from Crypto.Cipher import AES
import simplejson as json
import pika
import ast
from settings import client, db, credentials, params

# Загрузка документа в монгу
def insert_document(collection, data):
    return collection.insert(data, check_keys=False)

# Готовим данные для запроса
def add_quotes(a):
    return f"'{str(a)}'"

# Достаем данные из БД
def get_data_from_db_firm(cnx, query, as_dict=False, **kwargs):
    connect = mc(connection=cnx)
    with connect.open() as c:
        result = c.execute_query(query.format(**kwargs), dicted=as_dict)
    return result

# Способ дешифровки данных для БД Fabrikant текстовых данных
def decode_data_txt(data):
    #decode = data.encode("ascii")
    decode = base64.b64decode(data)
    decode = zlib.decompress(decode)
    decode = unserialize(decode, object_hook=phpobject)
    decode = loads(dumps(decode), object_hook=phpobject, decode_strings=True)
    decode = convert_member_dict(decode)
    decode = json.dumps(decode, iterable_as_array=True)
    decode = json.loads(decode)
    return decode

def add_update_data(data_from_db):
    entity_ids = {"ClarificationProtocol": "protocol_id",
                  "ClarificationResponseFortum": "request_id",
                  "ContractInfo": "protocol_id",
                  "CriteriasProposal": "procedure_id",
                  "CriteriasRepeatedTenderProposal": "procedure_id",
                  "EisClarificationRequest": "request_id",
                  "ClarificationRequest": "request_id",
                  "ExplanationAnswer": "answer_id",
                  "ExplanationRequest": "request_id",
                  "ListOfActualOffers": "procedure_id",
                  "Lot": "lot_id",
                  "NrCommon": "procedure_id",
                  "NrProposal": "procedure_id",
                  "Proposal": "proposal_id",
                  "ProposalFortum": "procedure_id",
                  "ProposalList": "procedure_id",
                  "ProposalListFortum": "procedure_id",
                  "ProposalStage": "stage_index",
                  "Protocol": "protocol_id",
                  "Protocols": "protocol_id",
                  "RepeatedTenderProposal": "repeated_tender_id",
                  "RepeatedTenderProposalList": "procedure_id"}

    data_from_db['data'] = decode_data_txt(data_from_db['data'])
    name_id = entity_ids[data_from_db['entity']]
    entity_keys = json.loads(json.dumps(data_from_db['data']['event']['keys'][name_id]), parse_int = str)
    entity_keys_dict = {name_id: entity_keys}
    fabrikant_data_collection = db[data_from_db['entity']]
    data_from_db.update(entity_keys_dict)
    finding_info = fabrikant_data_collection.find_one(entity_keys_dict, {'entity': data_from_db['entity']})
    if finding_info:
        result_update = fabrikant_data_collection.update_one({"_id": finding_info['_id']},
                                                             {"$set": data_from_db})
        if result_update.matched_count > 0:
            print("Success update  " + str(entity_keys_dict) + ' ' + str(data_from_db['entity']))
            pass
        else:
            print("Failure update " + str(entity_keys_dict) + str(data_from_db['entity']))
            pass
    else:
        insert_document(fabrikant_data_collection, data_from_db)
        print("Success insert " + str(entity_keys_dict) + ' ' + str(data_from_db['entity']))

def on_message(channel, method_frame, header_frame, body):
    data_from_queue = ast.literal_eval(body.decode('utf-8'))
    all_data_from_bi_pe_check = get_data_from_db_firm(mc.MS_BI_PE, q.get_data_from_bi_pe_check, as_dict=True,
                                                      event_gid_str=add_quotes(data_from_queue["event_gid"]))
    if all_data_from_bi_pe_check:
        for data_from_db in all_data_from_bi_pe_check:
            add_update_data(data_from_db)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.basic_consume('export_event_complete_bi', on_message, auto_ack=False)

channel.start_consuming()














































'''
# Информация по локальному подключению монги
client = pymongo.MongoClient('mongodb://10.31.86.41:27017')
db = client['qlik-SilMash']

# Информация по прослушиваемой очереди
credentials = pika.PlainCredentials('services', 'Cdte8cVr7RhHQWdq')
params = pika.ConnectionParameters('10.31.82.122', 5672, 'procedure_version_2', credentials)

# Загрузка документа в монгу
def insert_document(collection, data):
    return collection.insert(data, check_keys=False)

# Готовим данные для запроса
def add_quotes(a):
    return f"'{str(a)}'"

# Достаем данные из БД
def get_data_from_db_firm(cnx, query, as_dict=False, **kwargs):
    connect = mc(connection=cnx)
    with connect.open() as c:
        result = c.execute_query(query.format(**kwargs), dicted=as_dict)
    return result

# Способ дешифровки данных для БД Fabrikant текстовых данных
def decode_data_txt(data):
    base64_bytes = data.encode("ascii")
    decode = base64.b64decode(base64_bytes)
    decode = zlib.decompress(decode)
    decode = unserialize(decode, object_hook=phpobject)
    decode = loads(dumps(decode), object_hook=phpobject, decode_strings=True)
    decode = convert_member_dict(decode)
    decode = json.dumps(decode, iterable_as_array=True)
    decode = json.loads(decode)
    return decode

def on_message(channel, method_frame, header_frame, body):
    data_from_queue = ast.literal_eval(body.decode('utf-8'))
    all_data_from_bi_pe_check = get_data_from_db_firm(mc.MS_BI_PE, q.get_data_from_bi_pe_check, as_dict=True,
                                                      event_gid_str=add_quotes(data_from_queue["event_gid"]))
    if all_data_from_bi_pe_check:
        for i in all_data_from_bi_pe_check:
            i['data'] = decode_data_txt(i['data'])
            fabrikant_data_collection = db[i['entity']]
            finding_info = fabrikant_data_collection.find_one({'event_gid': i['event_gid'], 'entity': i['entity']})
            if finding_info:
                result_update = fabrikant_data_collection.update_one({"_id": finding_info['_id']},
                                                                     {"$set": {'data': i['data']}})
                if result_update.matched_count > 0:
                    print("Success update event_gid " + str(i['event_gid']) + ' ' + str(i['entity']))
                    pass
                else:
                    print("Failure update")
                    pass
            else:
                insert_document(fabrikant_data_collection, i)
                print("Success insert event_gid " + str(i['event_gid']) + ' ' + str(i['entity']))
    #channel.basic_ack(delivery_tag=method_frame.delivery_tag)

credentials = pika.PlainCredentials('services', 'Cdte8cVr7RhHQWdq')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.31.82.122', 5672, 'procedure_version_2', credentials))
channel = connection.channel()
channel.basic_consume('export_event_complete_bi', on_message, auto_ack=False)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()



'''
