import pymongo
import pandas as pd
import numpy as np
from ets.ets_excel_creator import Excel
import os
from okei_codes import okei_dict, procedure_type_system, purchase_section_system, purchase_category_system
import uuid

client = pymongo.MongoClient('mongodb://e.golikov:ianAScunb2jnASdhaS@10.30.46.185:27017')
db = client['qlik-SilMash']

def insert_document(collection, data):
    return collection.insert(data, check_keys=False)

def lot_to_dict_func(entity):
    if str(entity['data']['entity']['procedure']['system']['status']) == '2':
        procedure_id = str(entity['data']['entity']['procedure']['system']['procedure_id'])
        lot_system_status = str(entity['data']['entity']['procedure']['system']['status'])
        lot_id = str(entity['data']['entity']['lot']['system']['lot_id'])
        sequence_number = str(entity['data']['entity']['lot']['system']['sequence_number'])
        state_id = str(entity['data']['entity']['lot']['system']['state_id'])
        procedure_organizer = str(entity['data']['entity']['procedure']['fields']['procedure_organizer']['inner_elements']['firmNameFull']['value'])
        contactUser = str(entity['data']['entity']['procedure']['fields']['procedure_organizer']['inner_elements']['contactUser']['value']['user_name'])
        purchase_section = str(entity['data']['entity']['procedure']['fields']['purchase_section']['value'])
        procedure_type = str(entity['data']['entity']['procedure']['fields']['type']['value'])
        if 'lot_positions' in entity['data']['entity']['lot']['fields'].keys():
             okpd = str(entity['data']['entity']['lot']['fields']['lot_positions']['value']['0']['subject_contract']['0'])
             quantity = str(entity['data']['entity']['lot']['fields']['lot_positions']['value']['0']['quantity_for_code']['quantity'])
             quantity_for_code = str(entity['data']['entity']['lot']['fields']['lot_positions']['value']['0']['quantity_for_code']['code'])
             description = str(entity['data']['entity']['lot']['fields']['lot_currency']['value']['description'])
        if 'lot_plan_position_rows' in entity['data']['entity']['lot']['fields'].keys():
             okpd = str(entity['data']['entity']['lot']['fields']['lot_plan_position_rows']['value']['0']['okpd2_code']['0'])
             quantity = str(entity['data']['entity']['lot']['fields']['lot_plan_position_rows']['value']['0']['okei_code_typed']['quantity'])
             quantity_for_code = str(entity['data']['entity']['lot']['fields']['lot_plan_position_rows']['value']['0']['okei_code_typed']['code'])
             description = str(entity['data']['entity']['lot']['fields']['lot_price']['settings']['currency']['description'])
        procedure_customer = str(entity['data']['entity']['procedure']['fields']['procedure_customer']['value']['firmName']['firm_name'])
        if not procedure_customer:
             procedure_customer = "Является организатором"
        split_okpd = okpd.split(" ", 1)
        lot_name = str(entity['data']['entity']['lot']['system']['name'])
        lot_stage = str(entity['data']['entity']['lot']['system']['lot_stage'])
        purchase_category = str(entity['data']['entity']['procedure']['fields']['purchase_category']['value'])
        date_publication = str(entity['data']['entity']['procedure']['fields']['date_publication']['value']['current'])
        proposal_date_stop = str(entity['data']['entity']['lot']['fields']['proposal_date_stop']['value']['current'])
        summing_up_date = str(entity['data']['entity']['lot']['fields']['summing_up_date']['value']['current'])
        url = str(entity['data']['entity']['procedure']['url'])
        dict_to_df = {
             "Номер процедуры":procedure_id,
             "Системный статус лота":lot_system_status,
             "ID лота":lot_id,
             "Позиция лота":sequence_number,
             "Статус процедуры":state_id,
             "Организатор":procedure_organizer,
             "Контактное лицо ":contactUser,
             "Секция":purchase_section_system[purchase_section],
             "Тип торгов":procedure_type_system[procedure_type],
             "ОКПД2":split_okpd[0],
             "ОКПД2 текст": split_okpd[1],
             "Количество":quantity,
             "Наименование единицы измерения":okei_dict[quantity_for_code][3],
             "Валюта":description,
             "Заказчик":procedure_customer,
             "Наименование лота":lot_name,
             "Количество этапов":lot_stage,
             "Секция на ЭТП":purchase_category_system[purchase_category],
             "Дата публикации":date_publication,
             "Дата окончания приёма заявок":proposal_date_stop,
             "Планируемая дата завершения процедуры":summing_up_date,
             "Ссылка":url
             }
    else:
        procedure_id = str(entity['data']['entity']['procedure']['system']['procedure_id'])
        lot_system_status = str(entity['data']['entity']['procedure']['system']['status'])
        lot_id = str(entity['data']['entity']['lot']['system']['lot_id'])
        sequence_number = str(entity['data']['entity']['lot']['system']['sequence_number'])
        state_id = str(entity['data']['entity']['lot']['system']['state_id'])
        procedure_organizer = str(entity['data']['entity']['procedure']['fields']['procedure_organizer']['inner_elements']['firmNameFull']['value'])
        contactUser = str(entity['data']['entity']['procedure']['fields']['procedure_organizer']['inner_elements']['contactUser']['value']['user_name'])
        purchase_section = str(entity['data']['entity']['procedure']['fields']['purchase_section']['value'])
        procedure_type = str(entity['data']['entity']['procedure']['fields']['type']['value'])
        dict_to_df = {
             "Номер процедуры":procedure_id,
             "Системный статус лота":lot_system_status,
             "ID лота":lot_id,
             "Позиция лота":sequence_number,
             "Статус процедуры":state_id,
             "Организатор":procedure_organizer,
             "Контактное лицо ":contactUser,
             "Секция":purchase_section_system[purchase_section],
             "Тип торгов":procedure_type_system[procedure_type],
             }

    return dict_to_df

def prop_to_dict_func(entity):
    lot_id_prop = str(entity['data']['event']['keys']['lot_id'])
    if 'proposal_info' in entity['data']['entity']['participant'].keys():
        prop_status_v = str(entity['data']['entity']['participant']['proposal_info']['value']['status'])
        name_firm_v = str(entity['data']['entity']['participant']['participant']['value']['firm_info']['firm_name_short'])
        count_prop_v = str(entity['data']['entity']['lot_requirements']['lot_price']['value']['with_nds'])
        date_dispatch_v = str(entity['data']['entity']['participant']['proposal_info']['value']['date_dispatch'])
        date_giveup_v = str(entity['data']['entity']['participant']['proposal_info']['value']['date_giveup'])
        date_refuse_v = str(entity['data']['entity']['participant']['proposal_info']['value']['date_refuse'])
        date_withdraw_v = str(entity['data']['entity']['participant']['proposal_info']['value']['date_withdraw'])
        prop_dict = {
            "ID лота": lot_id_prop,
            "Статус предложения": prop_status_v,
            "Дата отправки": date_dispatch_v,
            "Дата отмены": date_giveup_v,
            "Дата отклонения": date_refuse_v,
            "Дата отзыва": date_withdraw_v,
            "Поставщик по заявке": name_firm_v,
            "Цена по заявке": count_prop_v
        }

    else:
        prop_dict = {
            "ID лота": lot_id_prop,
        }

    return prop_dict

def prot_to_dict_func(entity):

    #final and determination_and_summing_up
    if entity['data']['entity']['protocol']['system']['type'] == 'final' or entity['data']['entity']['protocol']['system']['type'] == 'determination_and_summing_up':
        protocol_dict = {}
        if 'winners' in entity['data']['entity']['protocol']['fields'].keys():
            num_pos = 1
            for participant in entity['data']['entity']['protocol']['fields']['winners']['value'].values():
                protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
                protocol_publication = "-"
                if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                    protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
                lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
                name_firm_k = "Победитель №" + str(num_pos)
                name_firm_v = str(participant['winners_3_participant_participant']['firm_info']['firm_name_short'])
                count_prot_k = "Цена победителя №" + str(num_pos)
                count_prot_v = str(participant['winners_3_lot_requirements_lot_price']['with_nds'])
                protocol_dict.update({"ID лота": lot_id_prot,
                                     "Тип протокола": protocol_types,
                                     "Дата публикации протокола": protocol_publication,
                                     name_firm_k: name_firm_v,
                                     count_prot_k: count_prot_v,
                                     "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                     "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                     "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                     "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})
                num_pos += 1

        if 'accepteds' in entity['data']['entity']['protocol']['fields'].keys():
            num_pos = 1
            for participant in entity['data']['entity']['protocol']['fields']['accepteds']['value'].values():
                protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
                protocol_publication = "-"
                if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                    protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
                lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
                name_firm_k = "Допущенный №" + str(num_pos)
                name_firm_v = str(participant['accepteds_3_participant_participant']['firm_info']['firm_name_short'])
                count_prot_k = "Цена допущенного №" + str(num_pos)
                count_prot_v = str(participant['accepteds_3_lot_requirements_lot_price']['with_nds'])
                protocol_dict.update({"ID лота": lot_id_prot,
                                      "Тип протокола": protocol_types,
                                      "Дата публикации протокола": protocol_publication,
                                      name_firm_k: name_firm_v,
                                      count_prot_k: count_prot_v,
                                      "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                      "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                      "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                      "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})
                num_pos += 1

        if 'recognize_procedure_failed' in entity['data']['entity']['protocol']['fields'].keys():
            lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
            protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
            protocol_publication = "-"
            if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
            protocol_dict.update({"ID лота": lot_id_prot,
                                  "Тип протокола": protocol_types,
                                  "Дата публикации протокола": protocol_publication,
                                  "Процедура несостоялась": str(entity['data']['entity']['protocol']['fields']['recognize_procedure_failed']['value']),
                                  "Заключить договор с единственным участником": str(entity['data']['entity']['protocol']['fields']['enter_contract_one_participant']['value']),
                                  "Описание причины признания закупки несостоявшейся": str(entity['data']['entity']['protocol']['fields']['cause_recognize_procedure_failed']['value']),
                                  "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                  "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                  "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                  "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})
        if 'decision' in entity['data']['entity']['protocol']['fields'].keys():
            lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
            protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
            protocol_publication = "-"
            if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
            protocol_dict.update({"ID лота": lot_id_prot,
                                  "Тип протокола": protocol_types,
                                  "Дата публикации протокола": protocol_publication,
                                  "Решение": str(entity['data']['entity']['protocol']['fields']['decision']['value']),
                                  "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                  "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                  "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                  "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})

    #determination
    if entity['data']['entity']['protocol']['system']['type'] == 'determination':
        protocol_dict = {}
        num_pos = 1
        for participant in entity['data']['entity']['protocol']['fields']['determination']['value'].values():
            protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
            protocol_publication = "-"
            if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
            lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
            name_firm_k = "Поставщик по протоколу №" + str(num_pos)
            name_firm_v = str(participant['determination_3_participant_participant']['firm_info']['firm_name_short'])
            count_prot_k = "Цена по протоколу №" + str(num_pos)
            count_prot_v = str(participant['determination_3_lot_requirements_lot_price']['with_nds'])
            protocol_dict.update({"ID лота": lot_id_prot,
                                 "Тип протокола": protocol_types,
                                 "Дата публикации протокола": protocol_publication,
                                 name_firm_k: name_firm_v,
                                 count_prot_k: count_prot_v,
                                 "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                 "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                 "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                 "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})
            num_pos += 1
        if protocol_dict == {}:
            protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
            protocol_publication = "-"
            if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
            lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
            protocol_dict.update({"ID лота": lot_id_prot,
                                 "Тип протокола": protocol_types,
                                 "Дата публикации протокола": protocol_publication,
                                 "Итог по рассмотрению": "Нет предложений участников",
                                 "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                 "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                 "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                 "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})

    #repeated_tender and repeated_tender_auction_type
    if entity['data']['entity']['protocol']['system']['type'] == 'repeated_tender' or entity['data']['entity']['protocol']['system']['type'] == 'repeated_tender_auction_type':
        protocol_dict = {}
        num_pos = 1
        for participant in entity['data']['entity']['protocol']['fields']['protocol_suggestions_participants']['value'].values():
            protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
            protocol_publication = "-"
            if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
            lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
            name_firm_k = "Поставщик по протоколу №" + str(num_pos)
            name_firm_v = str(participant['protocol_suggestions_participants_600_participant_participant']['firm_info']['firm_name_short'])
            count_prot_k = "Цена по протоколу №" + str(num_pos)
            count_prot_v = '-'
            if 'protocol_suggestions_participants_600_lot_requirements_lot_price' in participant.keys():
                count_prot_v = str(participant['protocol_suggestions_participants_600_lot_requirements_lot_price']['with_nds'])
            protocol_dict.update({"ID лота": lot_id_prot,
                                 "Тип протокола": protocol_types,
                                 "Дата публикации протокола": protocol_publication,
                                 name_firm_k: name_firm_v,
                                 count_prot_k: count_prot_v,
                                 "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                 "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                 "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                 "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})
            num_pos += 1
        if protocol_dict == {}:
            protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
            protocol_publication = "-"
            if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
                protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
            lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
            protocol_dict.update({"ID лота": lot_id_prot,
                                 "Тип протокола": protocol_types,
                                 "Дата публикации протокола": protocol_publication,
                                 "Итог по переторжке": "Нет предложений участников",
                                 "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                                 "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                                 "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                                 "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})

    #giveup
    if entity['data']['entity']['protocol']['system']['type'] == 'giveup':
        protocol_dict = {}
        protocol_types = str(entity['data']['entity']['protocol']['system']['type'])
        protocol_publication = "-"
        if isinstance(entity['data']['entity']['protocol']['fields']['date_publication']['value'], dict):
            protocol_publication = str(entity['data']['entity']['protocol']['fields']['date_publication']['value']['current'])
        lot_id_prot = str(entity['data']['entity']['protocol']['system']['lot_id'])
        protocol_dict.update({"ID лота": lot_id_prot,
                             "Тип протокола": protocol_types,
                             "Дата публикации протокола": protocol_publication,
                             "is_draft": str(entity['data']['entity']['protocol']['system']['is_draft']),
                             "is_actual": str(entity['data']['entity']['protocol']['system']['is_actual']),
                             "sign_id": str(entity['data']['entity']['protocol']['system']['sign_id']),
                             "is_final": str(entity['data']['entity']['protocol']['system']['is_final'])})

    if protocol_dict != {}:
        return protocol_dict


def data_to_qlik(entity, entity_type, collection_id):
    search_dict_lot = {'_id': collection_id}
    collection_name = str(entity_type) + "_to_qlik"
    collection_dict = db[collection_name]
    if entity_type == "Lot":
        info = lot_to_dict_func(entity)
    if entity_type == "Proposal":
        info = prop_to_dict_func(entity)
    if entity_type == "Protocol":
        info = prot_to_dict_func(entity)

    finding_info = collection_dict.find_one(search_dict_lot, {'entity': collection_name})
    if finding_info:
        result_update_lot = collection_dict.update_one({"_id": finding_info['_id']},
                                                       {"$set": info})
        if result_update_lot.matched_count > 0:
            print("Success update " + str(entity_type) + " to qlik " + str(finding_info['_id']))
            pass
        else:
            print("Failure update " + str(entity_type) + " to qlik " + str(finding_info['_id']))
            pass
    else:
        info.update(search_dict_lot)
        insert_document(collection_dict, info)
        print("Success insert " + str(entity_type) + " to qlik " + str(info['_id']))







