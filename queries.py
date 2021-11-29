get_data_from_bi_pe_check_1 = '''SELECT *
FROM procedure_export_1.bi
WHERE bi.event_gid = ({event_gid_str})
'''
get_data_from_bi_pe_check_2 = '''SELECT *
FROM procedure_export_2.bi
WHERE bi.event_gid = ({event_gid_str})
'''