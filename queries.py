get_data_from_bi_pe = '''SELECT *
FROM procedure_export_1.bi bi 
UNION
SELECT *
FROM procedure_export_2.bi bi 
'''

get_data_from_bi_pe_check = '''SELECT *
FROM procedure_export_1.bi bi 
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_2.bi bi 
WHERE bi.event_gid = ({event_gid_str})
'''
