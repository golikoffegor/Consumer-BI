get_data_from_bi_pe_check_1 = '''SELECT *
FROM procedure_export_1.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_2.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_3.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_4.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_5.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_6.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_7.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_8.bi
WHERE bi.event_gid = ({event_gid_str})
'''

get_data_from_bi_pe_check_2 = '''SELECT *
FROM procedure_export_9.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_10.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_11.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_12.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_13.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_14.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_15.bi
WHERE bi.event_gid = ({event_gid_str})
UNION
SELECT *
FROM procedure_export_16.bi
WHERE bi.event_gid = ({event_gid_str})
'''