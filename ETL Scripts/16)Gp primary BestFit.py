import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

#
source_pat = 'SELECT patient_id,contact_type_id,contact_id FROM patient_contact_details WHERE contact_id IS NOT NULL AND patient_id IS NOT NULL order by patient_id,contact_type_id'
source_pat_df = pd.read_sql(source_pat,target_myconnection)

# #
# final = """
# select *,count(rank)over(partition by patient_id order by contact_type_id) as rank_sum,LAG(contact_type_id) over(partition by patient_id) as prev_value
# from(
# select patient_id,contact_id,contact_type_id,row_number() over(partition by patient_id order by contact_type_id desc) as rank
# from source_pat_df s
# ) ORERB 
# """
# final_df = ps.sqldf(final)

# final = """
# SELECT *,LAG(contact_type_id,1,0) OVER(PARTITION BY patient_id,contact_type_id) as prev_value
# FROM source_pat_df s
# """
# final_df = ps.sqldf(final)

final_df = source_pat_df

final_df_gp = final_df[final_df['contact_type_id']==3]
final_df_ref = final_df[final_df['contact_type_id']==11]
final_df_sol = final_df[final_df['contact_type_id']==4]

final_df_gp['row_number'] = final_df_gp.groupby('patient_id').cumcount()+1
final_df_gp['primary'] = final_df_gp.apply(lambda x: 0 if x['row_number'] != 1 else 1,axis=1)

final_df_ref['row_number'] = final_df_ref.groupby('patient_id').cumcount()+1
final_df_ref['primary'] = final_df_ref.apply(lambda x: 0 if x['row_number'] != 1 else 1,axis=1)

final_df_sol['row_number'] = final_df_sol.groupby('patient_id').cumcount()+1
final_df_sol['primary'] = final_df_sol.apply(lambda x: 0 if x['row_number'] != 1 else 1,axis=1)


final_df1 = pd.concat([final_df_gp,final_df_ref,final_df_sol],ignore_index=True)

# def updated_rank(row):
#     if row['prev_value'] == 0 and row['contact_type_id'] in (3,9,4):
#         return 1
#     else:
#         return 0

# final_df['primary_rank'] = final_df.apply(lambda row:updated_rank(row),axis=1)

# def updated_primary(row):
#     if row['contact_type_id'] == 3 or row['contact_type_id'] == 9:
#         return 1
#     elif row['contact_type_id'] != 3 and row['rank_sum'] != 1:
#         return 0
#     elif row['contact_type_id'] == 3 and row['prev_value'] == 3:
#         return 0
#     else:
#         return 1

# final_df['updated_rank'] = final_df.apply(lambda row: updated_primary(row),axis=1)

for index,row in final_df1.iterrows():
    update_primary = """
    UPDATE patient_contact_details p
    SET p.`primary` = {}
    WHERE p.contact_id ={} and p.patient_id = {} and p.contact_type_id = {} and p.created_at = p.updated_at
    """.format(row['primary'],row['contact_id'],row['patient_id'],row['contact_type_id'])
    target_cursor.execute(update_primary)
    

target_myconnection.commit()
target_myconnection.close()
print('finished')

