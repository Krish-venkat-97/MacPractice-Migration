import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

from datetime import datetime

src_connection = get_src_myconnection()
tgt_connection = get_tgt_myconnection()

target_cursor = tgt_connection.cursor()


#Source Select Query
#App Desc 
src_appointments_desc = """
SELECT
    appointment_type_id   AS id,
    appointment_desc      AS name, 
    flag_active           AS is_default
FROM appointment_type
"""

src_appointments_desc_df = pd.read_sql(src_appointments_desc, src_connection)

#procedures
src_procedures = """
SELECT
    fee_id AS proc_id,
    short_description AS name,
    code,
    unit_fee AS rate,
    min_per_unit_0_4 AS minutes,
    archived AS is_archive
FROM fee
"""

src_procedures_df = pd.read_sql(src_procedures, src_connection)

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE procedures ADD COLUMN IF NOT EXISTS MacPractice_Procedure_Id VARCHAR(100) DEFAULT NULL;"
query_3 = "ALTER TABLE appointment_descriptions ADD COLUMN IF NOT EXISTS MacPractice_ApptDesc_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
target_cursor.execute(query_3)
tgt_connection.commit()

#id generation
procedure_max = 'SELECT MAX(id) FROM procedures'
procedure_max_df = pd.read_sql(procedure_max,tgt_connection)
max_id = 1 if procedure_max_df.iloc[0, 0] is None else procedure_max_df.iloc[0, 0] + 1
src_procedures_df.insert(0,'procedure_id',range(max_id,max_id+len(src_procedures_df)))

appt_desc_max = 'SELECT MAX(id) FROM appointment_descriptions'
appt_desc_max_df = pd.read_sql(appt_desc_max,tgt_connection)
max_id = 1 if appt_desc_max_df.iloc[0, 0] is None else appt_desc_max_df.iloc[0, 0] + 1
src_appointments_desc_df.insert(0,'appt_desc_id',range(max_id,max_id+len(src_appointments_desc_df)))

appt_desc_proc_max = 'SELECT MAX(id) FROM appointment_description_procedures'
appt_desc_proc_max_df = pd.read_sql(appt_desc_proc_max,tgt_connection)
max_id = 1 if appt_desc_proc_max_df.iloc[0, 0] is None else appt_desc_proc_max_df.iloc[0, 0] + 1
src_appointments_desc_df.insert(0,'appt_desc_proc_id',range(max_id,max_id+len(src_appointments_desc_df)))


#Fetching existing MacPractice Ids --procedures
tgt_procedure = 'SELECT MacPractice_Procedure_Id FROM procedures WHERE MacPractice_Procedure_Id IS NOT NULL'
tgt_procedure_df = pd.read_sql(tgt_procedure,tgt_connection)

tgt_procedure_df['MacPractice_Procedure_Id'] = tgt_procedure_df['MacPractice_Procedure_Id'].astype(str)

#Fetching existing MacPractice Ids --appointment descriptions
tgt_appointment_desc = 'SELECT MacPractice_ApptDesc_Id FROM appointment_descriptions WHERE MacPractice_ApptDesc_Id IS NOT NULL'
tgt_appointment_desc_df = pd.read_sql(tgt_appointment_desc,tgt_connection)

tgt_appointment_desc_df['MacPractice_ApptDesc_Id'] = tgt_appointment_desc_df['MacPractice_ApptDesc_Id'].astype(str)

#datetimeobj
datetimeobj = datetime.now()

#Insert Loop
procedure_bar = tqdm(total=len(src_procedures_df),desc='Inserting procedures',position=0)
appt_desc_bar = tqdm(total=len(src_appointments_desc_df),desc='Inserting appointment descriptions',position=0)

#Appointment Description insert
for _,row in src_appointments_desc_df.iterrows():
    appt_desc_bar.update(1)
    if str(row['id']) not in tgt_appointment_desc_df['MacPractice_ApptDesc_Id'].values:
        try:
            appt_desc_insert = f"""
            INSERT INTO appointment_descriptions (
                    id,
                    name,
                    code,
                    short_name,
                    created_user_id,
                    updated_user_id,
                    created_at,
                    updated_at,
                    MacPractice_ApptDesc_Id,
                    is_default
            )
            VALUES(
                {safe_value(row['appt_desc_id'])},
                {safe_value(row['name'])},
                {safe_value(row['name'])},
                {safe_value(row['name'])},
                1,
                1,
                '{datetimeobj}',
                '{datetimeobj}',
                {safe_value(row['id'])},
                {safe_value(row['is_default'])}
                )
                """
            target_cursor.execute(appt_desc_insert)
            
            appt_desc_proc_insert = f"""
            INSERT INTO appointment_description_procedures (
                id,
                appointment_description_id,
                minutes,
                created_at,
                updated_at,
                doctor_id
            )
            VALUES (
                {safe_value(row['appt_desc_proc_id'])},
                {safe_value(row['appt_desc_id'])},
                15,
                '{datetimeobj}',
                '{datetimeobj}',
                1
                )
                """
            target_cursor.execute(appt_desc_proc_insert)

        except Exception as e:
            tgt_connection.rollback() 
            logging.exception(f"Error: {e} \n Row: {appt_desc_insert}")
            print('Exception occured in the appointment descriptions module',flush=True)
            break
        
    else:
        continue

tgt_connection.commit()
appt_desc_bar.close()

#Procedure insert
for index,row in src_procedures_df.iterrows():
    procedure_bar.update(1)
    if str(row['proc_id']) not in tgt_procedure_df['MacPractice_Procedure_Id'].values:
        try:
            procedure_insert = f"""
            INSERT INTO procedures (
                id,
                name,
                code,
                short_name,
                rate,
                minutes,
                created_user_id,
                updated_user_id,
                created_at,
                updated_at,
                MacPractice_Procedure_Id,
                is_archive
            )
            VALUES (
                {safe_value(row['procedure_id'])},
                {safe_value(row['name'])},
                {safe_value(row['code'])},
                {safe_value(row['code'])},
                {safe_value(row['rate'])},
                {safe_value(row['minutes'])},
                1,
                1,
                '{datetimeobj}',
                '{datetimeobj}',
                {safe_value(row['proc_id'])},
                {safe_value(row['is_archive'])}
                )
                """
            target_cursor.execute(procedure_insert)
        except Exception as e:
            tgt_connection.rollback() 
            logging.exception(f"Error: {e} \n Row: {procedure_insert}")
            print('Exception occured in the procedures module',flush=True)
            break
    else:
        continue

tgt_connection.commit()
procedure_bar.close()

#update appointment_desc_id in procedure
update_appt_desc = """
UPDATE procedures p
INNER JOIN appointment_descriptions a
ON a.MacPractice_ApptDesc_Id = p.MacPractice_Procedure_Id
SET p.appointment_description_id = a.id
"""

update_appt_desc_proc = """
UPDATE appointment_description_procedures ap
INNER JOIN procedures p
ON ap.appointment_description_id = p.appointment_description_id
SET ap.procedure_id = p.id
"""
try:
    target_cursor.execute(update_appt_desc)
    target_cursor.execute(update_appt_desc_proc)
    tgt_connection.commit()
except Exception as e:
    tgt_connection.rollback()
    raise


tgt_connection.close()
print('finished appointment descriptions module',flush=True)
