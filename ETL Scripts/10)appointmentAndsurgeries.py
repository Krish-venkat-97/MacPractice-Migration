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
#Appointments 
src_appointments = """
SELECT
    a.appointment_id        AS id,
    a.patient_id            AS scr_patient_id,
    a.resource_id           AS service_location_id,
    a.appointment_type_id   AS appointment_description_id,
    CAST(a.start AS DATE)   AS appointment_date,

    -- Start time as full datetime with fixed date
    TIMESTAMP(
        '1970-02-01',
        CAST(a.start AS TIME)
    ) AS start_time,

    -- End time as full datetime with fixed date
    TIMESTAMP(
        '1970-02-01',
        CAST(a.end AS TIME)
    ) AS end_time,

    a.notes                 AS appointment_notes,
    a.appointmentstatus_id  AS appointment_status_id,
    a.autoremind_status_id  AS reminders,
    a.created_user_id       AS created_user_id,
    a.updated_user_id       AS updated_user_id,
    a.created               AS created_at,
    a.last_updated          AS updated_at
FROM appointment a;
"""

src_appointments_df = pd.read_sql(src_appointments, src_connection)

#surgeries
src_surgery = """
SELECT
    a.appointment_id        AS id,
    a.patient_id            AS scr_patient_id,
    a.resource_id           AS service_location_id,
    CAST(a.start AS DATE)   AS admission_date,

    -- Start time as full datetime with fixed date
    TIMESTAMP(
        '1970-02-01',
        CAST(a.start AS TIME)
    ) AS admission_time,
	    
	CAST(a.start AS DATE)   AS surgery_date,
	
	-- Start time as full datetime with fixed date
    TIMESTAMP(
        '1970-02-01',
        CAST(a.start AS TIME)
    ) AS start_time,

    -- End time as full datetime with fixed date
    TIMESTAMP(
        '1970-02-01',
        CAST(a.end AS TIME)
    ) AS end_time,

    a.notes                 AS surgery_notes,
    a.appointmentstatus_id  AS surgery_status_id,
    a.created_user_id       AS created_user_id,
    a.updated_user_id       AS updated_user_id,
    a.created               AS created_at,
    a.last_updated          AS updated_at
FROM appointment a
WHERE appointment_type_id = 193
"""
src_surgery_df = pd.read_sql(src_surgery, src_connection)


#Target patients dataframe
# fetch target patients mapping
tgt_patients_df = pd.read_sql('SELECT id, macpractice_patient_id FROM patients WHERE macpractice_patient_id IS NOT NULL',tgt_connection)    

# Create a mapping from source patient id to target patient id
patient_map = dict(zip(tgt_patients_df['macpractice_patient_id'], tgt_patients_df['id']))

# Map scr_patient_id to target patient id
src_appointments_df['patient_id'] = src_appointments_df['scr_patient_id'].map(patient_map)
src_surgery_df['patient_id'] = src_surgery_df['scr_patient_id'].map(patient_map)


#Target appointment_descriptions dataframe
# fetch target appointment_descriptions mapping
appt_desc_mapping_df = pd.read_sql(
    "SELECT id AS target_id, MacPractice_ApptDesc_Id AS source_id FROM appointment_descriptions",
    tgt_connection
)

appt_desc_mapping_df = appt_desc_mapping_df.dropna(subset=['source_id'])
appt_desc_mapping_df['source_id'] = appt_desc_mapping_df['source_id'].astype(int)

# convert to dict: {source_id: target_id}
appt_desc_mapping = dict(zip(appt_desc_mapping_df['source_id'], appt_desc_mapping_df['target_id']))


#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS MacPractice_Appointment_Id VARCHAR(100) DEFAULT NULL;"
query_3 = "ALTER TABLE surgeries ADD COLUMN IF NOT EXISTS MacPractice_Surgery_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
target_cursor.execute(query_3)
tgt_connection.commit()

#id generation
appointment_max = 'SELECT MAX(id) FROM appointments'
appointment_max_df = pd.read_sql(appointment_max,tgt_connection)
max_id = 1 if appointment_max_df.iloc[0, 0] is None else appointment_max_df.iloc[0, 0] + 1
src_appointments_df.insert(0,'appointment_id',range(max_id,max_id+len(src_appointments_df)))

surgery_max = 'SELECT MAX(id) FROM surgeries'
surgery_max_df = pd.read_sql(surgery_max,tgt_connection)
max_id = 1 if surgery_max_df.iloc[0,0] is None else surgery_max_df.iloc[0, 0] + 1
src_surgery_df.insert(0,'surgery_id',range(max_id,max_id+len(src_surgery_df)))

#datetimeobj
datetimeobj = datetime.now()

#Fetching existing MacPractice Ids
tgt_appointments = 'SELECT MacPractice_Appointment_Id FROM appointments WHERE MacPractice_Appointment_Id IS NOT NULL'
tgt_appointments_df = pd.read_sql(tgt_appointments,tgt_connection)

tgt_appointments_df['MacPractice_Appointment_Id'] = tgt_appointments_df['MacPractice_Appointment_Id'].astype(int)


#Insert Loop
appointment_bar = tqdm(total=len(src_appointments_df),position=0,desc='Inserting Appointments')

#Appointment insert
for _,row in src_appointments_df.iterrows():
    appointment_bar.update(1)    
    if str(row['id']) not in tgt_appointments_df['MacPractice_Appointment_Id'].astype(str).values:
        try:
            appointment_insert = f"""
            INSERT INTO appointments (
                id,
                patient_id,
                doctor_id,
                service_location_id,
                appointment_description_id,
                appointment_date,
                start_time,
                end_time,
                appointment_notes,
                appointment_status_id,
                reminders,
                created_user_id,
                updated_user_id,
                created_at,
                updated_at,
                MacPractice_Appointment_Id
            )
            VALUES (
                {safe_value(row['appointment_id'])},
                {safe_value(row['patient_id'])},
                1,
                3,
                {safe_value(appt_desc_mapping.get(row['appointment_description_id']))},  -- mapped target id
                {safe_value(row['appointment_date'])},
                {safe_value(row['start_time'])},
                {safe_value(row['end_time'])},
                {safe_value(row['appointment_notes'])},
                {safe_value(row['appointment_status_id'])},
                {safe_value(row['reminders'])},
                1,
                1,
                '{datetimeobj}',
                '{datetimeobj}',
                {safe_value(row['id'])}
            )            """
            target_cursor.execute(appointment_insert)
        except Exception as e:
            tgt_connection.rollback() 
            logging.error(f'Error in Appointment insert: {e} \n Row: {appointment_insert}')
            print('Exception occured in the appointments module',flush=True)
            break
    else:
        continue

appointment_bar.close()
tgt_connection.commit()
print('finished appointments module',flush=True)

#Fetching existing MacPractice Ids
tgt_surgeries = 'SELECT MacPractice_Surgery_Id FROM surgeries WHERE MacPractice_Surgery_Id IS NOT NULL'
tgt_surgeries_df = pd.read_sql(tgt_surgeries,tgt_connection)

tgt_surgeries_df['MacPractice_Surgery_Id'] = tgt_surgeries_df['MacPractice_Surgery_Id'].astype(int)

#Surgery Insert Loop
surgery_bar = tqdm(total=len(src_surgery_df),position=0,desc='Inserting Surgeries')

for _,row in src_surgery_df.iterrows():
    surgery_bar.update(1)    
    if str(row['id']) not in tgt_surgeries_df['MacPractice_Surgery_Id'].astype(str).values:
        try:
            surgeries_insert = f"""
            INSERT INTO surgeries (
                id,
                patient_id,
                doctor_id,
                service_hospital_id,
                admission_date,
                admission_time,
                start_time,
                end_time,
                surgery_notes,
                created_user_id,
                updated_user_id,
                created_at,
                updated_at,
                surgery_date,
                surgery_status_id,
                MacPractice_Surgery_Id
            )
            VALUES (
                {safe_value(row['surgery_id'])},
                {safe_value(row['patient_id'])},
                1,
                3,
                {safe_value(row['admission_date'])},
                {safe_value(row['admission_time'])},
                {safe_value(row['start_time'])},
                {safe_value(row['end_time'])},
                {safe_value(row['surgery_notes'])},
                1,
                1,
                '{datetimeobj}',
                '{datetimeobj}',
                {safe_value(row['surgery_date'])},
                {safe_value(row['surgery_status_id'])},
                {safe_value(row['id'])}
            )
            """
            target_cursor.execute(surgeries_insert)
        except Exception as e:
            tgt_connection.rollback() 
            logging.error(f'Error in Surgery insert: {e} \n Row: {surgeries_insert}')
            print('Exception occured in the surgeries module',flush=True)
            break
    else:
        continue

tgt_connection.commit()
surgery_bar.close()
print('finished surgeries module',flush=True)

#Updating appointment and surgery statuses based on date
update_queries = [
    "UPDATE surgeries s SET s.surgery_date = s.admission_date WHERE s.surgery_date = '0000-00-00';",
    "UPDATE appointments a SET a.appointment_status_id = 1 WHERE a.appointment_date >= CURDATE() AND a.appointment_status_id != 3;",
    "UPDATE appointments a SET a.appointment_status_id = 4 WHERE a.appointment_date < CURDATE() AND a.appointment_status_id != 3;",
    "UPDATE surgeries s SET s.surgery_status_id = 1 WHERE s.surgery_date >= CURDATE() AND s.surgery_status_id != 3;",
    "UPDATE surgeries s SET s.surgery_status_id = 4 WHERE s.surgery_date < CURDATE() AND s.surgery_status_id != 3;"
]

for q in update_queries:
    target_cursor.execute(q)

tgt_connection.commit()
tgt_connection.close()
print('finished updating appointment and surgery statuses based on date',flush=True)
