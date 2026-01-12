import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

src_patients_df = pd.read_sql("""
SELECT case when RIGHT(pe.salute,1) = '.' then REPLACE(pe.salute,'.','') ELSE pe.salute END AS title
,LTRIM(RTRIM(concat(pe.`first`,' ',IFNULL(pe.middle,'')))) AS first_name,pe.`last` AS sur_name,CONCAT(LTRIM(RTRIM(CONCAT(pe.`first`,' ',IFNULL(pe.middle,'')))),' ',pe.`last`) AS display_name,
pe.birthday AS dob,a.address1,a.address2,c.city AS address4,c.state AS county,c.zip AS postcode,
pe.phone1 AS home_phone,pe.phone2 AS work_phone,pe.phone3 AS mobile,pe.email,pins.insurance_id AS insurance_company_id,pins.plan_id AS insurance_plan_id,
case when pe.pop_sex = 1 then 1 ELSE 2 END AS gender,1 AS patient_type_id,pat.allergy_description,
pa.patient_notes,
case when pe.death_date IS NULL then 0 ELSE 1 END AS rip,pe.death_date AS deceased_date,
1 AS created_user_id,1 AS updated_user_id,0 AS deleted_user_id,CURRENT_TIMESTAMP() AS created_at,
CURRENT_TIMESTAMP() AS updated_at,pa.patient_id as Macpractice_patient_id,pe.person_id as Macpractice_person_id
FROM patient pa
INNER JOIN person pe
ON pa.person_id = pe.person_id
LEFT JOIN address a
ON pe.address_id = a.address_id
LEFT JOIN citystatezip c
ON a.citystatezip_id = c.citystatezip_id
LEFT JOIN person_ins_tie pins
ON pe.person_id = pins.person_id
LEFT JOIN patient_allergy_tie pat
ON pat.patient_id = pa.patient_id
""",source_myconnection)

tgt_titles_df = pd.read_sql("SELECT id as title_id, name as professional_title FROM titles",target_myconnection)
landing_patients_df = src_patients_df.merge(tgt_titles_df, left_on='title', right_on='professional_title', how='left')
landing_patients_df1 = landing_patients_df.drop(['title','professional_title'], axis=1)


#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS macpractice_patient_id INT(10) DEFAULT NULL;")
target_cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS macpractice_person_id INT(10) DEFAULT NULL;")
target_cursor.execute("ALTER TABLE medical_histories ADD COLUMN IF NOT EXISTS macpractice_id INT(10) DEFAULT NULL;")
target_cursor.execute("ALTER TABLE personal_histories ADD COLUMN IF NOT EXISTS macpractice_id INT(10) DEFAULT NULL;")
target_cursor.execute("ALTER TABLE episodes ADD COLUMN IF NOT EXISTS macpractice_id INT(10) DEFAULT NULL;")
target_myconnection.commit()

# target table to eliminate duplicates based on source identifier
existing_patients_df = pd.read_sql("SELECT macpractice_patient_id, macpractice_person_id FROM patients WHERE macpractice_patient_id IS NOT NULL",target_myconnection)
landing_patients_df1 = landing_patients_df1[~landing_patients_df1['Macpractice_patient_id'].isin(existing_patients_df['macpractice_patient_id'])]

# insert auto increment patient_ids
patient_max = pd.read_sql("SELECT IFNULL(MAX(id),0) AS max_id FROM patients",target_myconnection)
next_patient_id = int(patient_max['max_id'][0]) + 1
landing_patients_df1.insert(0,'patient_id',range(next_patient_id, next_patient_id + len(landing_patients_df1)))

personal_histories_max = pd.read_sql("SELECT IFNULL(MAX(id),0) AS max_id FROM personal_histories",target_myconnection)
next_personal_history_id = int(personal_histories_max['max_id'][0]) + 1
landing_patients_df1.insert(0,'personal_histories_id',range(next_personal_history_id, next_personal_history_id + len(landing_patients_df1)))

medical_histories_max = pd.read_sql("SELECT IFNULL(MAX(id),0) AS max_id FROM medical_histories",target_myconnection)
next_medical_history_id = int(medical_histories_max['max_id'][0]) + 1
landing_patients_df1.insert(0,'medical_histories_id',range(next_medical_history_id, next_medical_history_id + len(landing_patients_df1)))

epsiodes_max = pd.read_sql("SELECT IFNULL(MAX(id),0) AS max_id FROM episodes",target_myconnection)
next_episode_id = int(epsiodes_max['max_id'][0]) + 1
landing_patients_df1.insert(0,'episodes_id',range(next_episode_id, next_episode_id + len(landing_patients_df1)))

patients_bar = tqdm(total=len(landing_patients_df1), desc="Inserting Patients")

for index,row in landing_patients_df1.iterrows():
    patients_bar.update(1)
    try:
        patient_insert_query = f"""
        INSERT INTO patients (id,title_id,doctor_id,shared_doctor,first_name,surname,display_name,display_first_sur_name,dob,
        address1,address2,county,address4,postcode,home_phone,work_phone,mobile,emails,primary_insurance_company_id,
        primary_insurance_plan_id,
        gender,patient_type_id,allergies,notes,rip,deceased_date,
        created_user_id,updated_user_id,deleted_user_id,created_at,updated_at,
        macpractice_patient_id,macpractice_person_id) 
        VALUES({safe_value(row['patient_id'])},{safe_value(row['title_id'])},1,1,{safe_value(row['first_name'])},{safe_value(row['sur_name'])},
        {safe_value(row['display_name'])},{safe_value(row['display_name'])},{safe_value(row['dob'])},{safe_value(row['address1'])},
        {safe_value(row['address2'])},{safe_value(row['county'])},{safe_value(row['address4'])},
        {safe_value(row['postcode'])},{safe_value(row['home_phone'])},{safe_value(row['work_phone'])},
        {safe_value(row['mobile'])},{safe_value(row['email'])},{safe_value(row['insurance_company_id'])},{safe_value(row['insurance_plan_id'])},{safe_value(row['gender'])},
        {safe_value(row['patient_type_id'])},{safe_value(row['allergy_description'])},{safe_value(row['patient_notes'])},
        {safe_value(row['rip'])},{safe_value(row['deceased_date'])},{safe_value(row['created_user_id'])},
        {safe_value(row['updated_user_id'])},{safe_value(row['deleted_user_id'])},{safe_value(row['created_at'])},
        {safe_value(row['updated_at'])},{safe_value(row['Macpractice_patient_id'])},{safe_value(row['Macpractice_person_id'])});
        """
        target_cursor.execute(patient_insert_query)

        medical_histories_insert = f"""
        INSERT INTO medical_histories (id, patient_id, created_user_id, updated_user_id, created_at, updated_at,macpractice_id)
        VALUES({row['medical_histories_id']}, {row['patient_id']}, 1, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),1)
        """
        target_cursor.execute(medical_histories_insert)

        personal_histories_insert = f"""
        INSERT INTO personal_histories (id, patient_id, created_user_id, updated_user_id, created_at, updated_at,macpractice_id)
        VALUES({row['personal_histories_id']}, {row['patient_id']}, 1, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),1)
        """
        target_cursor.execute(personal_histories_insert)

        episode_insert = f"""
        INSERT INTO episodes (id, patient_id, created_user_id, updated_user_id, created_at, updated_at,macpractice_id)
        VALUES({row['episodes_id']}, {row['patient_id']}, 1, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),1)
        """
        target_cursor.execute(episode_insert)
        
    except Exception as e:
        print(f"Error inserting patient {row['patient_id']}: {e}")
        logging.error(f"Error inserting patient {row['patient_id']}: {e}")
        break

target_myconnection.commit()
patients_bar.close()
target_cursor.close()
target_myconnection.close()
print('Patient module completed.')