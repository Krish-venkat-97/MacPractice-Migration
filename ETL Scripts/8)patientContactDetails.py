import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

src_patient_referal = pd.read_sql("""
SELECT patient_referrals_tie_id, patient_id AS src_patient_id,referrals_id FROM patient_referrals_tie;
""",source_myconnection)

tgt_contacts_df = pd.read_sql("SELECT id AS contact_id,contact_type_id,macpractice_referral_id FROM contacts WHERE macpractice_referral_id IS NOT NULL",target_myconnection)
tgt_patients_df = pd.read_sql("SELECT id AS patient_id,macpractice_patient_id FROM patients WHERE macpractice_patient_id IS NOT NULL",target_myconnection)

landing_patient_referal = src_patient_referal.merge(tgt_contacts_df, left_on='referrals_id', right_on='macpractice_referral_id', how='inner')
landing_patient_referal1 = landing_patient_referal.merge(tgt_patients_df, left_on='src_patient_id', right_on='macpractice_patient_id', how='inner')
landing_patient_referal2 = landing_patient_referal1.drop(['macpractice_referral_id','macpractice_patient_id'], axis=1)

#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE patient_contact_details ADD COLUMN IF NOT EXISTS macpractice_patient_contact_id INT(10) DEFAULT NULL;")
target_myconnection.commit()

patient_contact_details_bar = tqdm(total=len(landing_patient_referal2), desc="Inserting Patient Contact Details")

for index,row in landing_patient_referal2.iterrows():
    patient_contact_details_bar.update(1)
    try:
        insert_query = f"""
        INSERT INTO patient_contact_details (patient_id,contact_id,contact_type_id,`primary`,created_at,updated_at,macpractice_patient_contact_id) 
        VALUES({safe_value(row['patient_id'])},{safe_value(row['contact_id'])},{safe_value(row['contact_type_id'])},1,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),{safe_value(row['patient_referrals_tie_id'])});
        """
        target_cursor.execute(insert_query)
        
    except Exception as e:
        print(f"Error inserting patient contact detail for patient_referrals_tie_id {row['patient_referrals_tie_id']}: {e}")
        break

target_myconnection.commit()
patient_contact_details_bar.close()
target_cursor.close()
target_myconnection.close()
print('Patient Contact Details module completed.')