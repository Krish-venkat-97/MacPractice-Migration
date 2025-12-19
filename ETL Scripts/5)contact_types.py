import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

src_contact_types_df = pd.read_sql("SELECT referral_type_id,LTRIM(RTRIM(UPPER(a_description))) as a_description FROM referral_type",source_myconnection)
tgt_contact_types_df = pd.read_sql("SELECT LTRIM(RTRIM(UPPER(name))) as name FROM contact_types",target_myconnection)
contact_types_to_insert = src_contact_types_df[~src_contact_types_df['a_description'].isin(tgt_contact_types_df['name'])]

contact_types_bar = tqdm(total=len(contact_types_to_insert), desc="Inserting Contact Types")

#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE contact_types ADD COLUMN IF NOT EXISTS macpractice_referral_type_id INT(10) DEFAULT NULL;")
target_myconnection.commit()

for index,row in contact_types_to_insert.iterrows():
    contact_types_bar.update(1)
    insert_query = f"""
    INSERT INTO contact_types (name,order_by,created_at,updated_at,macpractice_referral_type_id) 
    VALUES({safe_value(row['a_description'])},{safe_value(row['a_description'])},CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),{safe_value(row['referral_type_id'])});
    """
    try:
        target_cursor.execute(insert_query)
        target_myconnection.commit()
    except Exception as e:
        print(f"Error inserting contact type {row['a_description']}: {e}")
        logging.error(f"Error inserting contact type {row['a_description']}: {e}")
        break

contact_types_bar.close()
target_cursor.close()
target_myconnection.close()
print('Contact types insertion completed.')