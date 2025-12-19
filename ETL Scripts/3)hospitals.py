import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

src_hospital_df = pd.read_sql("""
SELECT o.office_id,o.name,a.address1,a.address2,c.city,c.state AS county,c.zip,o.phone,o.fax
FROM office o
LEFT JOIN address a
ON o.address_id = a.address_id
INNER JOIN citystatezip c
ON a.citystatezip_id = c.citystatezip_id""",source_myconnection)

hospital_bar = tqdm(total=len(src_hospital_df), desc="Inserting Hospitals")

#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE hospitals ADD COLUMN IF NOT EXISTS macpractice_office_id INT(10) DEFAULT NULL;")
target_myconnection.commit()

for index,row in src_hospital_df.iterrows():
    hospital_bar.update(1)
    insert_query = f"""
    INSERT INTO hospitals (name,type_id,address1,address2,county,town,postcode,phone,fax,created_at,updated_at,created_user_id,updated_user_id,macpractice_office_id) 
    VALUES({safe_value(row['name'])},4,{safe_value(row['address1'])},{safe_value(row['address2'])},
    {safe_value(row['county'])},{safe_value(row['city'])},{safe_value(row['zip'])},
    {safe_value(row['phone'])},{safe_value(row['fax'])},CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),1,1,{safe_value(row['office_id'])});
    """
    try:
        target_cursor.execute(insert_query)
        target_myconnection.commit()
    except Exception as e:
        print(f"Error inserting hospital {row['name']}: {e}")
        logging.error(f"Error inserting hospital {row['name']}: {e}")
        break


hospital_bar.close()
target_cursor.close()
target_myconnection.close()
print('Hospital insertion completed.')
