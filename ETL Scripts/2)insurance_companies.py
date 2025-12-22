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
src_insurance_companies  = src_insurance_companies = """
SELECT
    i.insurance_id AS insurance_id,
    i.carrier      AS name,
    a.address1     AS address1,
    a.address2     AS address2,
    c.city         AS town,
    c.ZIP          AS postcode
FROM insurance i
LEFT JOIN address a
    ON i.address_id = a.address_id
LEFT JOIN citystatezip c
    ON a.citystatezip_id = c.citystatezip_id
"""

src_insurance_companies_df = pd.read_sql(src_insurance_companies, src_connection)

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE insurance_companies ADD COLUMN IF NOT EXISTS MacPractice_InsComp_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
tgt_connection.commit()

#Datetime obj
datetimeobj = datetime.now()


#insurance_id generation
insurance_max = 'SELECT MAX(id) FROM insurance_companies'
insurance_max_df = pd.read_sql(insurance_max,tgt_connection)

if insurance_max_df is None or insurance_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = insurance_max_df.iloc[0, 0] + 1
src_insurance_companies_df.insert(0,'insurance_comp_id',range(max_id,max_id+len(src_insurance_companies_df)))


#Fetching existing MacPractice Id
tgt_insurance = 'SELECT DISTINCT MacPractice_InsComp_Id FROM insurance_companies'
tgt_insurance_df = pd.read_sql(tgt_insurance,tgt_connection)

tgt_insurance_df['MacPractice_InsComp_Id'] = tgt_insurance_df['MacPractice_InsComp_Id'].astype(str)


#Insert Loop

bar = tqdm(total=len(src_insurance_companies_df),desc='Inserting Insurance Companies',position=0)


for _, row in src_insurance_companies_df.iterrows():
    bar.update(1)

    if str(row['insurance_id']) not in tgt_insurance_df['MacPractice_InsComp_Id'].values:
        try:
            insurance_company_insert = f"""
            INSERT INTO insurance_companies (
                id,
                name,
                address1,
                address2,
                town,
                postcode,
                is_archive,
                is_default,
                created_user_id,
                updated_user_id,
                deleted_user_id,
                created_at,
                updated_at,
                MacPractice_InsComp_Id
            )
            VALUES (
                {safe_value(row['insurance_comp_id'])},
                {safe_value(row['name'])},
                {safe_value(row['address1'])},
                {safe_value(row['address2'])},
                {safe_value(row['town'])},
                {safe_value(row['postcode'])},
                0,                      -- is_archive
                0,                      -- is_default
                1,                      -- created_user_id
                1,                      -- updated_user_id
                0,                      -- deleted_user_id
                '{datetimeobj}',
                '{datetimeobj}',
                {safe_value(row['insurance_id'])}
            )
            """
            target_cursor.execute(insurance_company_insert)

            
        except Exception as e:
            logging.error(f"Error: {e} \n Row: {insurance_company_insert}")
            print('Exception occured in the insurance companies module',flush=True)
            break
    else:
        continue
tgt_connection.commit()
bar.close()

#Updating duplicate rows with MacPractice_InsComp_Id
query_duplicate_update = """
UPDATE insurance_companies ic
INNER JOIN 
(SELECT * FROM insurance_companies ic WHERE ic.MacPractice_InsComp_Id IS NOT NULL)ic1
ON ic.name = ic1.name
SET ic.MacPractice_InsComp_Id = ic1.MacPractice_InsComp_Id
WHERE ic.MacPractice_InsComp_Id IS NULL;
"""
target_cursor.execute(query_duplicate_update)
tgt_connection.commit()

query_delete_duplicates = """
DELETE ic 
FROM insurance_companies ic
INNER JOIN insurance_companies ic1
ON ic.MacPractice_InsComp_Id = ic1.MacPractice_InsComp_Id
WHERE ic.id > ic1.id;
"""
target_cursor.execute(query_delete_duplicates)
tgt_connection.commit()
tgt_connection.close()

print('finished insurance companies module',flush=True)