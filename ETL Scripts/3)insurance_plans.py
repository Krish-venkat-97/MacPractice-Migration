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
src_insurance_plans = """
SELECT
	ins_plan_type_id AS ins_plan_type_id,
   CONCAT(plan_type, ' ', '-', ' ', description) AS name
FROM ins_plan_type;
"""

src_insurance_plans_df = pd.read_sql(src_insurance_plans, src_connection)

#Datetime obj
datetimeobj = datetime.now()

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE insurance_plans ADD COLUMN IF NOT EXISTS MacPractice_InsPlan_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
query_3 = "DELETE FROM insurance_plans WHERE MacPractice_InsPlan_Id IS NOT NULL;"
target_cursor.execute(query_3)
tgt_connection.commit()


#id generation
insurance_plan_max = 'SELECT MAX(id) FROM insurance_plans'
insurance_plan_max_df = pd.read_sql(insurance_plan_max,tgt_connection)

if insurance_plan_max_df is None or insurance_plan_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = insurance_plan_max_df.iloc[0, 0] + 1

# Insert generated IDs directly into source dataframe
src_insurance_plans_df.insert(
    0,
    'ins_plan_id',
    range(max_id, max_id + len(src_insurance_plans_df))
)

#Insert Loop
bar = tqdm(
    total=len(src_insurance_plans_df),
    desc='Inserting insurance plans',
    position=0
)

for _, row in src_insurance_plans_df.iterrows():
    bar.update(1)

    try:
        insurance_plan_insert = f"""
        INSERT INTO insurance_plans (
            id,
            name,
            created_user_id,
            updated_user_id,
            created_at,
            updated_at,
            MacPractice_InsPlan_Id
        )
        VALUES (
            {safe_value(row['ins_plan_id'])},
            {safe_value(row['name'])},
            1,
            1,
            '{datetimeobj}',
            '{datetimeobj}',
            {safe_value(row['ins_plan_type_id'])}
        )
        """
        target_cursor.execute(insurance_plan_insert)

    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error(f"Query: {insurance_plan_insert}")
        print(
            'Exception occurred in the insurance plans module',
            flush=True
        )
        break

tgt_connection.commit()
tgt_connection.close()
bar.close()
print('finished insurance plans module',flush=True)

