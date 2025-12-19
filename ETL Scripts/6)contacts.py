import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

src_contacts_df = pd.read_sql("""
SELECT referrals_id,rt.referral_type_id AS contact_type_idX,
case when RIGHT(professional_title,1) = '.' then REPLACE(professional_title,'.','') ELSE professional_title END AS professional_titlex,
LTRIM(RTRIM(CONCAT(IFNULL(r.`first`,''), ' ',IFNULL(r.middle,'')))) AS first_name, r.`last` AS sur_name,
LTRIM(RTRIM(CONCAT(IFNULL(r.`first`,''), ' ',IFNULL(r.middle,''),' ',IFNULL(r.`last`,'')))) AS display_name,
a.address1,a.address2,c.city AS address3,c.state AS county,c.zip AS postcode,
r.phone1 AS work_phone,r.phone2 AS home_phone,r.phone3 AS mobile,r.email,r.web_site AS website,
0 AS is_archive,1 AS created_user_id,1 AS updated_user_id,CURRENT_TIMESTAMP() AS created_at,CURRENT_TIMESTAMP() AS updated_at
FROM referrals r
LEFT JOIN referral_type rt
ON r.referral_type_id = rt.referral_type_id
LEFT JOIN address a
ON r.address_id = a.address_id
LEFT JOIN citystatezip c
ON a.citystatezip_id = c.citystatezip_id
""",source_myconnection)

tgt_contact_types_df = pd.read_sql('SELECT id AS contact_type_id,macpractice_referral_type_id FROM contact_types WHERE macpractice_referral_type_id IS NOT NULL',target_myconnection)

tgt_titles_df = pd.read_sql("SELECT id as title_id, name as professional_title FROM titles",target_myconnection)

landing_contacts_df = src_contacts_df.merge(tgt_contact_types_df, left_on='contact_type_idX', right_on='macpractice_referral_type_id', how='left')
landing_contacts_df1 = landing_contacts_df.merge(tgt_titles_df, left_on='professional_titlex', right_on='professional_title', how='left')
landing_contacts_df2 = landing_contacts_df1.drop(['contact_type_idX','professional_titlex','macpractice_referral_type_id','professional_title'], axis=1)

#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS macpractice_referral_id INT(10) DEFAULT NULL;")
target_myconnection.commit()

# target table to eliminate duplicates based on source identifier
existing_contacts_df = pd.read_sql("SELECT macpractice_referral_id FROM contacts WHERE macpractice_referral_id IS NOT NULL",target_myconnection)
landing_contacts_df2 = landing_contacts_df2[~landing_contacts_df2['referrals_id'].isin(existing_contacts_df['macpractice_referral_id'])]

landing_contacts_df2['contact_type_id'] = landing_contacts_df2['contact_type_id'].fillna(1)
landing_contacts_df2['title_id'] = landing_contacts_df2['title_id'].fillna(1)

contacts_bar = tqdm(total=len(landing_contacts_df2), desc="Inserting Contacts")

for index,row in landing_contacts_df2.iterrows():
    contacts_bar.update(1)
    insert_query = f"""
    INSERT INTO contacts (contact_type_id,title_id,first_name,sur_name,display_name,
    address1,address2,county,town,postcode,work_phone,home_phone,mobile,email,website,
    is_archive,created_user_id,updated_user_id,created_at,updated_at,macpractice_referral_id) 
    VALUES({safe_value(row['contact_type_id'])},{safe_value(row['title_id'])},{safe_value(row['first_name'])},
    {safe_value(row['sur_name'])},{safe_value(row['display_name'])},{safe_value(row['address1'])},
    {safe_value(row['address2'])},{safe_value(row['county'])},{safe_value(row['address3'])},
    {safe_value(row['postcode'])},{safe_value(row['work_phone'])},{safe_value(row['home_phone'])},
    {safe_value(row['mobile'])},{safe_value(row['email'])},{safe_value(row['website'])},
    {safe_value(row['is_archive'])},{safe_value(row['created_user_id'])},{safe_value(row['updated_user_id'])},
    {safe_value(row['created_at'])},{safe_value(row['updated_at'])},{safe_value(row['referrals_id'])});
    """
    try:
        target_cursor.execute(insert_query)
        target_myconnection.commit()
    except Exception as e:
        print(f"Error inserting contact {row['display_name']}: {e}")
        logging.error(f"Error inserting contact {row['display_name']}: {e}")
        break

contacts_bar.close()
target_cursor.close()
target_myconnection.close() 
print('Contact insertion completed.')