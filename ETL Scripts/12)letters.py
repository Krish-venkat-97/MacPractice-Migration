import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

excel_path = getDocumentExcelPath()

src_letter_excel = os.path.join(excel_path, 'document_letter_mapping.csv')
src_letters_df = pd.read_csv(src_letter_excel)

#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE letters ADD COLUMN IF NOT EXISTS macpractice_attached_file_id INT(10) DEFAULT NULL;")
target_myconnection.commit()
target_cursor.execute("ALTER TABLE letters ADD COLUMN IF NOT EXISTS macpractice_hash LONGTEXT DEFAULT NULL;")
target_myconnection.commit()

tgt_letter_category = pd.read_sql("SELECT id as letter_category_id, name as letter_category_name FROM letter_categories",target_myconnection)
src_letter_categories  = src_letters_df[['source_table']].drop_duplicates().reset_index(drop=True)
new_letter_categories = src_letter_categories[~src_letter_categories['source_table'].isin(tgt_letter_category['letter_category_name'])]
new_letter_categories['name'] = new_letter_categories['source_table']

if not new_letter_categories.empty:
    for index, row in new_letter_categories.iterrows():
        insert_query = f"""
        INSERT INTO `letter_categories` (`name`, `is_default`, `is_archive`, `system_generated`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES ({safe_value(row['name'])}, 0, 0, 1, 0, 0, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL);
        """
        target_cursor.execute(insert_query)
target_myconnection.commit()

tgt_letter_category = pd.read_sql("SELECT id as letter_category_id, name as letter_category_name FROM letter_categories",target_myconnection)
landing_letters_df = src_letters_df.merge(tgt_letter_category, left_on='source_table', right_on='letter_category_name', how='left')
landing_letters_df1 = landing_letters_df.drop(['source_table','letter_category_name'], axis=1)

# target table to eliminate duplicates based on source identifier
existing_letters_df = pd.read_sql("SELECT macpractice_attached_file_id, macpractice_hash FROM letters WHERE macpractice_attached_file_id IS NOT NULL",target_myconnection)
landing_letters_df2 = landing_letters_df1[~landing_letters_df1[['attached_file_id','hash']].apply(tuple,1).isin(existing_letters_df[['macpractice_attached_file_id','macpractice_hash']].apply(tuple,1))]

letters_bar = tqdm(total=len(landing_letters_df2), desc="Updating Letters")

for index,row in landing_letters_df2.iterrows():
    letters_bar.update(1)
    try:
        insert_query = f"""
        INSERT INTO `letters` (id,`template_id`, `patient_id`, `doctor_id`, `description`, `letter_category_id`, `episode_id`, `extension`, `mail_flag`, `sms_flag`, `fax_flag`, `letter_flag`, `patient_communication`, `is_word_close`, `new`, `verified`, `printed`, `removed`, `completed`, `from_where`, `external_id`, `document_date`, `type`, `phyalert`, `active`, `workflow_state_id`, `clinic_workflow_id`, `available_editor_id`, `visible`, `ngupload`, `add_from_letter`, `status`, `temp_status`, `contact_type_id`, `contact_id`, `notes`, `dictate_it_key`, `followup`, `dictate_it_followup`, `from_dictate_it`, `skip_printtray`, `verified_date`, `email_sent_date`, `emailed`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, `finalize`, `macpractice_attached_file_id`,`macpractice_hash`) 
        VALUES ({safe_value(row['letter_id'])},NULL, {safe_value(row['tgt_patient_id'])}, 1, {safe_value(row['filename'])}, {safe_value(row['letter_category_id'])}, NULL, 'doc', 0, 0, 0, 0, '', 0, 0, 1, 0, 0, 1, '', 0, {safe_value(row['document_date'])}, 'word', NULL, 1, 0, 0, 2, 1, 0, 0, 3, 0, NULL, NULL, NULL, '0', 0, 0, 0, 0, NULL, NULL, 0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, 0,{safe_value(row['attached_file_id'])},{safe_value(row['hash'])});
        """
        target_cursor.execute(insert_query)
    except Exception as e:
        print(f"Error inserting letter for attached_file_id {row['attached_file_id']}: {e}")
        #logging.error(f"Error inserting attached_file_id {row['attached_file_id']}: {e}")
        break

target_myconnection.commit()
letters_bar.close()
target_cursor.close()
target_myconnection.close()
print('Letters migration completed.')