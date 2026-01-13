import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

excel_path = getDocumentExcelPath()

src_scan_excel = os.path.join(excel_path, 'document_scan_mapping.csv')
src_scan_df = pd.read_csv(src_scan_excel)

#Adding source identifier column in target table 
target_cursor.execute("ALTER TABLE scan_documents ADD COLUMN IF NOT EXISTS macpractice_attached_file_id INT(10) DEFAULT NULL;")
target_myconnection.commit()
target_cursor.execute("ALTER TABLE scan_documents ADD COLUMN IF NOT EXISTS macpractice_hash LONGTEXT DEFAULT NULL;")
target_myconnection.commit()

tgt_scan_category = pd.read_sql("SELECT id as scan_category_id, name as scan_category_name FROM scan_categories",target_myconnection)
src_scan_categories  = src_scan_df[['source_table']].drop_duplicates().reset_index(drop=True)
new_scan_categories = src_scan_categories[~src_scan_categories['source_table'].isin(tgt_scan_category['scan_category_name'])]
new_scan_categories['name'] = new_scan_categories['source_table']

if not new_scan_categories.empty:
    for index, row in new_scan_categories.iterrows():
        insert_query = f"""
        INSERT INTO `scan_categories` (`name`, `is_default`, `is_archive`, `system_generated`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES ({safe_value(row['name'])}, 0, 0, 1, 0, 0, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL);
        """
        target_cursor.execute(insert_query)
target_myconnection.commit()

tgt_scan_category = pd.read_sql("SELECT id as scan_category_id, name as scan_category_name FROM scan_categories",target_myconnection)
landing_scan_df = src_scan_df.merge(tgt_scan_category, left_on='source_table', right_on='scan_category_name', how='left')
landing_scan_df1 = landing_scan_df.drop(['source_table','scan_category_name'], axis=1)

# target table to eliminate duplicates based on source identifier
existing_scans_df = pd.read_sql("SELECT macpractice_attached_file_id, macpractice_hash FROM scan_documents WHERE macpractice_attached_file_id IS NOT NULL",target_myconnection)
landing_scan_df2 = landing_scan_df1[~landing_scan_df1[['attached_file_id','hash']].apply(tuple,1).isin(existing_scans_df[['macpractice_attached_file_id','macpractice_hash']].apply(tuple,1))]

scans_bar = tqdm(total=len(landing_scan_df2), desc="Updating Scans")

for index,row in landing_scan_df2.iterrows():
    scans_bar.update(1)
    try:
        insert_query = f"""
        INSERT INTO `scan_documents` (`id`, `patient_id`, `description`, `doctor_id`, `document_date`, `scan_category_id`, `file_extension`, `notes`, `file_path`, `status`, `mail_flag`, `sms_flag`, `fax_flag`, `letter_flag`, `patient_communication`, `episode_id`, `visible`, `is_imported`, `ngupload`, `email_sent_date`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, `macpractice_attached_file_id`,`macpractice_hash`) 
        VALUES ({safe_value(row['scan_id'])}, {safe_value(row['tgt_patient_id'])}, {safe_value(row['filename'])}, 1, {safe_value(row['document_date'])},{safe_value(row['scan_category_id'])}, '.pdf', NULL, '', 4, 0, 0, 0, 0, '', NULL, 1, 0, 0, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['attached_file_id'])},{safe_value(row['hash'])});
        """
        target_cursor.execute(insert_query)
    except Exception as e:
        print(f"Error inserting scan for attached_file_id {row['attached_file_id']}: {e}")
        #logging.error(f"Error inserting attached_file_id {row['attached_file_id']}: {e}")
        break

target_myconnection.commit()
scans_bar.close()
target_cursor.close()
target_myconnection.close()
print('Scan insertion completed.')
