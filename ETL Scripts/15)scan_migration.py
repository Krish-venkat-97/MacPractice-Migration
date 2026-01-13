import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

excel_path = getDocumentExcelPath()
input_file_path = getSourceFilePath()
output_file_path = getTargetFilePath()

src_scan_excel = os.path.join(excel_path, 'document_scan_mapping.csv')
src_scans_df = pd.read_csv(src_scan_excel)

src_scans_df1 = src_scans_df[['hash','tgt_patient_id','target_file']]

def SourceFilePath(hash):
    input_folder = hash[:3]
    full_source_path = os.path.join(input_file_path,input_folder,hash)
    return full_source_path

src_scans_df1['source_file_path'] = src_scans_df1['hash'].apply(SourceFilePath)

def getTargetFolder(row):
    target_folder = os.path.join(output_file_path,str(row['tgt_patient_id']),'scans','verified')
    return target_folder
src_scans_df1['target_folder_path'] = src_scans_df1.apply(getTargetFolder, axis=1)

def getTargetFilePath(row):
    target_file = os.path.join(row['target_folder_path'], row['target_file'])
    return target_file
src_scans_df1['final_target_file_path'] = src_scans_df1.apply(getTargetFilePath, axis=1)

landing_scans_files_df = src_scans_df1[['source_file_path','target_folder_path','final_target_file_path']]

scan_bar = tqdm(total=len(landing_scans_files_df), desc="Migrating Scan Files")

for index,row in landing_scans_files_df.iterrows():
    scan_bar.update(1)
    try:
        if not os.path.exists(row['target_folder_path']):
            os.makedirs(row['target_folder_path'])
        if  os.path.exists(row['source_file_path']):
            shutil.copy2(row['source_file_path'], row['final_target_file_path'])
        else:
            print(f"Source file does not exist: {row['source_file_path']}")
            #logging.error(f"Source file does not exist: {row['source_file_path']}")
    except Exception as e:
        print(f"Error migrating scan file {row['source_file_path']}: {e}")
        #logging.error(f"Error migrating scan file {row['source_file_path']}: {e}")
        break   

scan_bar.close()
print('Scan files migration completed.')