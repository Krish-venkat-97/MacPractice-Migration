import pandas as pd
import os  
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

file_loc = getSourceFilePath()
excel_path = getDocumentExcelPath()

file_list = []

for root,dirs,files in os.walk(file_loc):
    for file in files:
        file_list.append(os.path.join(file))

physical_file_df = pd.DataFrame(file_list, columns=['file_hash'])

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

sql_file_df = pd.read_sql("""
SELECT 
a.attached_file_id,
a.file_name AS filename,
a.hash,
i.patient_id as src_patient_id,
CAST(i.date AS DATE) AS document_date,
'image_attachment' AS source_table
FROM image_attachments i 
inner join attached_file a
ON i.attached_file_id = a.attached_file_id
WHERE patient_id IS NOT NULL
UNION 
SELECT a.attached_file_id,
case when a.file_name = '' then CONCAT(n.name,'.png')
ELSE a.file_name
END AS upd_file_name,
a.hash
,n.patient_id as src_patient_id,
CAST(n.created AS DATE) AS document_date,
'image_notes' AS source_table
FROM attached_file a
INNER JOIN notes_image_tie ni
ON a.attached_file_id = ni.attached_file_id
INNER JOIN notes n
ON n.notes_id = ni.notes_id
UNION 
SELECT a.attached_file_id,a.file_name AS file_name,a.hash,
d.patient_id as src_patient_id,CAST(d.date AS DATE) AS document_date,
'image_digirad_final' AS source_table
FROM digirad_image di
INNER JOIN digirad d
ON di.digirad_id = d.digirad_id
INNER JOIN attached_file a
ON di.final_attached_file_id = a.attached_file_id
UNION
SELECT a.attached_file_id,a.file_name AS file_name,a.hash,
d.patient_id as src_patient_id,CAST(d.date AS DATE) AS document_date,
'image_digirad_original' AS source_table
FROM digirad_image di
INNER JOIN digirad d
ON di.digirad_id = d.digirad_id
INNER JOIN attached_file a
ON di.original_attached_file_id = a.attached_file_id
UNION
SELECT a.attached_file_id,a.file_name AS file_name,a.hash,
d.patient_id as src_patient_id,CAST(d.date AS DATE) AS document_date,
'image_digirad_dicom' AS source_table
FROM digirad_image di
INNER JOIN digirad d
ON di.digirad_id = d.digirad_id
INNER JOIN attached_file a
ON di.dicom_attached_file_id = a.attached_file_id
UNION
SELECT p.attached_file_id,a.file_name AS filename,a.hash,p.patient_id as src_patient_id,
CAST(p.date AS DATE) AS document_date,
'patient_photo' AS source_table
FROM patient_photo p
INNER JOIN attached_file a
ON p.attached_file_id = a.attached_file_id;
""",source_myconnection)

merged_df = physical_file_df.merge(sql_file_df, left_on='file_hash', right_on='hash', how='left')

# getting the file extension
def get_file_extension(filename):
    if pd.isna(filename) or filename.strip() == '':
        return ''
    else:
        return os.path.splitext(filename)[1].lower()
    
merged_df['file_extension'] = merged_df['filename'].apply(get_file_extension)


# if file extension is numeric an source table is image_attachment then set file extension to .png
def correct_file_extension(row):
    if row['file_extension'].replace('.','').replace('. ','').isdigit() and row['source_table'] == 'image_attachment':
        return '.png'
    elif row['file_extension'] not in ['.pdf','.doc','.docx','.dcm','.rtf','.htm','.html'] and row['source_table'] == 'image_attachment':
        return '.png'
    elif row['file_extension'] == '':
        return '.png'
    else:
        return row['file_extension']
    
merged_df['file_extension'] = merged_df.apply(correct_file_extension, axis=1)

# getting the file extension in different dataframe to avoid recalculation
file_extension_df = merged_df[['file_extension']].drop_duplicates().reset_index(drop=True)

document_df = merged_df[['attached_file_id', 'filename', 'hash', 'src_patient_id','document_date', 'source_table', 'file_extension']]
document_df = document_df.dropna(subset=['attached_file_id', 'src_patient_id'])
document_df['attached_file_id'] = document_df['attached_file_id'].astype(int)
document_df['src_patient_id'] = document_df['src_patient_id'].astype(int)

tgt_patient_df = pd.read_sql("SELECT id AS tgt_patient_id,macpractice_patient_id FROM patients", get_tgt_myconnection())

merged_df1 = document_df.merge(tgt_patient_df, left_on='src_patient_id', right_on='macpractice_patient_id', how='inner')
merged_df2 = merged_df1.drop(columns=['macpractice_patient_id','src_patient_id'])

scan_df = merged_df2[merged_df2['file_extension'].isin(['.pdf','.jpg','.jp2','.jpeg','.png','.tif','.tiff','.html','.htm'])][['attached_file_id', 'filename', 'hash', 'tgt_patient_id','document_date', 'source_table', 'file_extension']]
letter_df = merged_df2[~merged_df2['file_extension'].isin(['.pdf','.jpg','.jp2','.jpeg','.png','.tif','.tiff','.html','.htm'])][['attached_file_id', 'filename', 'hash', 'tgt_patient_id','document_date', 'source_table', 'file_extension']]

# insert auto increment scan_ids
scan_max = pd.read_sql("SELECT IFNULL(MAX(id),0) AS max_id FROM scan_documents",target_myconnection)
next_scan_id = int(scan_max['max_id'][0]) + 1
scan_df.insert(0,'scan_id',range(next_scan_id, next_scan_id + len(scan_df)))

# insert auto increment letter_ids
letter_max = pd.read_sql("SELECT IFNULL(MAX(id),0) AS max_id FROM letters",target_myconnection)
next_letter_id = int(letter_max['max_id'][0]) + 1
letter_df.insert(0,'letter_id',range(next_letter_id, next_letter_id + len(letter_df)))

def getScanFile(row):
    file = str(row['scan_id']) + row['file_extension']
    return file
scan_df['target_file'] = scan_df.apply(getScanFile, axis=1)

def getLetterFile(row):
    file = str(row['letter_id']) + row['file_extension']
    return file
letter_df['target_file'] = letter_df.apply(getLetterFile, axis=1)


scan_df.to_csv(os.path.join(excel_path, 'document_scan_mapping.csv'), index=False)
letter_df.to_csv(os.path.join(excel_path, 'document_letter_mapping.csv'), index=False)

print('Creation of mapping files completed.')