import pandas as pd
import os  
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

file_loc = r'D:\Azeez\bcSHC\MPBackup.tmp.E2C98273-77D7-4E6C-B432-3D3CCD74B25A.mpbak\Data\Attachments'

file_list = []

for root,dirs,files in os.walk(file_loc):
    for file in files:
        file_list.append(os.path.join(file))

physical_file_df = pd.DataFrame(file_list, columns=['file_hash'])

source_myconnection = get_src_myconnection()

sql_file_df = pd.read_sql("""
SELECT 
a.attached_file_id,
a.file_name AS filename,
a.hash,
i.patient_id,
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
,n.patient_id,
CAST(n.created AS DATE) AS document_date,
'image_notes' AS source_table
FROM attached_file a
INNER JOIN notes_image_tie ni
ON a.attached_file_id = ni.attached_file_id
INNER JOIN notes n
ON n.notes_id = ni.notes_id
UNION 
SELECT a.attached_file_id,a.file_name AS file_name,a.hash,
d.patient_id,CAST(d.date AS DATE) AS document_date,
'image_digirad_final' AS source_table
FROM digirad_image di
INNER JOIN digirad d
ON di.digirad_id = d.digirad_id
INNER JOIN attached_file a
ON di.final_attached_file_id = a.attached_file_id
UNION
SELECT a.attached_file_id,a.file_name AS file_name,a.hash,
d.patient_id,CAST(d.date AS DATE) AS document_date,
'image_digirad_original' AS source_table
FROM digirad_image di
INNER JOIN digirad d
ON di.digirad_id = d.digirad_id
INNER JOIN attached_file a
ON di.original_attached_file_id = a.attached_file_id
UNION
SELECT a.attached_file_id,a.file_name AS file_name,a.hash,
d.patient_id,CAST(d.date AS DATE) AS document_date,
'image_digirad_dicom' AS source_table
FROM digirad_image di
INNER JOIN digirad d
ON di.digirad_id = d.digirad_id
INNER JOIN attached_file a
ON di.dicom_attached_file_id = a.attached_file_id
UNION
SELECT p.attached_file_id,a.file_name AS filename,a.hash,p.patient_id,
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

document_df = merged_df[['attached_file_id', 'filename', 'hash', 'patient_id','document_date', 'source_table', 'file_extension']]

scan_df = merged_df[merged_df['file_extension'].isin(['.pdf','.jpg','.jp2','.jpeg','.png','.tif','.tiff','.html','.htm'])][['attached_file_id', 'filename', 'hash', 'patient_id','document_date', 'source_table', 'file_extension']]
letter_df = merged_df[~merged_df['file_extension'].isin(['.pdf','.jpg','.jp2','.jpeg','.png','.tif','.tiff','.html','.htm'])][['attached_file_id', 'filename', 'hash', 'patient_id','document_date', 'source_table', 'file_extension']]
