import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

source_myconnection = get_src_myconnection()

target_myconnection = get_tgt_myconnection()
target_cursor = target_myconnection.cursor()

src_patient_titles_df = pd.read_sql("SELECT DISTINCT case when RIGHT(salute,1) = '.' then REPLACE(salute,'.','') ELSE salute END AS salute FROM person",source_myconnection)
src_patient_titles_df['salute'] = src_patient_titles_df['salute'].fillna('')
src_patient_titles_df['salute'] = src_patient_titles_df['salute'].str.strip()

src_refferals_titles_df = pd.read_sql("SELECT DISTINCT case when RIGHT(professional_title,1) = '.' then REPLACE(professional_title,'.','') ELSE professional_title END AS professional_title FROM referrals",source_myconnection)
src_refferals_titles_df['professional_title'] = src_refferals_titles_df['professional_title'].fillna('')
src_refferals_titles_df['professional_title'] = src_refferals_titles_df['professional_title'].str.strip()

# Combine and get unique titles
unique_titles = pd.concat([src_patient_titles_df['salute'], src_refferals_titles_df['professional_title']]).dropna().unique()
source_titles_df = pd.DataFrame(unique_titles, columns=['title'])

tgt_titles_df = pd.read_sql("SELECT name FROM titles",target_myconnection)
titles_to_insert = source_titles_df[~source_titles_df['title'].isin(tgt_titles_df['name'])]

title_bar = tqdm(total=len(titles_to_insert), desc="Inserting Titles")

for index,row in titles_to_insert.iterrows():
    title_bar.update(1)
    insert_query = f"""
    INSERT INTO titles (name,order_by,created_at,updated_at) 
    VALUES({safe_value(row['title'])},100,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP());
    """
    try:
        target_cursor.execute(insert_query)
        target_myconnection.commit()
    except Exception as e:
        print(f"Error inserting title {row['title']}: {e}")
        logging.error(f"Error inserting title {row['title']}: {e}")
        break

title_bar.close()
target_cursor.close()
target_myconnection.close()
print('Title insertion completed.')