import pandas as pd
import requests
from io import StringIO
import string
from unidecode import unidecode
pd.options.mode.chained_assignment = None
import numpy as np
import re
import unicodedata as ud
from ast import literal_eval
import seaborn as sns
import os
import requests
from io import StringIO

    
    
latin_letters= {}

def unique(list1):
    x = np.array(list1)
    listed  = np.unique(x)
    return listed

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr)
           for uchr in unistr
           if uchr.isalpha()) # isalpha suggested by John Machin


    
def multi_clean_name_function(name):
    name = str(name)
    name_1 = name.lower() ## lower the letters
    name_1 = re.sub(' +', ' ', name_1) ## remove double spaces
    name_1 = name_1.strip()## remove spaces in the begining and the end
    h = only_roman_chars(name_1)
    if h == True:
        name_2 = unidecode(name_1)# remove accent
        name_3 = name_2.strip(string.punctuation)
        clean_names = [name_1, name_2, name_3]
        return clean_names
    else:
        return name_1
    
def clean_country_function(country_code):
    country_code = country_code.upper()
    country_code = country_code.translate(str.maketrans('', '', string.punctuation))
    country_code = unidecode(country_code)
    country_code = ''.join([i for i in country_code if not i.isdigit()])
    return country_code


def reading_wgnd (dictionnary, path):
    if dictionnary == 1:
        try: 
            print("reading the dictionnary.")
            data = pd.read_csv(path + 'd1.csv.gz', compression="gzip" ) ### find a way to change to local path   
        except:
            print("downloading the dictionnary.")
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750348').content
            data = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
    if dictionnary == 2:
        try:    
            print("reading the dictionnary.")
            data = pd.read_csv(path + 'd2.csv.gz', compression="gzip" )
        except:
            print("downloading the dictionnary.")
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750350').content
            data = pd.read_csv(StringIO(s.decode('utf-8')),sep = ',')
    if dictionnary == 3:
        try: 
            print("reading the dictionnary.")
            data = pd.read_csv(path + 'd3.csv.gz', compression="gzip" ) 
        except:
            print("downloading the dictionnary.")
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
            data = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
    return data


import os
import pandas as pd
import requests
from io import StringIO

import os
import requests
import pandas as pd
from io import StringIO

def read_wgnd(path=False, All=True):
    # Définitions des noms de fichiers
    file1 = os.path.join(path or '', "d1.csv.gz")
    file2_1 = os.path.join(path or '', "d2_1.csv.gz")
    file2_2 = os.path.join(path or '', "d2_2.csv.gz")
    file2_3 = os.path.join(path or '', "d2_3.csv.gz")
    file2 = os.path.join(path or '', "d2.csv.gz")
    file3 = os.path.join(path or '', "d3.csv.gz")

    if All:
        # Vérifier et télécharger chaque fichier uniquement s'il est absent
        if not os.path.exists(file1):
            print('Downloading and saving first dictionary...')
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750348').content
            d1 = pd.read_csv(StringIO(s.decode('utf-8')), sep='\t')
            d1.to_csv(file1, index=False, compression="gzip")
        else:
            print('First dictionary already exists.')

        if not os.path.exists(file2):
            # Vérifier et télécharger les trois fichiers du deuxième dictionnaire
            all_d2_files_exist = os.path.exists(file2_1) and os.path.exists(file2_2) and os.path.exists(file2_3)
            if all_d2_files_exist:
                print('Concatenating second dictionary.')
                d2 = pd.concat([pd.read_csv(file2_1, compression="gzip"),
                                pd.read_csv(file2_2, compression="gzip"),
                                pd.read_csv(file2_3, compression="gzip")], ignore_index=True)
                d2.to_csv(file2, index=False, compression="gzip")
            else:
                print('Downloading and saving second dictionary...')
                if not os.path.exists(file2_1):
                    s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750350').content
                    d2_1 = pd.read_csv(StringIO(s.decode('utf-8')), sep=',')
                    d2_1.to_csv(file2_1, index=False, compression="gzip")
                if not os.path.exists(file2_2):
                    s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750352').content
                    d2_2 = pd.read_csv(StringIO(s.decode('utf-8')), sep=',')
                    d2_2.to_csv(file2_2, index=False, compression="gzip")  # Correction d'indentation ici
                if not os.path.exists(file2_3):
                    s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750353').content
                    d2_3 = pd.read_csv(StringIO(s.decode('utf-8')), sep=',')
                    d2_3.to_csv(file2_3, index=False, compression="gzip")
                # Concaténation des trois fichiers en un seul
                print('Concatenating second dictionary.')
                d2 = pd.concat([pd.read_csv(file2_1, compression="gzip"),
                                pd.read_csv(file2_2, compression="gzip"),
                                pd.read_csv(file2_3, compression="gzip")], ignore_index=True)
                d2.to_csv(file2, index=False, compression="gzip")
            
        else:
            print('Second dictionary already exists.')

        if not os.path.exists(file3):
            print('Downloading and saving third dictionary...')
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
            d3 = pd.read_csv(StringIO(s.decode('utf-8')), sep='\t')
            d3.to_csv(file3, index=False, compression="gzip")
        else:
            print('Third dictionary already exists.')
    else:
        # Si 'All' est False, on ne télécharge que le troisième dictionnaire
        if not os.path.exists(file3):
            print('Downloading and saving third dictionary...')
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
            d3 = pd.read_csv(StringIO(s.decode('utf-8')), sep='\t')
            d3.to_csv(file3, index=False, compression="gzip")
        else:
            print('Third dictionary already exists.')

    print('All required dictionaries are saved!')




def count_missing_values(df, column_name):
    return df[column_name].isnull() | (df[column_name].astype(str).str.strip() == '')


def get_gender(df, name_column, country_column=False, split=True, split_sep=' ', threshold=0.6, path='gender_it/dictionaries/'):
    # Reset index to handle potential multi-index DataFrame and add a unique identifier for each row
    df = df.reset_index(drop=True).reset_index(names='name_id')
    null_lines = df[name_column].isna().sum()
    print(f'WARNING: {null_lines} without names.')

    # Store a copy of the original DataFrame for merging the results later
    original = df.copy()

    # Select relevant columns, include the country column if provided
    if country_column:
        df = df[['name_id', name_column, country_column]]
    else:
        df = df[['name_id', name_column]]
    
    # Initialize `dff` as an empty DataFrame to avoid UnboundLocalError
    dff = pd.DataFrame()

    # Clean the name column using the external cleaning function
    df['clean_name'] = df[name_column].apply(multi_clean_name_function)
    df = df.drop(columns=[name_column])  # Remove the original name column for further processing

    # Split the clean_name into individual names if specified
    df = df.explode('clean_name')
    if split:
        df['clean_name'] = df['clean_name'].str.split(split_sep)
        df = df.explode('clean_name')  # Split each name into individual components
        df['surname_position'] = df.groupby('name_id').cumcount() + 1  # Track the position of each name part

    # Handle the presence of a country column
    if country_column:
        # Define a regex pattern to match valid two-letter country codes
        pattern = r'^[A-Z]{2}$'
        # Separate rows with valid two-letter country codes
        dff = df[df[country_column].str.upper().str.match(pattern, na=False)].copy()
        dff[country_column] = dff[country_column].astype(str)
        # Clean the country column values using the external cleaning function
        dff['clean_country_column'] = dff[country_column].apply(clean_country_function)
        dff = dff.drop(columns=[country_column])  # Remove the original country column
        # Separate rows without valid two-letter country codes (null, empty, non-matching values)
        dfn = df[~df[country_column].str.upper().str.match(pattern, na=False)].copy()
        dfn = dfn.drop(columns=[country_column])
        print('WARNING:', len(dfn['name_id'].unique()), 'without country_codes.')
    else:
        # If no country column is provided, use a copy of the DataFrame
        dfn = df.copy()
    # Initialize cols for later use
    cols = []
    
    # Step 1: Try to find gender using the name-country-gender dictionary
    found = pd.DataFrame()  # Initialize found to ensure it's defined before being used
    if country_column:
        print('Step 1 - Reading the name-country-gender dictionary')
        data = reading_wgnd(1, path)
        data = data.rename(columns={'name': 'clean_name', 'code': 'clean_country_column'})
        
        # Filter the dictionary to include only relevant names and countries
        data = data[data['clean_name'].isin(dff['clean_name'])]
        data = data[data['clean_country_column'].isin(dff['clean_country_column'])]
        
        # Remove duplicates and pivot to structure gender probabilities
        data = data.drop_duplicates(subset=['clean_name', 'clean_country_column'])
        data = data.pivot(index=['clean_name', 'clean_country_column'], columns="gender", values="wgt").reset_index()

        # Merge with the DataFrame to find matches
        found = data.merge(dff, on=['clean_name', 'clean_country_column'])
        cols = list(data.columns[2:])  # Get gender columns after pivot
        found = found[(found[cols] > threshold).any(axis=1)]  # Apply the threshold for gender probability
        
        # Sort by name position and drop duplicates
        found = found.sort_values('surname_position', ascending=True).drop_duplicates(subset='name_id')
        found['level'] = 1  # Mark the results level for later use
        found = found.fillna(0)  # Fill NaNs with zero for further processing

        # Remove found entries from the main DataFrame
        dff = dff[~dff['name_id'].isin(found['name_id'])]

    # Step 2: Attempt to find gender using the name-language-gender dictionary
    if country_column:
        print('Step 2 - Reading the name-language-gender dictionary')
        data = reading_wgnd(2, path)
        data = data.rename(columns={'name': 'clean_name', 'code': 'clean_country_column'})
        
        # Filter the dictionary to include relevant names and languages
        data = data[data['clean_name'].isin(dff['clean_name'])]
        data = data[data['clean_country_column'].isin(dff['clean_country_column'])]
        
        # Remove duplicates and merge with the DataFrame
        data = data.drop_duplicates(subset=['clean_name', 'clean_country_column'])
        res = data.merge(dff, on=['clean_name', 'clean_country_column'])
        
        # Sort and drop duplicates based on name ID
        res = res.sort_values('surname_position', ascending=True).drop_duplicates(subset='name_id')
        res['wgt'] = 1  # Assign a weight of 1 for found results
        res = res.pivot(index=['clean_name', 'clean_country_column', 'name_id'], columns="gender", values="wgt").reset_index()
        res['level'] = 2  # Mark the results level

        # Append to the previously found results and update DataFrame to exclude found entries
        found = pd.concat([found, res])
        dff = dff[~dff['name_id'].isin(found['name_id'])]

    # Step 3: Directly attempt to find gender using the name-gender dictionary
    print('Step 3 - Reading the name-gender dictionary')
    data = reading_wgnd(3, path)
    data = data.rename(columns={'name': 'clean_name'})

    # add unfound data into dfn
    if len(dff) > 0:
        print('dff', dff.sample())
        dfn = pd.concat([dff, dfn])
        #del dfn[country_column]
        dfn = dfn.drop_duplicates(subset='name_id')
    # Filter to include relevant names
    data = data[data['clean_name'].isin(dfn['clean_name'])]

    # Merge with the DataFrame to find matches
    res = data.merge(dfn, on='clean_name', how='inner')
    #print('Step 3:', len(res))

    # Sort and drop duplicates based on name ID
    res = res.sort_values('surname_position', ascending=True).drop_duplicates(subset='name_id')
    res['wgt'] = 1  # Assign a weight of 1 for found results

    # Pivot to organize gender information
    try:
        res = res.pivot(index=['name_id', 'clean_name', 'clean_country_column'], columns="gender", values="wgt").reset_index()
    except KeyError:
        res = res.pivot(index=['name_id', 'clean_name'], columns="gender", values="wgt").reset_index()

    res['level'] = 3  # Mark the results level

    # Append to the found results
    found = pd.concat([found, res], ignore_index=True)

    # Identify and prepare not found entries
    not_found = dfn[~dfn['name_id'].isin(found['name_id'])]
    not_found = not_found.drop_duplicates(subset='name_id')
    #print('Step 3: errors', len(not_found))
    
    # Initialize gender columns for not found entries
    for gender_col in ['F', 'M', '?']:
        if gender_col in found.columns:
            found[gender_col] = found[gender_col].fillna(0)
            not_found[gender_col] = 0
            if gender_col not in cols:
                cols.append(gender_col)

    # Set proper level and gender for not found entries
    not_found['level'] = 3
    not_found['gender'] = 'not found'

    # Determine gender for found entries
    found['gender'] = found[cols].idxmax(axis=1)
    
    # Prepare final columns
    result_columns = ['name_id', 'level', 'gender'] + cols
    if 'clean_name' in found.columns:
        result_columns.append('clean_name')
    if 'clean_country_column' in found.columns:
        result_columns.append('clean_country_column')
    if 'surname_position' in found.columns:
        result_columns.append('surname_position')

    # Ensure not_found has all necessary columns
    for col in result_columns:
        if col not in not_found.columns and col not in ['gender', 'level'] + cols:
            not_found[col] = None

    # Select columns for found and not_found
    found = found[result_columns]
    not_found = not_found[result_columns]
    
    # Combine found and not found results
    res_final = pd.concat([found, not_found])
    
    # Merge with original DataFrame
    res_final = res_final.merge(original, on='name_id', how='right')
    
    # Clean up the final DataFrame

    res_final = res_final.drop(columns=['clean_name', 'clean_country_column', 'surname_position'], errors='ignore')


# Filtrer uniquement les lignes avec des valeurs valides dans 'gender'
    #res_final['gender'] = res_final['gender'].astype(str)


    try:
        if 'gender' in res_final.columns:
            h = res_final['gender'].value_counts()
            h = pd.DataFrame(h)
            h['Percentage'] = (h['count'] / len(original)) * 100
            print('Results distribution is as follows:\n', h)
        else:
            print("La colonne 'gender' est absente de filtered_res_final")
    except KeyError as e:
        print(f"Erreur: {e}")

    # Drop name_id at the very end
    res_final = res_final.drop(columns=['name_id'], errors='ignore')
    
    return res_final



