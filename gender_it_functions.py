import pandas as pd
import requests
from io import StringIO
import string
from unidecode import unidecode
pd.options.mode.chained_assignment = None
import numpy as np
import re
import unicodedata as ud

def read_wgnd(path = False, All = True):
    if  path == False and All == True :
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750348').content
        d1 = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
        print('saving first dictionnary.')
        d1.to_csv("d1.csv.gz", index=False, compression="gzip") 
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750350').content
        d2 = pd.read_csv(StringIO(s.decode('utf-8')),sep = ',')
        d2.to_csv( "d2.csv.gz", index=False, compression="gzip") 
        print('saving second dictionnary.')
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
        d3 = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
        d3.to_csv("d3.csv.gz", index=False, compression="gzip")
        print('saving third dictionnary.')
    elif path != False and All == True:
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750348').content
        d1 = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
        print('saving first dictionnary.')
        d1.to_csv(path + "d1.csv.gz", index=False, compression="gzip") 
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750350').content
        d2 = pd.read_csv(StringIO(s.decode('utf-8')),sep = ',')
        d2.to_csv(path + "d2.csv.gz", index=False, compression="gzip") 
        print('saving second dictionnary.')
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
        d3 = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
        d3.to_csv(path + "d3.csv.gz", index=False, compression="gzip")
        print('saving third dictionnary.')
    elif All != True:
        s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
        d3 = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
        d3.to_csv(path + "d3.csv.gz", index=False, compression="gzip")
    print ('All dictionaries saved!')

    
    
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
            #data = pd.concat(map(pd.read_csv, [path + 'd2_1.csv.gz',path + 'd2_2.csv.gz',path + 'd2_3.csv.gz']))
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



def get_gender(df, name_column, country_column=False, split=True, split_sep=' ', threshold=0.6, path='gender_it/dictionaries/', first_name=True):
    # Reset index to handle potential multi-index DataFrame and add a unique identifier for each row
    df = df.reset_index(drop=True).reset_index(names='name_id')
    null_lines = df[count_missing_values(df, name_column)].shape[0]
    print(f'WARNING: {null_lines} without names.')

    # Store a copy of the original DataFrame for merging the results later
    original = df.copy()

    # Select relevant columns, include the country column if provided
    if country_column:
        df = df[['name_id', name_column, country_column]]
    else:
        df = df[['name_id', name_column]]

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
        pattern = r'^[A-Z]{2}$'
        dff = df[df[country_column].str.upper().str.match(pattern, na=False)].copy()
        dff[country_column] = dff[country_column].astype(str)
        dff['clean_country_column'] = dff[country_column].apply(clean_country_function)
        dff = dff.drop(columns=[country_column])  # Remove the original country column
        dfn = df[~df[country_column].str.upper().str.match(pattern, na=False)].copy()
        dfn = dfn.drop(columns=[country_column])
        print('WARNING:', len(dfn['name_id'].unique()), 'without country_codes.')
    else:
        dfn = df.copy()

    # Initialize cols for later use
    cols = []
    
    # Step 1: Try to find gender using the name-country-gender dictionary
    found = pd.DataFrame()
    if country_column:
        print('Step 1 - Reading the name-country-gender dictionary')
        data = reading_wgnd(1, path)
        data = data.rename(columns={'name': 'clean_name', 'code': 'clean_country_column'})
        
        data = data[data['clean_name'].isin(dff['clean_name'])]
        data = data[data['clean_country_column'].isin(dff['clean_country_column'])]
        
        data = data.drop_duplicates(subset=['clean_name', 'clean_country_column'])
        data = data.pivot(index=['clean_name', 'clean_country_column'], columns="gender", values="wgt").reset_index()

        found = data.merge(dff, on=['clean_name', 'clean_country_column'])
        cols = list(data.columns[2:])
        found = found[(found[cols] > threshold).any(axis=1)]
        
        found = found.sort_values('surname_position', ascending=first_name).drop_duplicates(subset='name_id')
        found['level'] = 1
        found = found.fillna(0)

        dff = dff[~dff['name_id'].isin(found['name_id'])]

    # Step 2: Attempt to find gender using the name-language-gender dictionary
    if country_column:
        print('Step 2 - Reading the name-language-gender dictionary')
        data = reading_wgnd(2, path)
        data = data.rename(columns={'name': 'clean_name', 'code': 'clean_country_column'})
        
        data = data[data['clean_name'].isin(dff['clean_name'])]
        data = data[data['clean_country_column'].isin(dff['clean_country_column'])]
        
        data = data.drop_duplicates(subset=['clean_name', 'clean_country_column'])
        res = data.merge(dff, on=['clean_name', 'clean_country_column'])
        
        res = res.sort_values('surname_position', ascending=first_name).drop_duplicates(subset='name_id')
        res['wgt'] = 1
        res = res.pivot(index=['clean_name', 'clean_country_column', 'name_id'], columns="gender", values="wgt").reset_index()
        res['level'] = 2

        found = pd.concat([found, res])
        dff = dff[~dff['name_id'].isin(found['name_id'])]

    # Step 3: Directly attempt to find gender using the name-gender dictionary
    print('Step 3 - Reading the name-gender dictionary')
    data = reading_wgnd(3, path)
    data = data.rename(columns={'name': 'clean_name'})

    if len(dff) > 0:
        dfn = pd.concat([dff, dfn])
        dfn = dfn.drop_duplicates(subset='name_id')
    data = data[data['clean_name'].isin(dfn['clean_name'])]

    res = data.merge(dfn, on='clean_name', how='inner')
    res = res.sort_values('surname_position', ascending=first_name).drop_duplicates(subset='name_id')
    res['wgt'] = 1

    try:
        res = res.pivot(index=['name_id', 'clean_name', 'clean_country_column'], columns="gender", values="wgt").reset_index()
    except KeyError:
        res = res.pivot(index=['name_id', 'clean_name'], columns="gender", values="wgt").reset_index()

    res['level'] = 3

    found = pd.concat([found, res], ignore_index=True)

    not_found = dfn[~dfn['name_id'].isin(found['name_id'])]
    not_found = not_found.drop_duplicates(subset='name_id')
    
    for gender_col in ['F', 'M', '?']:
        if gender_col in found.columns:
            found[gender_col] = found[gender_col].fillna(0)
            not_found[gender_col] = 0
            if gender_col not in cols:
                cols.append(gender_col)

    not_found['level'] = 3
    not_found['gender'] = 'not found'

    found['gender'] = found[cols].idxmax(axis=1)
    
    result_columns = ['name_id', 'level', 'gender'] + cols
    if 'clean_name' in found.columns:
        result_columns.append('clean_name')
    if 'clean_country_column' in found.columns:
        result_columns.append('clean_country_column')
    if 'surname_position' in found.columns:
        result_columns.append('surname_position')

    for col in result_columns:
        if col not in not_found.columns and col not in ['gender', 'level'] + cols:
            not_found[col] = None

    found = found[result_columns]
    not_found = not_found[result_columns]
    
    res_final = pd.concat([found, not_found])
    
    res_final = res_final.merge(original, on='name_id', how='right')
    
    res_final = res_final.drop(columns=['clean_name', 'clean_country_column', 'surname_position'], errors='ignore')

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

    res_final = res_final.drop(columns=['name_id'], errors='ignore')
    
    return res_final

