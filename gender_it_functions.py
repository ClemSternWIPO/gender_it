import pandas as pd
import requests
from io import StringIO
import string
from unidecode import unidecode
pd.options.mode.chained_assignment = None
import numpy as np

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
            data = pd.read_csv(path + 'd1.csv.giz', compression="gzip" ) ### find a way to change to local path
        except:
            print("downloading the dictionnary.")
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750348').content
            data = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
    if dictionnary == 2:
        try: 
            data = pd.read_csv(path + 'd2.csv.giz', compression="gzip" )
        except:
            print("downloading the dictionnary.")
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750350').content
            data = pd.read_csv(StringIO(s.decode('utf-8')),sep = ',')
    if dictionnary == 3:
        try: 
            data = pd.read_csv(path + 'd2.csv.giz', compression="gzip" ) 
        except:
            print("downloading the dictionnary.")
            s = requests.get('https://dataverse.harvard.edu/api/access/datafile/4750351').content
            data = pd.read_csv(StringIO(s.decode('utf-8')),sep = '\t')
    return data

def get_name_dataframe_treshold(df, name_column, country_column = False,  split = True, split_sep = ' ', treshold = 0.6):
    df = df.reset_index(drop=True) ### in case of multiple index
    df = df.reset_index(names = 'name_id') ### we need the name_id to reconnect to final results
    df['clean_name'] = df.apply(lambda x: multi_clean_name_function(x[name_column]), axis = 1 )
    df = df.explode('clean_name')
    df['clean_name_position'] = df.groupby('name_id').cumcount() + 1 ### each version of the cleaning is associated with a position, the smaller the more precise
    try:
        dfp = df [['clean_name','clean_name_position', country_column,'name_id']]
    except:
        dfp = df [['clean_name','clean_name_position','name_id']]
    if split == True:
        dfp['clean_name'] = dfp['clean_name'].str.split(split_sep)
        dfp = dfp.explode('clean_name')
    dfp['clean_name'] = dfp['clean_name'].astype(str)
    if country_column != False:
        dfp [country_column] = dfp [country_column].astype(str)
        dfp ['clean_country_column'] =  dfp[country_column].apply(lambda x: clean_country_function(x) )
    if split == True: ### Need to know the position of the surname, as first surname holds more meaning. 
        dfp['surname_position'] = dfp.groupby('name_id').cumcount() + 1
        if country_column != False:
            position = dfp[['clean_name','clean_country_column','surname_position','name_id']]
        else:
            position = dfp[['clean_name','surname_position','name_id']]
        del dfp['surname_position']
    else:
        if country_column != False:
            position = dfp[['clean_name','clean_country_column','name_id']]
        elif country_column == True:
            position = dfp[['clean_name','name_id']]
    if split == False and country_column != False:
        dfp['clean_country_column'] = dfp['clean_country_column'].fillna('NAN')
    if country_column != False:
        dfp_na = dfp[dfp['clean_country_column'] == 'NAN']#.drop_duplicates(subset = (name_column, 'clean_country_column')) ## direct to no code
        del dfp_na ['name_id']
        dfp_full = dfp[dfp['clean_country_column'] != 'NAN'].drop_duplicates(subset = ('clean_name', 'clean_country_column'))
        del dfp_full ['name_id']
        if len (dfp_na)>0:
            print (len(dfp_full), 'names-country pairs identified will be gendered first.', 
                   len(dfp_na), 'names are not associated with a country and will be gendered in last step.')    
        else :
            print (len(dfp_full), 'names-country pairs identified.')
    elif country_column == False:
        dfp_na = dfp.drop_duplicates(subset = ('clean_name'))
        del dfp_na['name_id']
        print (len(dfp_na), 'unique names to be identified.')
    #######################################################################################################################
    ############################################################################ FIRST TRY
    #######################################################################################################################
    if country_column != False:
        print('Step 1 - reading the name-country-gender dictionary')
        data = reading_wgnd (1, path)
        #print(len(data))
        data = data.rename(columns = {'name':name_column,'code':'clean_country_column'})### GET list of countries and names of interest
        #print(len(data))
        data = data [data[name_column].isin(list(dfp_full['clean_name']))]
        #print(len(data))
        data = data [data['clean_country_column'].isin(list(dfp_full['clean_country_column']))]
        #print(len(data))
        data = data.drop_duplicates(subset = (name_column,'clean_country_column',"gender"))
        cols = unique(data['gender'].tolist())
        data = data.pivot(index=[name_column,'clean_country_column'], columns="gender", values="wgt").reset_index()
        dfp_full = pd.merge(dfp_full, data, left_on= ('clean_country_column', 'clean_name'),right_on= ('clean_country_column', name_column), how = 'left')
        del dfp_full[name_column]
        res = dfp_full[(dfp_full[cols] > treshold).any(axis = 1)]
        res = res.sort_values('clean_name_position', ascending = True).drop_duplicates(['clean_country_column', 'clean_name'])
        errors = dfp_full[~(dfp_full[cols] > treshold).any(axis = 1)]
        #print(dfp_full.columns)
        del data### DISTINGUISH BETWEEN RESULTS AND ERRORS
        res['level'] = 1
        res= res.fillna(0)
        errors = errors[['clean_name', 'clean_country_column','clean_name_position']]
        res_two = pd.DataFrame() ## create an empty dfp to store results
        res_two = pd.concat([res_two, res])
        #print(1, res_final.columns)
        print (len(res), 'name-country pairs were found,', len(errors) , ' were not found and will be reexamined in step 2.')
        del res
        #######################################################################################################################
        ################################################################################# Second Try
        #######################################################################################################################
        print('Step 2 - reading the name-language-gender dictionary')
        data = reading_wgnd (2)
        data = data.rename(columns = {'name':name_column,'code':'clean_country_column'})### GET list of countries and names of interest
        data = data [data[name_column].isin(list(errors['clean_name']))]
        data = data [data['clean_country_column'].isin(list(errors['clean_country_column']))]
        data = data.drop_duplicates(subset = (name_column,'clean_country_column',"gender"))
        errors = pd.merge(errors,data, left_on= ('clean_country_column', 'clean_name'),right_on= ('clean_country_column', name_column), how = 'left')
        del errors[name_column]
        res = errors [~(errors ['gender'].isna())]
        errors = errors [errors ['gender'].isna()]
        errors = errors[['clean_name', 'clean_country_column','clean_name_position']]
        print(len(res),'additional names identified',len(errors),'names-country remains unfound, those names will be looked for in next step.')
        #dfp_na = pd.concat([dfp_na,errors])
        res['wgt'] = 1
        res = res.pivot(index=['clean_name','clean_country_column','clean_name_position'], columns="gender", values="wgt").reset_index()
        res['level'] = 2
        res = res.sort_values('clean_name_position', ascending = True).drop_duplicates(['clean_country_column', 'clean_name'])
        res_two = pd.concat([res_two,res])
        #print(2, res_final.columns)
        #print (len(res), 'additional name-country pairs were found,', len(errors) , ' were not found and will be reexamined in step 2.')
        del res_two ['clean_name_position']
        #print (res_two.columns)
        #del errors
        ############### MERGE FIRST AND SECOND ROUND OF RESULTS ##################################################################
        res_two =  res_two.merge(position, on = ('clean_country_column','clean_name'))
        if split == True:
            res_two =  res_two.sort_values('surname_position',ascending = True).drop_duplicates(subset = 'name_id', keep = 'first' )
            del res_two['surname_position']
            del res_two['clean_name']
        #######################################################################################################################
        ######################################################################################### LAST CHANCE / Direct try
        ########################################################################################################################
    print('Step 3 - reading the name-gender dictionary.')
    data = reading_wgnd (3)
    data = data.rename(columns = {'name':name_column})
    ##### Filter on relevant data
    data = data [data[name_column].isin(list(dfp_na['clean_name']))]
    data = data.drop_duplicates(subset = (name_column,"gender"))
    res = pd.merge(dfp_na, data, left_on= ( 'clean_name'),right_on= ( name_column))
    if country_column != False:
        errors = errors.merge(data, left_on= ( 'clean_name'),right_on= ( name_column))
        res = pd.concat([res, errors])
    del res[name_column]
    res['wgt'] = 1
    if country_column != False:
        res = res.sort_values('clean_name_position', ascending = True).drop_duplicates(subset = ('clean_name','clean_country_column'))
        del res ['clean_name_position']
        res = res.pivot(index=['clean_name','clean_country_column'], columns="gender", values="wgt").reset_index()
    else:
        res = res.sort_values('clean_name_position', ascending = True).drop_duplicates(subset = 'clean_name')
        del res ['clean_name_position']
        res = res.pivot(index=['clean_name'], columns="gender", values="wgt").reset_index()
    res ['level'] = 3
    del data
    print (len(res), 'names were found in the last dictionnary.')#, len(dfp_na) - len(res) , 'remain unfound.')
    del dfp_na
    ############### MERGE LAST ROUND OF RESULTS ##################################################################
    if country_column != False:
        res = res.merge(position, on = ('clean_country_column','clean_name'))
    else:
        res = res.merge(position, on = ('clean_name'))
        #print(len(res))
    if split == True:
        res = res.sort_values('surname_position',ascending = True).drop_duplicates(subset = 'name_id', keep = 'first' )
        del res['surname_position']
    #######################################################################################################################
    ######################################################################################### MERGING RESULTS
    ########################################################################################################################
    try:
        res_final = pd.concat([res,res_two])
        #print('a', res_final.columns)
    except:
        res_final = res
        del res
    #print (res_final.columns)
    try:
        del res_final ['clean_name']
        del res_final ['clean_country_column']
        del res_final [country_column]
    except:
        pass
    try:
        del res_final['surname_position']
    except:
        pass
    res_final = df.merge(res_final, on = ('name_id'), how = 'left' )#.fillna('not found')
    #print(len(res_final), res_final.columns)
    #print (res_final.columns)
    try:
        del res_final['surname_position']
    except:
        pass
    not_found = res_final[res_final['level'].isna()]
    found = res_final[~(res_final['level'].isna())]
    cols = []
    try :
        not_found ['F']= not_found['F'].fillna('not found')
        found ['F']= found['F'].fillna(0)
        cols.append('F')
    except:
        pass
    try :
        not_found ['M']= not_found['M'].fillna('not found')
        found ['M']= found['M'].fillna(0)
        cols.append('M')
    except:
        pass
    try:
        not_found ['?']= not_found['?'].fillna('not found')
        found ['?']= found['?'].fillna(0)
        cols.append('?')
    except:
        pass
    found['gender'] = found[cols].idxmax(axis=1)
    res_final = pd.concat([not_found, found])
    res_final = res_final.sort_values('name_id', ascending = True)
    s =  res_final  ['name_id']
    res_final.set_index( [s, s/s])
    del res_final  ['name_id']
    res_final['gender'] = res_final['gender'].fillna('not found')
    h = res_final['gender'].value_counts()
    try:
        del res_final['clean_name']
        del res_final['clean_name_position']
    except:
        pass
    print ('Results distirbution is as follow:','\n',h)
    print(len(res_final[res_final['gender'] == 'M']),'rows were identified as ')
    return res_final

