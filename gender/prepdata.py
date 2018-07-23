#---------------------------------------------------------------------------
# Prepare Raw Data Girly and Boy names for Gender Attribution Code
#---------------------------------------------------------------------------

import os
import sys
import csv

import pandas as pd
from os import listdir
from os.path import isfile, join

raw_names_dir =  join (os.environ['GOZUP'], '..', 'libs', 'newgender', 'rawdata')
english_names_dir = join (raw_names_dir, 'english')

english_first_names = join (english_names_dir, 'first_names.csv')
english_last_names =  join (english_names_dir, 'last_names.csv')
english_first_name_cols = ['name', 'gender', 'count']

top_girl_names = join (english_names_dir, 'TopGirlNames1000.csv')
top_boy_names = join (english_names_dir, 'TopBoyNames1000.csv')

french_first_names = join(raw_names_dir, 'prenoms.txt')

names_dir = join (os.environ['GOZUP'], '..', 'libs', 'newgender')
girls_file = join(names_dir, "FirstNamesGirls.csv")
boys_file = join(names_dir, "FirstNamesBoys.csv")
last_file = join(names_dir, "LastNames.csv")
                  
#---------------------------------------------------------------------------

def files_in_dir(dir):
    return [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

#---------------------------------------------------------------------------

def load_csv_dataframe  (file, mode='rows', encoding='utf-8', delimiter=',',cols=None):
    return pd.read_csv(file, sep=delimiter, names=cols)

#---------------------------------------------------------------------------

# Loads each file in <files> as a cvs into a DF, then concatenatesthe DFs.

def load_files_as_df(files, cols):
    df_list = []
    for f in files:
        df = load_csv_dataframe(f, cols=cols)
        df_list.append(df)
    full_df = pd.concat(df_list, axis=0)
    return full_df

#---------------------------------------------------------------------------

def load_english_first_names ():
    return pd.read_csv(english_first_names, names=english_first_name_cols)

#---------------------------------------------------------------------------

def load_english_last_names ():
    return pd.read_csv(english_last_names)

#---------------------------------------------------------------------------

# Data source: https://www.babble.com/pregnancy/1000-most-popular-girl-names/
def load_top_girl_names ():
    return pd.read_csv(top_girl_names, names=['name'])

#---------------------------------------------------------------------------

# Data source: https://www.babble.com/pregnancy/1000-most-popular-boy-names/
def load_top_boy_names ():
    return pd.read_csv(top_boy_names, names=['name'])

#---------------------------------------------------------------------------
# Extract, Dedup, Filter Nmaes
#---------------------------------------------------------------------------

default_threshold = 1000

def english_names (df, gender, threshold=default_threshold):
    gdf = df.loc[df['gender']==gender]
    gdf = gdf.drop(['gender'], axis=1)
    gdf = gdf.groupby(['name'], as_index=False).sum()
    gdf = gdf.loc[gdf['count'] >= threshold]
    return gdf.drop(['count'], axis=1)

#---------------------------------------------------------------------------

def english_girl_names(df, top_boys, threshold=default_threshold):
    # Extract girl names from all names df.
    all_girls =  english_names(df, 'F', threshold)
    # Remove top boys from girls list
    filtered_names = list(set(list(all_girls['name'])) - set(list(top_boys['name'])))
    return pd.DataFrame (filtered_names, columns=['name'])

#---------------------------------------------------------------------------

def english_boy_names(df, top_girls, threshold=default_threshold):
   # Extract boy names from all names df.
    all_boys = english_names(df, 'M', threshold)
    # Remove top boys from girls list
    filtered_names = list(set(list(all_boys['name'])) - set(list(top_girls['name'])))
    return pd.DataFrame (filtered_names, columns=['name'])
    
#---------------------------------------------------------------------------
# Save Boy & Girl Names
#---------------------------------------------------------------------------

def save_girl_names(df):
    df = df.sort_values(['name'])
    df.to_csv(girls_file, header=False, index=False)

#---------------------------------------------------------------------------

def save_boy_names(df):
    df = df.sort_values(['name'])
    df.to_csv(boys_file, header=False, index=False)

#---------------------------------------------------------------------------

def save_last_names(df):
    df = df.sort_values(['lastname'])
    df.to_csv(last_file, header=False, index=False)

#----------------------------------------------------------------------------
# Test overlap
#----------------------------------------------------------------------------

# NB: A threshold of 1,500 yields about 5,000 girls and 3,000 boys with an
# overlap of only 250.

def test_overlap (threshold=1500):
    all_first_names = load_english_first_names()
    top_girls = load_top_girl_names ()
    top_boys = load_top_boy_names ()
    girl_first_names = english_girl_names(all_first_names, top_boys, threshold)
    boy_first_names = english_boy_names(all_first_names, top_girls, threshold)
    girl_set = set(list(girl_first_names['name']))
    boy_set = set(list(boy_first_names['name']))
    ambi_set = list(girl_set.intersection (boy_set))
    ambi_set.sort()
    return ambi_set , len(ambi_set)

#----------------------------------------------------------------------------
# Main
#----------------------------------------------------------------------------
    
def main():
    threshold = int(sys.argv[1])
    all_first_names = load_english_first_names()
    top_girls = load_top_girl_names ()
    top_boys = load_top_boy_names ()
    # Boy names
    girl_first_names = english_girl_names(all_first_names, top_boys, threshold)
    save_girl_names(girl_first_names)
    # Boy names
    boy_first_names = english_boy_names(all_first_names, top_girls, threshold)
    save_boy_names(boy_first_names)
    # Last names
    last_names = load_english_last_names()
    save_last_names(last_names)
    
#----------------------------------------------------------------------------

if __name__== "__main__":
  main()

#---------------------------------------------------------------------------
# End of File
#---------------------------------------------------------------------------

