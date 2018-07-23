#*******************************************************************************************
# Python File Utilities
#*******************************************************************************************

#-------------------------------------------------------------------------------------------
# Dependencies
#-------------------------------------------------------------------------------------------

# Standard Python modules
import os
from os import listdir
from os.path import isfile, join
from urllib.parse import urlparse,quote, unquote

import re
from itertools import islice
import csv

# Data Frames
import pandas as pd

#*******************************************************************************************
# Part 1: Files, Paths, Directories, Readers, Writers & Parsers
#*******************************************************************************************

#-------------------------------------------------------------------------------------------
# File Tools
#-------------------------------------------------------------------------------------------

def files_in_dir(dir):
    return [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

#-------------------------------------------------------------------------------------------
# Pathname Name Tools
#-------------------------------------------------------------------------------------------

def make_dir_pathname (file, subdir='', dir=''):
    return os.path.join(dir, subdir, file)
  
#-------------------------------------------------------------------------------------------

def make_sub_filename(output_file, postfix_name, ext='csv'):
    dirname, filename = os.path.split(output_file)
    name, ext = filename.split('.')
    return dirname + "/" + name + "-" + postfix_name + "." + ext

#-------------------------------------------------------------------------------------------
# Loadind and Saving text files
#-------------------------------------------------------------------------------------------

def load_text_file (file, encoding='utf-8'):
    f = open(file, 'r',  encoding='utf-8')
    text = f.read()
    f.close()
    return text

#-------------------------------------------------------------------------------------------

# Returns a list with the text content of each file. Tex

def load_text_directory(dir):
    files = files_in_dir(dir)
    return [load_text_file(f) for f in files]

#-------------------------------------------------------------------------------------------

# Python 3.6 supposedly handles unicode natively. We'll see. 

def save_text_file (lines, file):
    f = open(file,'w', encoding='utf-8')
    for line in lines:
        f.write(line)
    f.close()
    return True

#-------------------------------------------------------------------------------------------

# def remove_undecodable_words (words):
#     new_words = []
#     for w in words:
#         try:
#             print(w)
#             new_words.append(w)
#         except Exception:
#             x = -1
#     return new_words


#-------------------------------------------------------------------------------------------
# Loadind and Saving CSV files
#-------------------------------------------------------------------------------------------

# Mode allows the return value to be either a list of rows with columns or just a list
# of strings.

def quoted_field_p (field):
    return (field[1]=='"') and (field[-1]=='"')

#-------------------------------------------------------------------------------------------

def load_csv_file (file, mode='rows', encoding='utf-8'):
    rows = []
    with open(file, mode='rU', encoding=encoding) as csvfile:
        freader = csv.reader(csvfile, delimiter=',', quotechar='|')
        count = 0
        if mode=='text':
            try:
                for row in freader:
                    row = [field[1 : -1] if quoted_field_p(field) else field for field in row ]
                    rows += row
                    count += 1
            except Exception as e:
                print ('Error on row number: ' + str(count))
                print ('Error msg: ' + str(e))
        else:
            try:
                for row in freader:
                    rows.append(row)
                    count += 1
            except Exception as e:
                print ('Error on row number: ' + str(count))
                print ('Error msg: ' + str(e))
    return rows

#--------------------------------------------------------------------------------------

def load_csv_dataframe  (file, mode='rows', encoding='utf-8'):
    rows = load_csv_file(file, mode='rows', encoding='utf-8')
    return pd.DataFrame(rows[1:], columns = rows[0])

#--------------------------------------------------------------------------------------

# Python 3.6 supposedly handles unicode natively. We'll see. 

# Lines can be a list of strings and will be converted to single column format.

def save_csv_file (lines, file, encoding='utf-8'):
    with open(file, 'w', newline='\n', encoding=encoding) as csvfile:
        fwriter = csv.writer(csvfile, delimiter=',', quotechar='|',
                             quoting=csv.QUOTE_MINIMAL)
        for line in lines:
            # Allow strings as single column
            if type(line)==str:
                line = [line]
            fwriter.writerow(line)
    return True

#-------------------------------------------------------------------------------------
# Loading and Saving Excel files into Pandas DataFrames
#-------------------------------------------------------------------------------------

lexicon_classified_file = 'c:\\Projects\\Python\\sentiment\\data\\lexicon_classified.xlsx'

# Returns a Pandas Data Frame

def load_excel_file(file):
    xls_file = pd.ExcelFile(file)
    df = xls_file.parse(0)
    return df
 
#--------------------------------------------------------------------------------------

def save_excel_file(df, file, sheet_name='PySheet'):
    writer = pd.ExcelWriter(file, engine='xlsxwriter')
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()
    return True

#--------------------------------------------------------------------------------------

def save_list_to_excel(l, file, sheet_name='PySheet', sort=False):
    if sort==True:
        l.sort()
    df = pd.DataFrame(l)
    save_excel_file(df, file, sheet_name)
    return True

#--------------------------------------------------------------------------------------
# Loading PDF Files
#--------------------------------------------------------------------------------------

# Code copied from:
# https://stackoverflow.com/questions/26494211/extracting-text-from-a-pdf-file-using-pdfminer-in-python
# Install pdfminer using: pip install git+https://github.com/pdfminer/pdfminer.six.git
# NB: Needed to change StringIO import statement to import from io in Python 3

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams

from io import StringIO

# Upgraded to handle exceptions of type NameError when text extraction not allowed:
#
# In [49]: PDFTextExtractionNotAllowed
# ---------------------------------------------------------------------------
# NameError                                 Traceback (most recent call last)
# <ipython-input-49-3103147440c4> in <module>()
# ----> 1 PDFTextExtractionNotAllowed

def load_pdf_file(pdfname):

    try:
        # PDFMiner boilerplate
        rsrcmgr = PDFResourceManager()
        sio = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, sio, codec=codec, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        
        # Extract text
        fp = open(pdfname, 'rb')
        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
        fp.close()
            
        # Get text from StringIO
        text = sio.getvalue()
            
        # Cleanup
        device.close()
        sio.close()

        return text
    except Exception:
        return ''

#-------------------------------------------------------------------------------------------

def load_pdf_directory(dir):
    files = files_in_dir(dir)
    return [_pdf_file(f) for f in files]

#-------------------------------------------------------------------------------------------
# Dowloading Files
#-------------------------------------------------------------------------------------------

# NB: Borrowed from slack.com, not yet tested.

def download_file(download_url):
   web_file = urllib.urlopen(download_url)
   local_file = open('some_file.pdf', 'w')
   local_file.write(web_file.read())
   web_file.close()
   local_file.close()
   
#-------------------------------------------------------------------------------------------
# Parsers
#-------------------------------------------------------------------------------------------

def parse_text (text, delimiter='.', remove=['\n']):
    # Build a dijunctive pattern of chars to match 
    reg = '[' + '|'.join(remove) + ']'
    # Removing matching chars
    text = re.sub(reg, '', text)
    # Split text into sentences
    sentences = text.split(delimiter)
    return(sentences)
    
#-------------------------------------------------------------------------------------------

def parse_text_file (pathname, delimiter='.', remove=[]):
    text = load_csv_file(pathname, mode='text')
    return parse_text (text, delimiter, remove)
    
#-------------------------------------------------------------------------------------------

def parse_pdf_file (pathname, delimiter='.', remove=['\n']):
    text = load_pdf_file(pathname)
    return parse_text (text, delimiter, remove)
  
#-------------------------------------------------------------------------------------------

def parse_pdf_directory (dir, delimiter='.', remove=['\n']):
    files = files_in_dir(dir)
    sentences = []
    for file in files:
        sentences = sentences + parse_pdf_file(file)
    return sentences

#-------------------------------------------------------------------------------------------
# End of File
#-------------------------------------------------------------------------------------------

