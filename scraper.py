#********************************************************************
# WEBPAGE SCRAPING MODULE
#
# Part 0: NLP Tools
# Part 1: Generic web scraping tools
#
#********************************************************************


#--------------------------------------------------------------------
# All Imports 
#--------------------------------------------------------------------

# Standard Python
import os
import sys
from urllib.parse import urlparse,quote, unquote

# Data Frames
import pandas as pd

# Web tools
import requests
from bs4 import BeautifulSoup, SoupStrainer


# NLTK Dependencies
# Note: Need to run nltk.dowload() manually to download predefined corpuses
# TODO: Persist corpuses to avoid manual download.

import nltk
import nltk.classify.util
from nltk.corpus import names
from nltk.corpus import stopwords
from tabulate import tabulate

#--------------------------------------------------------------------
# Project Directory
#--------------------------------------------------------------------

def get_project_dir():
    try:
        return os.environ['PROJECT_DIR']
    except KeyError:
       print ("Error: Unable to access environment variable PROJECT_DIR.")
       return None

#-------------------------------------------------------------------------------------------

PROJECT_DIR = get_project_dir()

#********************************************************************
# Part 0: NLP Tools
#********************************************************************

#-------------------------------------------------------------------------------------------
# Stopwords
#-------------------------------------------------------------------------------------------
    
# Pre-defined stopwords from the NLTK corpus. Currently including both French and Enlglish
# Will most likely separate later

# stopWords = set().union(stopwords.words('english'), stopwords.words('french'))
# stopWords = stopwords.words('english')

# French StopWords not in NLTK

additional_french_stopwords = ['a', 'les', 'plus', 'comme', 'ils', 'tout', 'si', 'tous',
                               'cela', 'celle', 'celui']

en_stops =  stopwords.words('english')
fr_stops = stopwords.words('french')

stopWords = en_stops + fr_stops + additional_french_stopwords

def show_stopwords(stopwords= stopWords):
    for x in stopwords:
        print(unquote(x))

#-------------------------------------------------------------------------------------------
# Aphabetic words
#-------------------------------------------------------------------------------------------

# Returns True if word only contains alpabetic characters.

def alphabetic_word_p(word):
    m =  re.match(r'[^\W\d]*$', word)
    if m == None:
        return False
    else:
        return True

#-------------------------------------------------------------------------------------------

# Tokenize text and apply filters.
# TODO: Clarify logic below and use filter function from functional tools library.

def tokenize_text(text, remove_stopwords=True, alphabetic_only=True ):
    tokens = nltk.word_tokenize(text.lower())
    tokensFiltered = []
    for token in tokens:
        alpha_word_p = alphabetic_word_p(token)
        stop_word_p = token in stopWords
        condition_1 = remove_stopwords==False or not stop_word_p
        condition_2 = alphabetic_only==False or alpha_word_p
        if condition_1 and condition_2:
            tokensFiltered.append(token)
    return tokensFiltered

#-------------------------------------------------------------------------------------------
# Clean Sentence
#-------------------------------------------------------------------------------------------

# Defined for convenience. Tokenizes then the reasembles.

def clean_sentence (s):
    tokens = tokenize_text(s)
    return ' '.join(tokens)

#********************************************************************
# Part 1: Generic web scraping tools*
#********************************************************************

# Returns a response object

def get_url_response (url):
    try:
        return requests.get(url)
    except Exception:
        return None

#--------------------------------------------------------------------

# Extracts the content of the response of a request.

def get_url_data (url):
    response = get_url_response(url)
    if response==None:
        return None
    else:
        return response.content

#--------------------------------------------------------------------

def link_contains_stop_word (link, stop_words):
    result = False
    for x in stop_words:
        if x in link['href']:
            result = True
    return result

#--------------------------------------------------------------------
# URL Predicates
#--------------------------------------------------------------------

def full_url_p (url):
    return ('http://' in url) or ('https://' in url)

#--------------------------------------------------------------------

def make_full_url (url, full_url):
    urlcomps = urlparse(full_url)
    return urlcomps.scheme + '://' + urlcomps.netloc + url

#--------------------------------------------------------------------'

def same_domain_p (url1, url2):
    comps1 = urlparse(url1)
    comps2 = urlparse(url2)
    return comps1.netloc==comps2.netloc

#--------------------------------------------------------------------'

def make_domain_url (url):
    comps = urlparse(url)
    return comps.scheme + '://' + comps.netloc + '/'

#--------------------------------------------------------------------

def internal_link_p (url, site_url):
    return same_domain_p (url, site_url)

#--------------------------------------------------------------------
# URL Extraction.
#--------------------------------------------------------------------

def extract_urls (url, filter='', stop_words=[]):
    response = get_url_response(url)
    urls = []
    if response is not None:
        soup = BeautifulSoup(response.content, 'lxml')
        for link in soup.find_all('a', href=True):
            if filter in link['href'] and not link_contains_stop_word (link, stop_words):
                urls.append(unquote(link['href']))
                urls = list(set(urls))
    return urls

#--------------------------------------------------------------------

def extract_full_urls(url, root_url, filter='', stop_words=[]):
    urls = extract_urls(url, filter, stop_words)
    full_urls = []
    for u in urls:
        if not full_url_p(u):
            full_url = make_full_url(u, root_url)
        else:
            full_url = u
        full_urls.append(full_url)
    return full_urls
 
#--------------------------------------------------------------------

def extract_internal_urls(url, root_url, filter='', stop_words=[]):
    urls = extract_full_urls(url, root_url, filter, stop_words)
    result = []
    for u in urls:
        if internal_link_p(u, root_url) == True:
            result.append(u)
    return(result)

#--------------------------------------------------------------------

def extract_text(url):
    content = get_url_data(url)
    text = []
    if content is not None:
        soup = BeautifulSoup(content, 'lxml')
        for x in soup.find_all('p'):
            text.append(x.get_text())
        text = [unquote(x)  for x in text if len(x) > 2]
    return text

#--------------------------------------------------------------------

def extract_clean_text (url):
    text = extract_text(url)
    clean = [clean_sentence(x) for x in text]
    filtered = [x for x in clean if len(x.split(' ')) > 1]
    return filtered

#********************************************************************
# Part 2: CRIF scraping tools
#********************************************************************

def make_scrapesites_pathname (file, dir=PROJECT_DIR):
    return ct.make_crif_pathname(file, 'scrapesites', dir)

#-------------------------------------------------------------------------------------------
# Classification predicates
#-------------------------------------------------------------------------------------------

def positive_classification_p(classification):
    return classification.lower()=='positive' or classification.lower()=='positif'

#-------------------------------------------------------------------------------------------

def negative_classification_p(classification):
    return classification.lower()=='negative' or classification.lower()=='negatif'

def print_urls(urls):
    for url in urls:
        print (url)

#------------------------------------------------------------------------------------------
# Load & Save Scraped sentences File
#------------------------------------------------------------------------------------------

# TODO: Verify column headers and ensure they match expected headers.

scraper_input_columns = ['description', 'url', 'classification',
                         'follow_links', 'crawl_website',
                         'year', 'type']

scraper_output_columns  = ['source', 'text', 'classification', 'url', 'type']

#------------------------------------------------------------------------------------------

def load_websites_file (websites_file='websites_list.xlsx'):
    pathname = ct.make_lists_pathname (websites_file)
    df = ct.load_excel_file (pathname)
    return df

#------------------------------------------------------------------------------------------

def save_websites_sentences (rows, file):
    pathname = ct.make_local_data_pathname(file)
    df = pd.DataFrame(rows, columns=scraper_output_columns)
    return ct.save_excel_file (df, file)

#------------------------------------------------------------------------------------------

def make_webite_sentences_pathname (file_name, suffix):
    dirname, filename = os.path.split(file_name)
    name, ext = filename.split('.')
    return ct.make_local_data_pathname(name + suffix + "." + ext)

#------------------------------------------------------------------------------------------

# This saves the sentences for a particular website in Excel format

def save_website_sentences (rows, filename, count=0):
    pathname = make_webite_sentences_pathname (filename, '_' + str(count))
    df = pd.DataFrame(rows, columns=scraper_output_columns)
    return ct.save_excel_file (df, pathname)

#------------------------------------------------------------------------------------------

websites_sentences_file = 'websites_sentences.csv'

# This loads the sentences for all websites from a csv file.

def load_websites_sentences (websites_file=websites_sentences_file):
    pathname = ct.make_local_data_pathname (websites_file)
    df = pd.read_csv (pathname)
    return df

#------------------------------------------------------------------------------------------

def merge_websites_files(output_file=websites_sentences_file):
    print ("Merging website files into big data...")
    dir = ct.make_local_data_pathname('')
    files = ct.files_in_dir(dir)
    files = [f for f in files if 'website_sentences' in f]
    full_df = pd.DataFrame(ct.load_excel_file(files[0]))
    
    for f in files[1:]:
        df = pd.DataFrame(ct.load_excel_file(f))
        full_df = pd.concat([full_df, df], axis=0)
        
    # HACK: Cleanup column names:
    full_df['temp'] = full_df['source']
    full_df['source'] = full_df['text']
    full_df['text'] = full_df['temp']
    full_df = full_df.drop(['temp'], axis=1)
    
    output_pathname = ct.make_local_data_pathname(output_file)
    full_df.to_csv(output_pathname, encoding='utf-8', index=False)
    #ct.save_excel_file(full_df, output_pathname)
    return True

#------------------------------------------------------------------------------------------
# CRAWL WEBSITE
#------------------------------------------------------------------------------------------

# ['source', 'text', 'classification', 'url', 'type']

def make_output_row(sentence, row, url):
    return ['website', sentence, row['classification'], url, row['type']]

def make_output_rows(sentences, row, url):
    rows = []
    for s in sentences:
        if len(s) > ct.MIN_SENTENCE_LEN:
            rows.append(make_output_row(s, row, url))
    return rows

#------------------------------------------------------------------------------------------

def limit_reached_p (n, limit):
    if limit == -1:
        return False
    elif n > limit:
        return True
    else:
        return False
#------------------------------------------------------------------------------------------

def crawl_website (root_url, limit=5000):
    print ('\nCrawling website: ' + root_url)
    urlcomps = urlparse(root_url)
    processed = []
    remaining = [root_url]
    results = []
    page_count = 0
    while remaining and not limit_reached_p (page_count, limit):
        url = remaining.pop()
        if url not in processed:
            if page_count%250==0:
                print ('Website pages visited: ' + str(page_count))
            page_count += 1
            
            text = extract_clean_text(url)
            # print ('URL: ', url)
            # print ('Text: '+ str(len(text)))
            # print ('Chars: ' + str(list(map (len, text))))
                   
            results.append ([url, text])
            processed.append(url)
            
            urls = extract_full_urls(url, root_url, filter='')
            #print ('Urls count: ' + str(len(urls)))
            next_urls = [u for u in urls if internal_link_p(u, root_url)]
            #print ('Next Urls count: ' + str(len(next_urls)))
            remaining += next_urls
            
    print ('Website pages: ', str(page_count))
    return results, page_count

#------------------------------------------------------------------------------------------

# Turns crawler results into individual rows of sentences.

def process_crawler_results (results, row):
    rows = []
    for [url, sentences] in results:
        for s in sentences:
            if len(s) > ct.MIN_SENTENCE_LEN:
                rows.append (make_output_row(s, row, url))
    return rows

# Assumes following colums are present:
#
# ['Description', 'url', 'classification', 'follow_links', 'crawl_website', 'year', 'type']

#------------------------------------------------------------------------------------------
# PROCESSING MESSAGES
#------------------------------------------------------------------------------------------

def print_processing_msg (row):
    print ('\n------------------------------------------------------')
    print ('Processing row: ')
    print (str(row))
    print ('------------------------------------------------------')
 
#------------------------------------------------------------------------------------------

def print_totals_msg (rows, page_count):
    print ("----------------------------------------------------------------------")
    print ("Website pages visited: " + str(page_count))
    print ("Website rows generated: " + str(len(rows)))
    print ("----------------------------------------------------------------------")
 
#------------------------------------------------------------------------------------------

def print_all_totals_msg (all_rows, total_pages):
    print ("----------------------------------------------------------------------")
    print ("Total pages visited: " + str(total_pages))
    print ("Total rows processed: " + str(len(all_rows)))
    print ("----------------------------------------------------------------------")

#------------------------------------------------------------------------------------------
# PROCESS SINGLE ROW
#------------------------------------------------------------------------------------------

def process_website_row (row, limit=2000):

    try:
        # Scrape specified url page
        url = row['url']
        root_url = make_domain_url(url)
        text = extract_clean_text (url)
        rows = make_output_rows(text, row, url)
        page_count = 1
        # Follow or crawl if requested
        follow = row['follow_links']
        crawl = row['crawl_website']
    
        if follow.lower()=='yes':
            print ('Following links...')
            links = extract_internal_urls(url, root_url) 
            for link in links:
                print ('Following: ' + str(link))
                text = extract_clean_text(link)
                rows += make_output_rows(text, row, url)
            page_count += len(links)
        elif crawl.lower()=='yes':
            results, page_count = crawl_website(root_url, limit=2000)
            rows += process_crawler_results(results, row)
    
        # Return the list of scraped sentences
        return rows, page_count
    except Exception:
        return [], 0
    
#------------------------------------------------------------------------------------------
# Process Websites File
#------------------------------------------------------------------------------------------

follow_url = 'http://rootsisrael.com/'
sample_url = 'http://www.crif.com/'

list_file = 'websites_list.xlsx'
sentences_file = 'website_sentences.xlsx'

# This reads the websites file that is in Google Drive, follows the urls in the file,
# exracts the text and saves it in an excel document to be used as input for the word
# embeddings module

# Input colums: 'description', 'url', 'classification',
#               'followf_links', 'crawl_website',
#               'year', 'type'
#
# Output columns: 'text', 'classification', 'url', 'type'

def process_websites_file (list_file=list_file, sentences_file=sentences_file):
    print ('\nProcessing websites file: ' + list_file)
    df = load_websites_file (list_file)
    all_rows =  []
    count = 0
    total_pages = 0
    for index, row in df.iterrows():
        rows, page_count = process_website_row (row)
        total_pages += page_count
        print_totals_msg (rows, page_count)
        save_website_sentences (rows, sentences_file, count=count)
        count += 1
        all_rows += rows
    #df = save_websites_sentences (all_rows, sentences_file)
    print_all_totals_msg (all_rows, total_pages)
    merge_websites_files()
    return df

#-----------------------------------------------------------------------------------------
# Main Function
#-----------------------------------------------------------------------------------------

def main():
    #websites_file = sys.argv[1]
    #sentences_file = sys.argv[2]
    # Process websites
    #process_websites_file (websites_file, sentences_file)
    process_websites_file ()

#-----------------------------------------------------------------------------------------

if __name__== "__main__":
  main()
    
#-----------------------------------------------------------------------------------------
# End of File 
#-----------------------------------------------------------------------------------------

