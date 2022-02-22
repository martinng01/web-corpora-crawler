import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import os
import concurrent.futures
import time
from tqdm import tqdm
import logging
from itertools import zip_longest

#SINGAPORE MANDARIN DATABASE WEB CRAWLER FOR BACK TRANSLATION

USE_CACHE = True
NUM_PAGES = 20
OUTPUT_EN = 'output/smd_corpus.en'
OUTPUT_ZH = 'output/smd_corpus.zh'
CACHE_FILEPATH = 'cache/linkcache_smd.txt'
BASE_WEBSITE = 'https://www.languagecouncils.sg'
SCRAPING_WEBSITE = 'https://www.languagecouncils.sg/mandarin/ch/learning-resources/singaporean-mandarin-database/search'
ERROR_LINK = 'errorlinks/errorlinks_smd.txt'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36', #Google Chrome user agent header
    'hl'        :  'en'
    }
Log_Format = "%(message)s"
logging.basicConfig(filename = ERROR_LINK,
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.ERROR)
logger = logging.getLogger()

s = requests.Session()
s.headers.update(HEADERS)
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
s.mount('http://', adapter)
s.mount('https://', adapter)

def gather_urls(save_to_cache = False):
    article_list = []
    zh_chars = []
    en_chars = []

    for page_num in range(1, NUM_PAGES+1):
        r = s.get(SCRAPING_WEBSITE, params={'alp' : '', 'page' : page_num})
        soup = BeautifulSoup(r.text, 'lxml') #Parse XML file

        for row in soup.find_all('div', class_='table-row'):
            if 'header' in row.get('class'):
                continue
            article_list.append(row.get('onclick').split('\'')[1]) #['window.location=', '/mandarin/ch/learning-resources/singaporean-mandarin-database/terms/loan-shark', '']
            zh_chars.append(row.find(class_='ch').get_text())
            en_chars.append(row.find(class_='en').get_text())

    if save_to_cache:
        with open(CACHE_FILEPATH, 'w', encoding='utf-8') as cache_file:
            for idx, link in enumerate(article_list):
                cache_file.write(f'{BASE_WEBSITE}{link}|{zh_chars[idx]}|{en_chars[idx]}\n')
            print(f'Saved URLs into {CACHE_FILEPATH}')

    return article_list, zh_chars, en_chars

def scrape_article(article):
    zh_output = []
    en_output = []

    try:
        r = s.get(article)
    except Exception as e:
        print(e)
        logger.error(article)
        return

    soup = BeautifulSoup(r.text, 'lxml')
    
    # #Etymology
    # zh_etymology_id = 'smcplaceholdercontent_0_ChineseEtymologyContent'
    # en_etymology_id = 'smcplaceholdercontent_0_EnglishEtymologyContent'
    # if soup.find('div', id=zh_etymology_id) or soup.find('div', id=en_etymology_id):
    #     en_para_list = []
    #     zh_para_list = []

    #     zh_para_list = [element.get_text().strip() for element in soup.find('div', id=zh_etymology_id).find_all('li')]

    #     #If english etymology paragraph exists
    #     if soup.find('div', id=en_etymology_id):
    #         if soup.find('div', id=en_etymology_id).find('li'):
    #             en_para_list = [element.get_text().strip() for element in soup.find('div', id=en_etymology_id).find_all('li')]
    #         else:
    #             en_para_list = soup.find('div', id=en_etymology_id).get_text().strip().split('\n')

    #     for zh_para, en_para in zip_longest(zh_para_list, en_para_list):
    #         if zh_para and zh_para.find('\u00A0') != -1: #Ignore non breaking space chars
    #             continue
    #         if en_para and en_para.find('\u00A0') != -1: #Ignore non breaking space chars
    #             continue

    #         zh_output += (zh_para + '\n') if zh_para else '-\n'
    #         en_output += (en_para + '\n') if en_para else '-\n'

    #Remove &nbsp; \n , strip string
    def clean_string(string):
        return string.replace(u'\u00a0', '').replace('\n', ' ').strip()

    #Definition
    zh_definition_div = soup.find('div', id='smcplaceholdercontent_0_ChineseDefinitionContent')
    en_definition_div = soup.find('div', id='smcplaceholdercontent_0_EnglishDefinitionContent')
    zh_definition = en_definition = '-'
    if zh_definition_div:
        if zh_definition_div.find('li'): #If definition is a list
            for subdefinition in zh_definition_div.find_all('li'):
                zh_definition = ''
                zh_definition += clean_string(subdefinition.get_text())
        elif zh_definition_div.find('div', class_='column'):
            zh_definition = clean_string(zh_definition_div.find('div', class_='column').get_text())
        elif clean_string(zh_definition_div.find('p', class_='english__text grammarBox__define').get_text()) == '':
            zh_definition = clean_string(zh_definition_div.find('p', class_='english__text grammarBox__define').next_sibling.get_text())
        else:
            zh_definition = clean_string(zh_definition_div.find('p', class_='english__text grammarBox__define').get_text())
    if en_definition_div:
        if en_definition_div.find('li'): #If definition is a list
            for subdefinition in en_definition_div.find_all('li'):
                en_definition = ''
                en_definition += clean_string(subdefinition.get_text())
        elif en_definition_div.find('div', class_='column'):
            en_definition = clean_string(en_definition_div.find('div', class_='column').get_text())
        elif clean_string(en_definition_div.find('p', class_='english__text grammarBox__define').get_text()) == '':
            zh_definition = clean_string(en_definition_div.find('p', class_='english__text grammarBox__define').next_sibling.get_text())
        else:
            en_definition = clean_string(en_definition_div.find('p', class_='english__text grammarBox__define').get_text())
    zh_output.append(zh_definition)
    en_output.append(en_definition)

    #Sample sentence
    zh_sample_div = soup.find('div', id='smcplaceholdercontent_0_ChineseSentencesContent')
    en_sample_div = soup.find('div', id='smcplaceholdercontent_0_EnglishSentencesContent')
    zh_sample = en_sample = '-'
    if zh_sample_div:
        zh_sample = clean_string(zh_sample_div.get_text())
    if en_sample_div:
        en_sample = clean_string(en_sample_div.get_text())
    zh_output.append(zh_sample)
    en_output.append(en_sample)

    #Terms used in other regions
    zh_region_div = soup.find('div', id='smcplaceholdercontent_0_ChineseTermsUsedContent')
    en_region_div = soup.find('div', id='smcplaceholdercontent_0_EnglishTermsUsedContent')
    zh_region = en_region = '-'
    if zh_region_div:
        zh_region = clean_string(zh_region_div.get_text())
    if en_region_div:
        en_region = clean_string(en_region_div.get_text())
    zh_output.append(zh_region)
    en_output.append(en_region)

    return zh_output, en_output

def main():
    print(f'{USE_CACHE=}')

    article_list = []
    zh_list = []
    en_list = []
    if USE_CACHE:
        if os.path.isfile(CACHE_FILEPATH) and os.path.getsize(CACHE_FILEPATH) > 0:
            print('Using URLs from cache file')

            with open(CACHE_FILEPATH, 'r', encoding='utf-8') as cache_file:
                for line in cache_file:
                    article, zh, en = line.strip().split('|')
                    article_list.append(article)
                    zh_list.append(zh)
                    en_list.append(en)
        else:
            print('Cache file does not exist... creating now')
            article_list, zh_list, en_list = gather_urls(save_to_cache=True)
    else:
        article_list, zh_list, en_list = gather_urls(save_to_cache=False)

    print('Starting article scraping...')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        with open(OUTPUT_EN, 'w', encoding='utf8') as en_file, open(OUTPUT_ZH, 'w', encoding='utf8') as zh_file:
            for idx, (zh_output, en_output) in enumerate(tqdm(executor.map(scrape_article, article_list), total=len(article_list))):
                zh_file.write(zh_list[idx] + '\n')
                en_file.write(en_list[idx] + '\n')
                for line_zh, line_en in zip(zh_output, en_output):
                    zh_file.write(line_zh + '\n')
                    en_file.write(line_en + '\n')
                zh_file.write('\n')
                en_file.write('\n')

if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()
    print(f'Program took {t2-t1} seconds to complete')