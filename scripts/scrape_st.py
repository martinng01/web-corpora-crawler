import requests
from bs4 import BeautifulSoup
import os
import concurrent.futures
import time
from tqdm import tqdm
import gc

#STRAITS TIMES WEB CRAWLER FOR BACK TRANSLATION

USE_CACHE = True
SITEMAP_NUM_PAGES = 30 #MAX 30
OUTPUT_FILEPATH = 'output/st_corpus.txt'
CACHE_FILEPATH = 'cache/linkcache_st.txt'
DEFAULT_WEBSITE = 'https://www.straitstimes.com'
SITEMAP = 'https://www.straitstimes.com/sitemap.xml'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36', #Google Chrome user agent header
    'hl'        :  'en'
    }

s = requests.Session()
s.headers.update(HEADERS)

def gather_urls(save_to_cache = False):
    article_list = []

    for sitemap_page in range(1, SITEMAP_NUM_PAGES+1):
        r = s.get(SITEMAP, params={'page' : sitemap_page})
        soup = BeautifulSoup(r.text, 'lxml') #Parse XML file

        for loc_element in soup.find_all('loc'):
            link = loc_element.get_text()
            if link == f'{DEFAULT_WEBSITE}/': #Ignore straitstimes.com front page
                continue
            if link.find('multimedia') != -1: #Ignore multimedia articles
                continue

            article_list.append(link)
        print(f'Scraping URLs from page {sitemap_page}/{SITEMAP_NUM_PAGES}')

    if save_to_cache:
        with open(CACHE_FILEPATH, 'w', encoding='utf-8') as cache_file:
            for link in article_list:
                cache_file.write(link + '\n')
            print(f'Saved URLs into {CACHE_FILEPATH}')

    return article_list

def scrape_article(article):
    output = ''

    try:
        r = s.get(article)
    except Exception as e:
        print(e)
        return 'ERROR\n'

    soup = BeautifulSoup(r.text, 'lxml')

    if soup.find('div', class_='paid-premium st-flag-1'): #Do not scrape premium articles
        return
    if not soup.find('div', class_='clearfix text-formatted field field--name-field-paragraph-text field--type-text-long field--label-hidden field__item'): #Skip if article has no paragraph
        return

    for paragraph in soup.find('div', class_='clearfix text-formatted field field--name-field-paragraph-text field--type-text-long field--label-hidden field__item').find_all('p', class_=''):
        if paragraph.get_text() == "READ MORE HERE": #Ignore the READ MORE HERE from Morning Briefing articles
            continue
        if paragraph.get_text().find('\u00A0') != -1: #Ignore non breaking space chars
            continue
        output += paragraph.get_text() + '\n'
    
    return output


def main():
    print(f'{USE_CACHE=}')

    if USE_CACHE:
        if os.path.isfile(CACHE_FILEPATH) and os.path.getsize(CACHE_FILEPATH) > 0:
            print('Using URLs from cache file')

            with open(CACHE_FILEPATH, 'r', encoding='utf-8') as cache_file:
                article_list = [line.strip() for line in cache_file]
        else:
            print('Cache file does not exist... creating now')
            article_list = gather_urls(save_to_cache=True)
    else:
        article_list = gather_urls(save_to_cache=False)
    
    print('Starting article scraping...')

    results = []
    print(len(gc.get_objects()))
    with concurrent.futures.ProcessPoolExecutor() as executor: 
        for article in tqdm(executor.map(scrape_article, article_list), total=len(article_list)):
            results.append(article)
    gc.collect()
    print(len(gc.get_objects()))
    with open(OUTPUT_FILEPATH, 'a', encoding='utf-8') as output_file:
        for page in results:
            if page:
                output_file.write(page + '\n')

if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()
    print(f'Program took {t2-t1} seconds to complete')