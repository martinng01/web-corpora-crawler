import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import os
import concurrent.futures
import time
from tqdm import tqdm
import logging

#CNA WEB CRAWLER FOR BACK TRANSLATION

USE_CACHE = True
SITEMAP_NUM_PAGES = 55 #MAX 55, change for debugging
NUM_URLS_TO_SCRAPE = -1 #change to -1 for all URLs to be scraped per sitemap page
OUTPUT_FILEPATH = 'output/cna_corpus.txt'
CACHE_FILEPATH = 'cache/linkcache_cna.txt'
DEFAULT_WEBSITE = 'https://www.channelnewsasia.com/'
ERROR_LINK = 'errorlinks/errorlinks_cna.txt'
SITEMAP = 'https://www.channelnewsasia.com/sitemap.xml'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36', #Google Chrome user agent header
    'hl'        :  'en'
    }
BLACKLISTED_LINKS = ['/brandstudio/', '/tokyo-2020/', '/taxonomy/', '/author/', '/rss/', '/interactives/', '/node/', '/about-us/', '/contact-us/']

s = requests.Session()
s.headers.update(HEADERS)
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
s.mount('http://', adapter)
s.mount('https://', adapter)

Log_Format = "%(message)s"
logging.basicConfig(filename = ERROR_LINK,
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.ERROR)
logger = logging.getLogger()

def gather_urls(save_to_cache = False):
    article_list = []

    #Returns 1 if link needs to be skipped, 0 if okay
    def determine_skip_link(link):
        if link == f'{DEFAULT_WEBSITE}': #Ignore front page aka (DEFAULT_WEBSITE)
            return True
        if link.split(DEFAULT_WEBSITE)[1].find('/') == -1: #Ignore section pages like https://www.channelnewsasia.com/international
            return True
        for blacklisted_link in BLACKLISTED_LINKS:
            if link.find(blacklisted_link) != -1:
                return True
        
        return False

    for sitemap_page in range(1, SITEMAP_NUM_PAGES+1):
        r = s.get(SITEMAP, params={'page' : sitemap_page})
        soup = BeautifulSoup(r.text, 'lxml') #Parse XML file

        for loc_element in soup.find_all('loc'):
            link = loc_element.get_text()
            if determine_skip_link(link): 
                continue
            article_list.append(link)
        
        print(f'Scraping URLs from page {sitemap_page}/{SITEMAP_NUM_PAGES}', end = '\r')

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
        logger.error(article)
        return

    soup = BeautifulSoup(r.text, 'lxml')

    if soup.find('title') and soup.find('title').string.find('Page Not found') != -1:
        logger.error(article)
        return

    if soup.find('div', class_='text-long'):
        for paragraph in soup.find('div', class_='text-long').find_all('p', class_=''):
            if paragraph.get_text().find('\u00A0') != -1: #Ignore non breaking space chars
                continue
            output += paragraph.get_text() + '\n'
    
    if soup.find('div', class_='podcast-main__description'):
        for paragraph in soup.find('div', class_='podcast-main__description').find_all('p', class_=''):
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

    if NUM_URLS_TO_SCRAPE != -1:
        article_list = article_list[:NUM_URLS_TO_SCRAPE]

    num_access_denied = 0
    num_nones = 0
    with concurrent.futures.ThreadPoolExecutor() as executor:
        with open(OUTPUT_FILEPATH, 'w', encoding='utf8') as output_file:
            for article in tqdm(executor.map(scrape_article, article_list), total=len(article_list)):
                if article == None:
                    num_nones += 1
                else:
                    output_file.write(article)

    print(f'{num_access_denied=}')
    print(f'{num_nones=}')

if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()
    print(f'Program took {t2-t1} seconds to complete')