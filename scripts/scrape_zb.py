import requests
from bs4 import BeautifulSoup
import bs4
import os
import concurrent.futures
import time
from tqdm import tqdm
import gc

#ZAOBAO WEB CRAWLER FOR BACK TRANSLATION

USE_CACHE = True
NUM_URLS_TO_SCRAPE = -1 #change to -1 for all URLs to be scraped per sitemap page
OUTPUT_FILEPATH = 'output/zb_corpus.txt'
CACHE_FILEPATH = 'cache/linkcache_zb.txt'
DEFAULT_WEBSITE = 'https://www.zaobao.com.sg/'
SITEMAP = 'https://www.zaobao.com.sg/sitemap.xml'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36', #Google Chrome user agent header
    'hl'        :  'en'
    }
BLACKLISTED_LINKS = ['/zodiac/']

s = requests.Session()
s.headers.update(HEADERS)

def gather_urls(save_to_cache = False):
    article_list = []
    sitemap_subpages_list = []

    #Returns 1 if link needs to be skipped, 0 if okay
    def determine_skip_link(link):
        if link == f'{DEFAULT_WEBSITE}': #Ignore front page aka (DEFAULT_WEBSITE)
            return True
        for blacklisted_link in BLACKLISTED_LINKS:
            if link.find(blacklisted_link) != -1:
                return True
        
        return False
    
    r = s.get(SITEMAP)
    soup = BeautifulSoup(r.text, 'lxml')

    for loc_element in soup.find_all('loc'):
        link = loc_element.get_text()
        if link == 'https://www.zaobao.com.sg/sitemaps/sitemap-0.xml': #Skip page 0 of sitemap
            continue
        sitemap_subpages_list.append(link)

    
    total_pages = len(sitemap_subpages_list)
    for idx, sitemap_subpage in enumerate(sitemap_subpages_list):
        r = s.get(sitemap_subpage)
        soup = BeautifulSoup(r.text, 'html.parser')

        for loc_element in soup.find_all(text=lambda tag: isinstance(tag, bs4.CData)):
            link = loc_element.string.strip()
            if determine_skip_link(link): 
                continue
            article_list.append(link)
        
        print(f'Scraping URLs from page {idx+1}/{total_pages}')

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
        return ''

    soup = BeautifulSoup(r.text, 'lxml')

    if not soup.find('div', class_='article-content-rawhtml'): #Skip if article has no paragraph
        return

    for paragraph in soup.find('div', class_='article-content-rawhtml').find_all('p', class_=''):
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
    if NUM_URLS_TO_SCRAPE != -1:
        article_list = article_list[:NUM_URLS_TO_SCRAPE]

    with concurrent.futures.ProcessPoolExecutor() as executor: 
        for article in tqdm(executor.map(scrape_article, article_list), total=len(article_list)):
            results.append(article)

    gc.collect()

    with open(OUTPUT_FILEPATH, 'w', encoding='utf-8') as output_file:
        for page in results:
            if page:
                output_file.write(page)

if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()
    print(f'Program took {t2-t1} seconds to complete')