from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import coloredlogs
import logging
import pandas as pd
import pandasgui
import requests
import multiprocessing
import multiprocessing_logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def wait_url_change(driver, timeout=600, match=None):
    logger.debug(f"Waiting for URL change. timeout={timeout}s, match={match}")
    old_url = driver.current_url
    WebDriverWait(driver, timeout).until(
        lambda driver: old_url != driver.current_url
        and driver.execute_script("return document.readyState == 'complete'")
    )
    logger.debug(driver.current_url)
    if match and not re.search(match, driver.current_url):
        wait_url_change(driver, timeout, match)


def make_soup(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup


def list_search_results(driver, url):
    driver.get(url)
    soup = make_soup(driver.page_source)
    products = soup.select('ul.productGrid > li')
    links = ['http:' + p.select_one('a').get('href') for p in products]
    return links


def fetch_product_data(product_url):
    def list_product_options(soup):
        options = soup.select('select.form-select.form-select--small.form-select--alt > option[data-product-attribute-value]')
        if not options:
            return None
        names = [e.text for e in options]
        return names

    soup = make_soup(requests.get(product_url).content)
    variants = list_product_options(soup)
    if not variants:
        logger.warning(f'No variants for product at {product_url}')
        return []
    products = []
    for v in variants:
        products.append({
            'Name': soup.select_one('.productView-title').text,
            'Variant': v,
            'URL': product_url,
        })
    logger.debug(f"Fetched data for {products[0]['Name']} with {len(products)} variants")
    return products


def mp_fetch_product_data(url, products):
    products += fetch_product_data(url)


def single_threaded(search_url):
    with webdriver.Firefox() as driver:
        links = list_search_results(driver, search_url)
    logger.info(f'Found {len(links)} search results')

    products = []
    i = 0
    for i, product_url in enumerate(links):
        products += fetch_product_data(product_url)
        if i == 2:
            break

    return pd.DataFrame.from_records(products)


def multi_threaded(search_url):
    with webdriver.Firefox() as driver:
        links = list_search_results(driver, search_url)
    logger.info(f'Found {len(links)} search results')

    # processes = []
    # with multiprocessing.Manager() as manager:
    #     products = manager.list([])
    #     i = 0
    #     for i, product_url in enumerate(links):
    #         process = multiprocessing.Process(target=mp_fetch_product_data, args=(product_url, products))
    #         processes.append(process)
    #         process.start()
    #         if i == 99:
    #             break
    #
    #     for p in processes:
    #         p.join()
    #     return pd.DataFrame.from_records(products)

    pool = multiprocessing.Pool()
    results = [pool.apply(fetch_product_data, args=(url,)) for url in links[0:10]]
    pool.close()
    pool.join()
    products = []
    for r in results:
        products.extend(r)
    return pd.DataFrame.from_records(products)


def main():
#    coloredlogs.install(level='DEBUG', logger=logger)
    multiprocessing_logging.install_mp_handler()

    SEARCH = 'https://www.rockbottomgolf.com/golf-clubs/hybrids/#/?profile_id=7ce0e69e2cb37bcee2e3073332381477&session_id=43e6a6e8-d701-11e9-b23e-0242ac110003&authorized_profile_id=&Searchcat1=Woods&Searchcat2=Hybrids&Gender=Mens&Dexterity=Right&Flex=Regular&sort_by_field=Price+Low+To+High&search_return=all&Searchcondition=Used'
    #df = single_threaded(SEARCH)
    df = multi_threaded(SEARCH)
    pandasgui.show(df)


if __name__ == "__main__":
    main()