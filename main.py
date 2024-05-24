import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from twisted.internet.asyncioreactor import AsyncioSelectorReactor as reactor
from scrapy.utils.reactor import install_reactor
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
import scrapy.signals
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright
from selenium import webdriver
from selenium.webdriver.common.by import By

import sys
import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class HHSpider(scrapy.Spider):
    name='HHSpider'
    start_urls=['data:,']
    driver: webdriver.Chrome

    custom_settings={
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def parse(self, response, **kwargs):
        output = []
        for i in range(10):
            output.extend( self.get_cian_flats_data(i + 1))
        return output

    def get_cian_flats_data(self, page):
        self.driver.get(f'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p=1&region=4777&room1=1&p={page}')
        elements       = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-name="CardComponent"]')
        
        def getArea(element):
            priceForArea = element.find_element(By.CSS_SELECTOR, 'p[data-mark="PriceInfo"]').text
            price = element.find_element(By.CSS_SELECTOR, 'span[data-mark="MainPrice"] > span').text
            floatval = float(price[:-2].replace(" ", "")) / float(priceForArea[:-5].replace(" ", ""))
            return f"{floatval:.2f} м²"

        def getAddress(element):
            labels = map(lambda x: x.text, element.find_elements(By.CSS_SELECTOR, 'a[data-name="GeoLabel"]'))
            return ', '.join(labels)

        def getRoomNumber(element):
            text = element.find_element(By.CSS_SELECTOR, 'span[data-mark="OfferTitle"] > span').text
            return text.split("-комн.")[0] if "-комн." in text else "1"

        def getID(element):
            link = element.find_element(By.CSS_SELECTOR, 'div[data-testid="offer-card"] > a').get_attribute("href")
            return link[:-1].split('/')[-1]

        titles         = map(lambda x: x.text, self.driver.find_elements(By.CSS_SELECTOR, 'span[data-mark="OfferTitle"] > span'))
        roomNumbers    = map(lambda x: x.split("-комн.")[0] if "-комн." in x else "1", titles)
        prices         = map(lambda x: x.text, self.driver.find_elements(By.CSS_SELECTOR, 'span[data-mark="MainPrice"] > span'))
        pricesForArea  = map(lambda x: x.text, self.driver.find_elements(By.CSS_SELECTOR, 'p[data-mark="PriceInfo"]'))
        areas          = map(lambda x: x[:-2].replace(" ", ""), prices)#map(getArea         , zip(prices, pricesForArea))
        
        elements = filter(lambda element: "этаж" in element.find_element(By.CSS_SELECTOR, 'span[data-mark="OfferTitle"] > span').text, elements)

        return [{
            "title": element.find_element(By.CSS_SELECTOR, 'span[data-mark="OfferTitle"] > span').text,
            "roomNumber" : getRoomNumber(element=element),
            "price" :element.find_element(By.CSS_SELECTOR, 'span[data-mark="MainPrice"] > span').text,
            "area": getArea(element=element),
            "floor": element.find_element(By.CSS_SELECTOR, 'span[data-mark="OfferTitle"] > span').text.split(' ')[4],
            "address": getAddress(element=element),
            "price": element.find_element(By.CSS_SELECTOR, 'span[data-mark="MainPrice"] > span').text,
            "ID": getID(element=element),
            "page": page
            } for element in elements]
        #return [{"title": title, "roomNumber": roomNumber, "price": price, "area": area} for title, roomNumber, price, area in zip(titles, roomNumbers, prices, areas)]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(HHSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        crawler.signals.connect(spider.spider_opened, signal=scrapy.signals.spider_opened)
        return spider

    def spider_closed(self, spider):
        spider.driver.close()

    def spider_opened(self, spider):
        chromeOptions = webdriver.ChromeOptions()
        #chromeOptions.add_argument("--headless=new")
        
        spider.driver = webdriver.Chrome(options=chromeOptions)
        

#install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')
configure_logging()

process = CrawlerProcess(
    settings={
        "FEEDS": {
            "items.json": {"format": "json",}
        },
        
    }
) 

process.crawl(HHSpider)
process.start()