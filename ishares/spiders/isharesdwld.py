import scrapy
import pandas as pd
import requests


class IsharesdwldSpider(scrapy.Spider):
    name = 'isharesdwld'
    allowed_domains = ['www.ishares.com']
    custom_settings = {
        'ITEM_PIPELINES': {
            'ishares.pipelines.IsharesDwldPipeline': 300
        }
    }


    def start_requests(self):

        # Collect filtered ishares links (on LS)
        path_filted_ishares_links = "brlinks_on_ls.csv"
        df = pd.read_csv(path_filted_ishares_links)
        links = df["etf_url"]

        for link in links:
            yield scrapy.Request(url=link, callback=self.parse)

    def parse(self, response):

        # Collect ISIN for filename
        ISIN = response.xpath(
            'normalize-space(//div[contains(@class,"col-isin")]/span[@class="data"]/text())').get()

        # Collect links to excel file from web
        new_url = response.xpath(
            '(//a[@class="icon-xls-export"]/@href)[2]').get()
        
        if 'fileType=xls' not in new_url:
            new_url = response.xpath(
            '(//a[@class="icon-xls-export"]/@href)[3]').get()
        
        abs_url = response.urljoin(new_url)

        def download_file(url, filename):
            ''' Downloads file from the url and save it as filename '''
            response = requests.get(url)
            # Check if the response is ok (200)
            if response.status_code == 200:
                with open(filename, 'w+b') as f:
                    f.write(response.content)

        # Call function and create file
        path = '.\\downloaded_files\\'
        filename = path + ISIN + '.xls'
        download_file(abs_url, filename)
