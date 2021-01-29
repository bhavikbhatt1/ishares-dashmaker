''' Script pulls links to ishares subpages from ishares website along with ISIN + WKN references'''

import scrapy


class IsharesbrSpider(scrapy.Spider):
    name = 'IsharesbrSpider'
    allowed_domains = ['www.ishares.com']
    custom_settings = {
        'ITEM_PIPELINES': {
            'ishares.pipelines.IsharesbrPipeline': 300
        }
    }

    def start_requests(self):
        # start_url is overview of all ishares ETFs with links to subpages
        start_url = "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments?switchLocale=y&siteEntryPassthrough=true"
        yield scrapy.Request(url=start_url, callback=self.parse)

    def parse(self, response):
        # Create a SelectorList of <a> elements with hyperlinks to ishares ETF subpages
        ishares_list = response.xpath(
            '//table[1]/tbody/tr/td[@class="links"][2]/a')

        # Send links to parse_2 to collect ISIN WKN (if links exist)
        if ishares_list:
            for i in ishares_list:
                r_url = i.xpath('.//@href').get()
                a_url = response.urljoin(r_url)
                a_url = a_url+'?switchLocale=y&siteEntryPassthrough=true'  # handle cookies
                yield scrapy.Request(url=a_url, callback=self.parse_2)

    def parse_2(self, response):
        etf_title = response.xpath(
            'normalize-space((//*[@id="fundHeader"]//h1[contains(@class, "product-title")]/text())[1])').get()
        etf_WKN = response.xpath(
            'normalize-space(//*[@id="keyFundFacts"]//div[contains(@class,"wkn")]/span[@class="data"]/text())').get()
        etf_ISIN = response.xpath(
            'normalize-space(//*[@id="keyFundFacts"]//div[contains(@class,"isin")]/span[@class="data"]/text())').get()

        yield {
            'etf_title': etf_title,
            'etf_WKN': etf_WKN,
            'etf_ISIN': etf_ISIN,
            'etf_url': response.url
        }
