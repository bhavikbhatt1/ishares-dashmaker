''' Script pulls ishares data from Lang & Schwarz'''

import scrapy

class isharesLS(scrapy.Spider):
    name = "isharesLS"
    allowed_domains = ["www.ls-x.de"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'ishares.pipelines.IsharesPipeline': 300
        }
    }

    def start_requests(self):
        start_url = "https://www.ls-x.de/de/etfs"
        yield scrapy.Request(url=start_url, callback=self.parse)

    def parse(self, response):
        # Create a SelectorList of <a> elements with hyperlinks to LS ETF pages
        etf_list = response.xpath('//a[contains(@href,"etf/")]')

        # # Filter to ishares links only
        # ishares_list = []
        # for i in etf_list:
        #     if "IS" in (i.xpath('.//text()').get())[0:2]:
        #         ishares_list.append(i)

        # Don't filter to ishares links only
        ishares_list = etf_list

        # Send links to parse_2 to collect ISIN WKN from LS ETF page (if links exist)
        if ishares_list:
            for i in ishares_list:
                r_url = i.xpath('.//@href').get()
                a_url = response.urljoin(r_url)
                yield scrapy.Request(url=a_url, callback=self.parse_2)

        # Handle pagination
        next_url = response.xpath('(//a[@class="next"]/@href)[1]').get()
        if next_url:
            next_url = response.urljoin(next_url)
            yield scrapy.Request(url=next_url, callback=self.parse)

    def parse_2(self, response):
        etf_header = response.xpath('//title/text()').get()
        etf_header_split = etf_header.split(' | ')
        yield {
            'etf_title': etf_header_split[0],
            'etf_WKN': etf_header_split[3],
            'etf_ISIN': etf_header_split[4]
        }