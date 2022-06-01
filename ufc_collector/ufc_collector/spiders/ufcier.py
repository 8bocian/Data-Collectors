from scrapy.spiders import CrawlSpider
from scrapy_splash import SplashRequest


class PwspiderSpider(CrawlSpider):
    name = 'ufcier'
    allowed_domains = ['www.ufc.com']
    start_urls = ['https://www.ufc.com/athletes/all']
    url = 'https://www.ufc.com/athletes/all'
    main_site_url = 'https://www.ufc.com'

    def start_requests(self):
        # Use splash to get the response from web page that uses js
        yield SplashRequest(url=self.url, callback=self.parse)

    def parse(self, response, **kwargs):
        # get all athlete cards
        athletes = response.css('div.c-listing-athlete-flipcard__inner')

        for athlete in athletes:
            name = athlete.css('span.c-listing-athlete__name::text')[0].re(r'(\w+|[-]) (\w+|[-])')
            weight_class = athlete.css(
                'div.field.field--name-stats-weight-class.field--type-entity-reference.field--label-hidden.field__items div.field__item::text').get()
            record = athlete.css('span.c-listing-athlete__record::text').get()

            # don't accept new athletes don't have any experience
            if record == "0-0-0 (W-L-D)":
                continue

            # link to the athlete main page
            link = self.main_site_url + athlete.css('div.c-listing-athlete-flipcard__back a').attrib['href']

            yield response.follow(url=link, callback=self.parse_athlete, cb_kwargs={'info': {'name': name,
                                                                                             'weight_class': weight_class,
                                                                                             'record': record}})
        # get new list of athletes
        try:
            next_page = response.css('a.button').attrib['href']
            yield response.follow(url=self.url + next_page, callback=self.parse)
        except:
            return

    def parse_athlete(self, response, info):
        main_dict = info

        # if athlete is not fighting then return nothing
        if response.css('div.c-bio__text::text').get() == 'Not Fighting':
            return

        for key, value in zip(response.css('h2.e-t3::text').getall(),
                              response.css('text.e-chart-circle__percent::text').getall()):
            main_dict[key] = float(value.strip('%'))

        for key, value in zip(response.css('dt.c-overlap__stats-text::text').getall(),
                              response.css('dd.c-overlap__stats-value::text').getall()):
            main_dict[key] = float(value)

        for key, value in zip(response.css('div.c-stat-compare__label::text').getall(),
                              response.css('div.c-stat-compare__number::text').getall()):

            value = float(value.strip()) if len(value.strip()) < 5 else value.strip()
            main_dict[key.strip()] = value

        for key, value in zip(response.css('div.c-stat-3bar__label::text').getall(),
                              response.css('div.c-stat-3bar__value::text').re("\A[0-9]+")):
            main_dict[key] = float(value)

        for data in response.css('div.c-stat-body__diagram g')[1:4]:
            text = data.css('text::text').getall()
            key = text[2]
            value = text[1]
            main_dict[key] = value

        # collect data from athlete biogram
        for key, value_div in zip(response.css('div.c-bio__label::text').getall(),
                              response.css('div.c-bio__text')):
            value = value_div.css('::text').get()
            if key == "Age":
                value = value_div.css('div div::text').get()
            if value[0].isnumeric():
                value = float(value)
            main_dict[key] = value

        yield main_dict
