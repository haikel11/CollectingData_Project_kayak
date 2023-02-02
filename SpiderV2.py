import logging
from urllib.parse import parse_qs, urlparse
import os 
import logging
import scrapy
from scrapy.crawler import CrawlerProcess


class BookingSpider(scrapy.Spider):
    name = 'booking_data'
    top_cities = ["Mont Saint Michel","St Malo","Bayeux","Le Havre","Rouen","Paris","Amiens","Lille","Strasbourg","Chateau du Haut Koenigsbourg","Colmar","Eguisheim","Besancon","Dijon","Annecy","Grenoble","Lyon","Gorges du Verdon","Bormes les Mimosas","Cassis","Marseille","Aix en Provence","Avignon","Uzes","Nimes","Aigues Mortes","Saintes Maries de la mer","Collioure","Carcassonne","Ariege","Toulouse","Montauban","Biarritz","Bayonne","La Rochelle"]

    list_url = ['https://www.booking.com/searchresults.fr.html?aid=304142&ss={}&offset='.format(n) for n in top_cities]
    start_urls = []
    for n in list_url:
        for i in range(0,25):
            start_urls.append(str(n) + str(i))
    
    def start_requests(self):
        top_cities = ["Mont Saint Michel","St Malo","Bayeux","Le Havre","Rouen","Paris","Amiens","Lille","Strasbourg","Chateau du Haut Koenigsbourg","Colmar","Eguisheim","Besancon","Dijon","Annecy","Grenoble","Lyon","Gorges du Verdon","Bormes les Mimosas","Cassis","Marseille","Aix en Provence","Avignon","Uzes","Nimes","Aigues Mortes","Saintes Maries de la mer","Collioure","Carcassonne","Ariege","Toulouse","Montauban","Biarritz","Bayonne","La Rochelle"]
        z = 0
        for url in self.start_urls:
            yield scrapy.Request(url, self.parse , meta ={"city" :top_cities[z] })
            z+=1


    def parse(self, response):
        rows = response.xpath("//div[@data-testid = 'property-card']")
      
        for row in rows:
            hotel_name = row.xpath('.//div[@data-testid = "title"]/text()').get()
            url_hotel = row.xpath('.//h3[@class = "a4225678b2"]/a/@href').get()
            hotel_score = row.xpath('.//div[@class = "b5cd09854e d10a6220b4"]/text()').get()
            city = response.request.meta["city"]

            yield response.follow(url= url_hotel, callback=self.parse_hotel, meta={"hotel_name": hotel_name, "hotel_score": hotel_score, "url_hotel": url_hotel, "city":city})
        

    def parse_hotel(self, response):
        url_hotel = response.request.meta['url_hotel']
        hotel_name = response.request.meta['hotel_name']
        hotel_score = response.request.meta['hotel_score']
        city = response.request.meta['city']
        gps = response.xpath('/html/body/script[26]/text()').get()
        lat = float((gps.split(' ')[2].split(';'))[0])
        lon = float((gps.split(' ')[4].split(';'))[0])
        description = ''
        for row in response.xpath('//div[@id = "property_description_content"]/p'):

            description += str((row.xpath('.//text()').get()))
        yield {
                "hotel_name": hotel_name,
                "description": description,
                "city": city,
                "hotel_score": hotel_score,
                "url_hotel": url_hotel,
                "lat": lat,
                "lon": lon
                
            }


process = CrawlerProcess( {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36',
    "FEEDS": {
       "hotel" : {"format": "csv"},
    }
})


# Name of the file where the results will be saved
filename = "hotelsV2.json"

# If file already exists, delete it before crawling (because Scrapy will concatenate the last and new results otherwise)
if filename in os.listdir('./src/'):
        os.remove('./src/' + filename)
        

# Declare a new CrawlerProcess with some settings
process = CrawlerProcess(settings = {
    'USER_AGENT': 'Chrome/97.0',
    'LOG_LEVEL': logging.INFO,
    "FEEDS": {
        './src/' + filename: {"format": "json"},
    }
})

# Start the crawling using the spider you defined above
process.crawl(BookingSpider)
process.start()