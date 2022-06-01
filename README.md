# Data-Collectors
Different data collectors

1. ufc_collector
  Required packages:
  -scrapy
  -scrrapy-splash
  
  Run the spider: scrapy crawl ufcier -O athletes_data.json
  Before you start the spider setup splash to run on docker and change address and port in settings.py file
  This command runs the spider and creates/overwrites athletes_data.json file with scraped data

2. financial_collector
  Required packages:
  -pandas
  -BeautifulSoup
  -sqlalchemy
  
  How to use the script: create new SP500FinancialDataCollector object and use save_financial_data_to_sql method
