# Data-Collectors
Different data collectors

1. ufc_collector
  Required packages:
  1. scrapy
  2. scrrapy-splash

  Run the spider: scrapy crawl ufcier -O athletes_data.json
  This command runs the spider and creates/overwrites athletes_data.json file with scraped data

2. financial_collector
  Required packages:
  1. pandas
  2. BeautifulSoup
  3. sqlalchemy
  
  How to use the script: create new SP500FinancialDataCollector object and use save_financial_data_to_sql method
