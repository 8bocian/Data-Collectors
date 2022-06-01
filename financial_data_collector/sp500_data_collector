#  Copyright (c) 2022. Oskar "Bocian" Możdżeń
#  All rights reserved.

from collections import Counter
import datetime
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


class SP500FinancialDataCollector:
    def __init__(self, tickers_file, log_file, database_url):
        """
        Saves financial info to sql database
        :param tickers_file: csv file where column 'Symbols' contains tickers of companies with separator=; or list of tickers
        :param log_file: path to the file where the error info will be saved
        :param database_url: database url with dialect, driver, username, password, database address, port, database name
        "dialect+driver://username:password@server_address:port/data_base"
        """

        self.sql_engine = create_engine(database_url, pool_recycle=3600)
        self.database_connection = self.sql_engine.connect()

        if isinstance(tickers_file, str):
            self.tickers_file = tickers_file
            self.tickers_data_frame = pd.read_csv(self.tickers_file, sep=";")
            self.tickers = list(self.tickers_data_frame['Symbol'])
        else:
            self.tickers = tickers_file

        self.log_file = log_file
        self.errors = []
        self.date = ""

    def save_financial_data_to_sql(self, data_table_name="companies", missing_data_table_name="missing_data",
                                   saving_batch_size=16, mode='append'):
        """
        collects financial data from the market watch web page
        :param data_table_name: target table in the database
        :param missing_data_table_name: table where missing data amount will be stored
        :param saving_batch_size: number of data frames in the batch
        :param mode: 'fail', 'append', 'replace' use append if using batches
        """

        self.date = str(datetime.date.today())
        data_frames = []
        nans_amount = {}

        for idx, ticker in enumerate(self.tickers):
            data_frames.append(self.create_company_data_frame(ticker, idx))

            if (idx + 1) % saving_batch_size == 0 or idx + 1 == len(self.tickers):

                # concatenate all companies data
                batch_data_frame = pd.concat(data_frames, ignore_index=True)

                # clear the data
                batch_data_frame, nans = self.clean_data(batch_data_frame)

                # check the data format and create uniform columns names
                for column in batch_data_frame.columns[3:]:
                    batch_data_frame[column] = batch_data_frame[column].apply(self.change_values_to_numeric)
                    batch_data_frame.rename(columns={column: str(column).replace(" ", "_").replace("/", "_").lower()},
                                            inplace=True)

                # save the data to database
                batch_data_frame.to_sql(data_table_name, self.database_connection, if_exists=mode, index_label=True,
                                        index=False)
                nans_amount = Counter(nans_amount) + Counter(nans)
                data_frames.clear()

        nans_amount["date"] = self.date

        for key, value in nans_amount.items():
            nans_amount[key] = [value]

        nans_data_frame = pd.DataFrame(data=nans_amount)
        nans_data_frame.to_sql(missing_data_table_name, self.database_connection, if_exists=mode, index_label=True,
                               index=False)

    def create_company_data_frame(self, ticker, idx):
        """
        :param ticker: uppercase ticker of a company
        :param idx: company id in sql database
        :return: the company data frame with combined financial info
        """

        print(f"Errors: {len(self.errors)}")
        print(f"Progress: {((idx / len(self.tickers)) * 100):.2f}", flush=True)
        print(f"Current ticker: {ticker} {idx}")

        # create data_frame of financial data
        data_frame = self.get_financial_info(ticker, idx)

        # get price of company share
        price = self.get_price(ticker)

        # combine gathered data
        data_frame.insert(3, "price", price)

        os.system('cls')

        return data_frame

    def get_financial_info(self, ticker, idx):
        """
        get financial info from market watch about the company
        :param ticker: uppercase ticker of a company
        :param idx: company id in sql database
        :return: the company data frame with financial info form market watch
        """

        try:
            url = f"https://www.marketwatch.com/investing/stock/{ticker}/company-profile?mod=mw_quote_tab"
            data_frame = pd.concat(pd.read_html(url)[4:9]).T

            # change columns names for data frame
            data_frame.columns = data_frame.iloc[0]
            data_frame.drop(0, axis=0, inplace=True)
            data_frame.drop(["Income Per Employeee", "Revenue/Employee", "Receivables Turnover"], axis=1,
                            inplace=True)

            # add collected data together
            additional_data = {"ticker": [ticker], "date": [self.date], "ticker_id": [int(idx)]}
            for key, value in additional_data.items():
                # noinspection PyTypeChecker
                data_frame.insert(0, key, value=value)

        except Exception as exception:
            self.write_to_log(exception)
            data_frame = pd.DataFrame(data={"ticker_id": idx, "date": self.date, "ticker": [ticker]})

        return data_frame

    def get_price(self, ticker):
        """
        :param ticker: uppercase ticker of a company
        :return: price per share of a company
        """
        # get the share price of company
        # receive content of the web page
        html_page = requests.get(f'https://www.marketwatch.com/investing/stock/{ticker}?mod=mw_quote_tab').content
        soup = BeautifulSoup(html_page, 'html.parser')

        # search the web page content for appropriate tag
        # if price is not available then price = 0
        try:
            price = float(str(soup.find('h2', {"class": "intraday__price"}).getText())[3:-1].replace(",", ""))
        except Exception as error:
            self.write_to_log(error)
            price = float("nan")

        return price

    @staticmethod
    def clean_data(data_frame: pd.DataFrame):
        """
        clears the data frame
        :param data_frame: data frame to clear
        :return: cleared data frame & amount of missing data in each column
        """
        nans = dict()
        for column in data_frame.columns:
            nans[column] = data_frame[column].isna().sum()
        cleared_data_frame = data_frame.fillna(-1.0)
        return cleared_data_frame, nans

    @staticmethod
    def change_values_to_numeric(x):
        """
        changes string values that represent percentage or monetary value to float
        :param x: value to change
        :return: numeric values
        """
        string_x = str(x).replace("$", "").replace("€", "").replace(",", "")

        if string_x[-1] == "M":
            value = float(string_x.replace("M", "")) * 1000000
        elif string_x[-1] == "%":
            value = float(string_x.replace("%", "")) / 100
        else:
            value = float(string_x)

        return round(value, 3)

    def write_to_log(self, error: Exception):
        """
        saves an error to log_file
        :param error: error to save
        """

        name = os.path.basename(__file__)
        error_message = f"\n{name} {datetime.datetime.now()}: {str(error)}"
        self.errors.append(error_message)
        f = open(self.log_file, 'a')
        f.writelines(error_message)
        f.close()
