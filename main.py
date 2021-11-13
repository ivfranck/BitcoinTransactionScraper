from bs4 import BeautifulSoup
import requests
import logging
from datetime import datetime, timedelta
import time
import json
import pymongo as mongo
import redis


class Scraper:
    def __init__(self):
        self.link = 'https://www.blockchain.com/btc/unconfirmed-transactions'
        self.data = []
        self.final_data = []
        self.current_time = datetime.now().strftime("%H:%M")
        self.old_current_time = ""
        self.dic = {}

    def connection(self):
        return requests.get(self.link)

    def redis_conn(self):
        return redis.Redis()

    def get_data(self):

        time_change = timedelta(hours=1)
        soup = BeautifulSoup(self.connection().text, "html.parser")
        amounts = []
        # get all bitcoin values in a separate list and sort
        for box in soup.findAll('div', {'class': 'sc-1g6z4xm-0 hXyplo'}):
            for category in box.findAll('span', {'class': 'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'}):
                if "BTC" in category.text:
                    amounts.append(float(category.text.strip("BTC")))

        amounts = sorted(amounts, reverse=True)
        top_ten_amounts = []
        for ele in amounts:
            top_ten_amounts.append(str(ele) + " BTC")

        details = ""
        for amt in top_ten_amounts:
            for box in soup.findAll('div', {'class': 'sc-1g6z4xm-0 hXyplo'}):
                for category in box.findAll('span',
                                            {'class': 'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'}):
                    if "BTC" in category.text:
                        if amt == category.text:
                            for addresses in box.findAll('a', {
                                'class': 'sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK'}):
                                details = addresses.text
                            for cats in box.findAll('span', {
                                'class': 'sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC'}):
                                details = details + "+" + cats.text

                            self.data.append(details.split("+"))
            details = ""

        # update date
        for ele in self.data:
            old_time_str = datetime.strptime(ele[1], "%H:%M")
            changed_time = old_time_str + time_change
            updated_time = datetime.strftime(changed_time, "%H:%M")

            ele[1] = updated_time

        corr_data = []

        for ele in self.data:
            for info in ele:
                if info == self.current_time:
                    corr_data.append(ele)

        self.final_data = corr_data[:10]

        if len(self.final_data) != 10:
            self.final_data = []
            self.get_data()

        if len(corr_data) == 0:
            # for an unknown reason corr_data is sometimes empty
            self.get_data()

    def redis_cache(self):
        hash_info = {}
        exp_dic = {}
        headers = ["Hash", "Time", "BTC Amount", "Dollar Amount"]

        for transactions in self.final_data:
            hash_info[transactions[0]] = {headers[1]: transactions[1], headers[2]: transactions[2],
                                          headers[3]: transactions[3]}

        self.dic[self.current_time] = hash_info
        exp_dic[self.current_time] = hash_info

        r = self.redis_conn()
        json_str = json.dumps(exp_dic)
        r.set(self.current_time, json_str, ex=60)

    def to_mongodb(self):
        data = self.redis_conn().get(self.current_time)
        result = json.loads(data)

        client = mongo.MongoClient("mongodb://127.0.0.1:27017")
        transactions_db = client["transactions"]
        col_transactions = transactions_db["hashes"]
        col_transactions.insert_one(result)

    def to_json(self):
        json_obj = json.dumps(self.dic, default=str)
        with open("transc.json", "w") as outfile:
            outfile.write(json_obj)

    def store_data(self):
        logging.basicConfig(filename="transc.log",
                            format='%(message)s',
                            filemode='w')
        logger = logging.getLogger()

        logger.setLevel(logging.INFO)

        string = ""
        for ele in self.final_data:
            for text in ele:
                string = string + "     " + text
            print(string.lstrip())
            logger.info(string.lstrip())
            string = ""
        logger.info("\n")

    def run(self):
        while True:
            if self.current_time != self.old_current_time:
                self.get_data()
                self.store_data()
                self.redis_cache()
                self.to_mongodb()
                self.to_json()
                self.data = []
                self.final_data = []
                self.old_current_time = self.current_time
                time.sleep(60)
                self.current_time = datetime.now().strftime("%H:%M")
            else:
                time.sleep(30)


run = Scraper()
run.run()
