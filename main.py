# coding: utf-8
from pymongo import MongoClient
from bs4 import BeautifulSoup
import re
import requests
from dateutil import parser
import logging
import json
from datetime import datetime
import time

# ロガー作成
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]: %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


class ScrapeReleaseDate():
    def __init__(self, debug=None):

        # mode debug
        self.debug = True if debug else False

        if self.debug:
            # use in dev
            # for test mongodb
            self.url = 'mongodb://hogehoge:27017'

            # dev_production
            # self.mongodb_url = 'hogehoge'

            # dev_develop
            # self.mongodb_url = 'hogehoge'
            self.client_db = MongoClient(self.url)

            # manipulate product mongodb
            self.client_db = MongoClient(self.url)
            self.db_test = self.client_db.test

        else:
            # use in prod
            # this url will change soon for production
            self.url = 'mongodb://hogehoge'
            # manipulate product mongodb
            self.client_db = MongoClient(self.url)
            self.db_prod = self.client_db.hoge

    def main_handler(self):

        try:
            # find data which contains Type Number from products collection
            products_data = self.find_products_data()

            # loop above each data which contains Type Number
            for key, value in enumerate(products_data):
                # 　Type Number is contained in dict of item_attrs
                for key_item, value_item in enumerate(value
                                                      ['hoge_info'][
                                                          'hoge_attrs']):

                    # Scrape function that extract scraped data
                    scraped_data = self. \
                        try_scraping(value, value_item)

                    """ not scraping release date using the number, then
                    break process forcefully to extract next type number """
                    if scraped_data == "nothing":
                        break

                    # when have scraped data, then insert the data into mongodb
                    if scraped_data:
                        # Insert function that insert above data to mongodb
                        self.insert_scrape_data(scraped_data,
                                                value_item["hoge"])
                        break

                    else:
                        logger.info('waiting scraped_data...')

            logger.info('finish!! scraped all data in mongodb!!')

        except Exception as e:
            # this error happened when unexpected error
            logger.error('unexpected error!!\n' + str(e))

    def find_products_data(self):

        """return extracting data which contains
                               Type Number from products collection"""
        # this response is returned in dev
        if self.debug:
            f = open("./scraping_test.json", 'r')
            json_data = json.load(f)

            return json_data

        else:
            # this response is returned in production
            return self.db_prod.hoge.find({
                "hoge.hoge_attrs": {
                    '$exists': 1,
                    '$elemMatch': {"name": {'$regex'
                                            : ".*hoge.*"}, "hoge": {'$ne': ""}}
                },
                "hoge_datetime": {
                    '$exists': 0,
                }
            })

    def try_scraping(self, value, value_item):

        # check whether key have name "型番" or not
        if value_item["name"] == "型番":
            logger.info('***************************')
            logger.info('START NEW TYPE NUMBER TO SCRAPE ')
            logger.info('{}'.format(value_item["hoge"]))

            # extract scraped data
            scraped_data = self.scrape_logic(value_item["hoge"],
                                             value["hoge_id"])
            # whether have scrape data, or not
            if scraped_data:

                return scraped_data

            else:
                logger.info('did not exist data to scrape '
                            'in kakakucom to the type '
                            'number {}'.format(value_item["hoge"]))
                return "nothing"

        else:
            logger.info('waiting value that have type number...')
            return False

    def scrape_logic(self, type_number, product_id):

        # use time sleep to prevent form damaging kakakucom server
        time.sleep(1)

        # use in dev
        if self.debug:

            try:
                # save test file
                # url = "http://kakaku.com/search_results/{}".\
                #     format(type_number)
                #
                # # get url of kakakucom, searching typenumber
                # type_number_search_res = requests.get(url)
                #
                # # encoding for ja
                # content_type_encoding = type_number_search_res.encoding if \
                #     type_number_search_res.encoding != 'ISO-8859-1' \
                #     else None
                #
                # soup = BeautifulSoup(type_number_search_res.content,
                #                      'html.parser',
                #                      from_encoding=content_type_encoding)
                # save the html file in local file, adjusting bs4
                # with open('./test_kakakucom_html/{}.html'
                #                   .format(type_number), mode='w',
                #           encoding='utf-8') as fw:
                #     fw.write(str(soup))

                # # open test file
                soup = BeautifulSoup(
                    open('./test_kakakucom_html/{}.html'
                         .format(type_number), encoding='utf-8'),
                    'html.parser')

            except Exception as e:
                logger.warning('not having file to the type number\n' + str(e))
                pass

        # use in prod
        else:

            """エンコードすると、そのエンコードした文字がテキストボックスに入り、その表記で
            検索される。なので、ここでは、型番は英数字と言うのもあり、実質的にencodeする
            必要性がないので、わざわざencodeしない"""
            url = "http://kakaku.com/search_results/{}".format(type_number)

            # get url of kakakucom, searching typenumber
            type_number_search_res = requests.get(url)

            # encoding for ja
            content_type_encoding = type_number_search_res.encoding if \
                type_number_search_res.encoding != 'ISO-8859-1' \
                else None

            soup = BeautifulSoup(type_number_search_res.content, 'html.parser',
                                 from_encoding=content_type_encoding)

        for index, content in enumerate(soup.find_all('p', class_='itemnameN')):
            """ getting text and splitting each space,
             whether this text is match to type number or not"""
            if str(type_number) in content.getText().split():

                # find tags that have release information
                scraped_data = self.set_scraped_data(soup, product_id,
                                                     index)

                return scraped_data

            else:
                logger.info("searching type number in title text...")

        return False

    def set_scraped_data(self, soup, product_id, index):

        try:
            # find tags that have release information

            """ first, find text that have all item information 
             to match index position. this logic is useful 
             when type number is like D-28"""
            parent_release_raw_data = [
                soup.find_all('div', {'class': 'itemInfo'})[index]]

            """ second, find text that have item release information, 
             matching parent index position"""
            child_release_raw_data = re.findall('<li class="itemSpec">(.*)</li>'
                                                , str(parent_release_raw_data))

            return self.release_check(product_id, child_release_raw_data)

        except Exception as e:
            logger.error(str(e))
            logger.error("above error details: can not run this code "
                         "soup.find_all('li', {'class': 'itemSpec'})[0]'")
            logger.error("meaning: The Tag to scrape does not exist")
            return False

    @staticmethod
    def release_check(hoge_id, release_raw_data):

        # get now
        now = datetime.now()

        # extract release data from above text
        # Is the date included in release data?
        if re.search(r'発売日(.+)日', str(release_raw_data)):

            match = re.search(r'発売日(.+)日',
                              str(release_raw_data))

            # delete extra text
            keyword = match.group(1).replace("：", "")

            # extract year text
            release_year = keyword[:4]

            # apply datetime to db format
            keyword_datetime = keyword.replace("年", "-").replace("月", "-")

            return {
                'created': now,
                'update': now,
                'release_year': release_year,
                'release_datetime': parser.parse(keyword_datetime),
                'hoge_id': hoge_id,
            }
        # Is the "旬" included in release data?
        elif re.search(r'発売日(.+)旬', str(release_raw_data)):

            match = re.search(r'発売日(.+)旬',
                              str(release_raw_data))

            # delete extra text
            keyword = match.group(1).replace("：", "")

            # extract year text
            release_year = keyword[:4]

            # apply datetime to db format
            keyword_datetime = keyword.replace("年", "-")

            # delete extra text
            keyword_datetime = keyword_datetime[:len(keyword_datetime) - 2]

            # Unify everything on the first day of the month
            keyword_datetime = keyword_datetime + "-1"

            return {
                'created': now,
                'update': now,
                'release_year': release_year,
                'release_datetime': parser.parse(keyword_datetime),
                'hoge_id': hoge_id,
            }

        # Is the month included in release data?
        elif re.search(r'発売日(.+)月', str(release_raw_data)):

            match = re.search(r'発売日(.+)月',
                              str(release_raw_data))

            # delete extra text
            keyword = match.group(1).replace("：", "")

            # extract year text
            release_year = keyword[:4]

            # apply datetime to db format
            keyword_datetime = keyword.replace("年", "-")

            # Unify everything on the first day of the month
            keyword_datetime = keyword_datetime + "-1"

            return {
                'created': now,
                'update': now,
                'release_year': release_year,
                'release_datetime': parser.parse(keyword_datetime),
                'hoge_id': hoge_id,
            }

        # when not having month data, not create release_datetime
        elif re.search(r'発売日(.+)年', str(release_raw_data)):

            match = re.search(r'発売日(.+)年',
                              str(release_raw_data))

            # delete extra text
            keyword = match.group(1).replace("：", "")

            # extract year text
            # get only year text
            release_year = keyword[:4]

            return {
                'created': now,
                'update': now,
                'release_year': release_year,
                'hoge_id': hoge_id,
                "release_datetime": []
            }

        else:
            logger.info('not containing release datetime text')
            return False

    def insert_scrape_data(self, value, type_number):

        # insert value to data
        data = {
            "hoge_id": value["hoge_id"],
            "update": value["update"],
            "release_year": value["release_year"],
            "release_datetime": value["release_datetime"]
        }

        # use in dev
        if self.debug:
            # add key, value for test db
            data["created"] = value["created"]
            data["type_number"] = type_number

            # insert scraping data
            self.db_test.products_release_date.insert_one(
                data
            )
            logger.info('SUCCESS IN TEST DB')
            logger.info('RESULT: {}'.format(data))

        # use in prod
        else:
            # get id which have the hoge_id
            product_id_info = self.db_prod.hoge.find_one(
                {"hoge_id": value["hoge_id"]}
            )["_id"]

            # update data
            self.db_prod.hoge.update(
                {"_id": product_id_info},
                {"$set": data}
            )
            logger.info('SUCCESS IN PROD DB')
            logger.info('RESULT: {}'.format(data))


if __name__ == '__main__':
    # dev
    scrape_release_date = ScrapeReleaseDate(debug=True)
    # prod
    # scrape_release_date = ScrapeReleaseDate(debug=None)
    scrape_release_date.main_handler()
