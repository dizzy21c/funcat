# -*- coding: utf-8 -*-
#

from cached_property import cached_property

from .backend import DataBackend
from ..utils import lru_cache, get_str_date_from_int, get_int_date


class MongoDataBackend(DataBackend):

    @cached_property
    def mongo(self):
        try:
            from easyquant import MongoIo
            mongo = MongoIo()
            return mongo
        except ImportError:
            print("-" * 50)
            print(">>> Missing easyquant-MongoIo. Please run `pip install tushare`")
            print("-" * 50)
            raise

    @cached_property
    def stock_basics(self):
        return self.mongo.get_stock_list()

    @cached_property
    def code_name_map(self):
        code_name_map = self.stock_basics.name.to_dict()
        return code_name_map

    def convert_code(self, order_book_id):
        # return order_book_id.split(".")[0]
        return order_book_id

    @lru_cache(maxsize=4096)
    def get_price(self, order_book_id, start, end, freq):
        """
        :param order_book_id: e.g. 000002.XSHE
        :param start: 20160101
        :param end: 20160201
        :returns:
        :rtype: numpy.rec.array
        """
        start = get_str_date_from_int(start)
        end = get_str_date_from_int(end)
        code = self.convert_code(order_book_id)
        freq = '1d'
        is_index = False
        # if ((order_book_id.startswith("0") and order_book_id.endswith(".XSHG")) or
        #     (order_book_id.startswith("3") and order_book_id.endswith(".XSHE"))
        #     ):
        #     is_index = True
        ktype = freq
        if freq[-1] == "m":
            ktype = freq[:-1]
        elif freq == "1d":
            ktype = "D"
        # else W M

        df = self.mongo.get_stock_day(code, st_start=start, st_end=end)

        if freq[-1] == "m":
            df["datetime"] = df.apply(
                lambda row: int(row["date"].split(" ")[0].replace("-", "")) * 1000000 + int(row["date"].split(" ")[1].replace(":", "")) * 100, axis=1)
        elif freq in ("1d", "W", "M"):
            df["datetime"] = df["date"].apply(lambda x: int(x.replace("-", "")) * 1000000)

        del df["code"]
        arr = df.to_records()

        return arr

    @lru_cache()
    def get_order_book_id_list(self):
        """获取所有的股票代码列表
        """
        info = self.mongo.get_stock_list()
        code_list = info.code.sort_values().tolist()
        return code_list
        # order_book_id_list = [
            # (code + ".XSHG" if code.startswith("6") else code + ".XSHE")
            # for code in code_list
        # ]
        # return order_book_id_list

    @lru_cache()
    def get_trading_dates(self, start, end):
        """获取所有的交易日

        :param start: 20160101
        :param end: 20160201
        """
        start = get_str_date_from_int(start)
        end = get_str_date_from_int(end)
        df = self.mongo.get_k_data("000001", index=True, start=start, end=end)
        trading_dates = [get_int_date(date) for date in df.date.tolist()]
        return trading_dates

    @lru_cache(maxsize=4096)
    def symbol(self, order_book_id):
        """获取order_book_id对应的名字
        :param order_book_id str: 股票代码
        :returns: 名字
        :rtype: str
        """
        code = self.convert_code(order_book_id)
        return "{}[{}]".format(order_book_id, self.code_name_map.get(code))
