import configparser
import sys


class my_parser(configparser.ConfigParser):
    def as_dict(self):
        d = dict(self._sections)
        for k in d:
            d[k] = dict(d[k])
        return d


def parse_args(path):
    parser = my_parser()
    parser.read(path, encoding="utf-8")
    config = parser.as_dict()
    return config
