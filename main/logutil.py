#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging


def get_logger(name, level=logging.INFO, format_str='%(asctime)s - %(levelname)s:%(name)s:%(module)s:%(lineno)d:   %(message)s'):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(format_str)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)

    return logger