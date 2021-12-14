import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d - [%(levelname)s] %(name)s [%(module)s.%(funcName)s:%(lineno)d]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = False