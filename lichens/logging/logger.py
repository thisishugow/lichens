import logging
import string

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format:string.Template = string.Template("%(asctime)s - %(name)s - $color%(levelname)s\x1b[0m - (%(filename)s:%(lineno)d) - %(message)s ")

    FORMATS = {
        logging.DEBUG: format.substitute({"color":grey}),
        logging.INFO: format.substitute({"color":green}),
        logging.WARNING: format.substitute({"color":yellow}),
        logging.ERROR:format.substitute({"color":red}),
        logging.CRITICAL: format.substitute({"color":bold_red})
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
    
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)