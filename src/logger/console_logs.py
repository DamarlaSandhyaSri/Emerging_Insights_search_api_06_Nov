import logging


class Loggercheck:

    _instance = None

    def __new__(cls, name: str):
        if cls._instance is None:
            cls._instance = super(Loggercheck, cls).__new__(cls)
            cls._instance._initialize(name)
        return cls._instance

    def _initialize(self, name: str):
        self.log = logging.getLogger(name)
        self.log.setLevel(logging.DEBUG)

        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(logging.DEBUG)

        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.console_handler.setFormatter(self.formatter)

        if not self.log.hasHandlers():
            self.log.addHandler(self.console_handler)

    def get_logger(self):
        return self.log

    def logg_message(self, message, level="info"):
        if level == "debug":
            self.log.debug(message)
        elif level == "info":
            self.log.info(message)
        elif level == "error":
            self.log.error(message)
        else:
            self.log.warning(f"Log level unknown: {level}.Message:{message}")
