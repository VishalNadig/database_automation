"""Log handler module for managing application logs."""
import logging


class Logger():
    """A Logger class to handle logging for the applications.
    """
    def __init__(self, name: str = "log_handler", level=logging.INFO, filename:str = "app.log"):
        self.level = level
        self.filename = filename
        self.name = name
        logging.basicConfig(
            level=self.level,
            filemode="a",
            filename=self.filename,
            format="%(asctime)s;%(levelname)s;%(message)s",
        )


    def get_logger(self) -> logging.Logger:
        """Get a logger instance.

        Args:
            name (str, optional): Name of the logger. Defaults to __name__.

        Returns:
            logging.Logger: Logger instance.
        """
        return logging.getLogger(name=self.name)


    def log(self, message: str, level: str = "info"):
        """Log a message with a specified level.

        Args:
            message (str): Message to log.
            level (str, optional): Log level. Defaults to "info".
        """
        logger = self.get_logger()
        if level == "info":
            logger.info(f"{self.name}: " + message)
        elif level == "warning":
            logger.warning(f"{self.name}: " + message)
        elif level == "error":
            logger.error(f"{self.name}: " + message)
        elif level == "debug":
            logger.debug(f"{self.name}: " + message)
        elif level == "critical":
            logger.critical(f"{self.name}: " + message)
        elif level == "exception":
            logger.exception(f"{self.name}: " + message)
        else:
            logger.info(f"{self.name}: " + message)
        return logger


if __name__ == "__main__":
    log_handler = Logger()
    log_handler.log(message="This is an info message.", level="info")
    log_handler.log(message="This is an warning message.", level="warning")
