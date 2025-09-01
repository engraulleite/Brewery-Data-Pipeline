import logging
from logging import StreamHandler, Formatter
from datetime import datetime
from pytz import timezone


class CustomLogger:
    """A custom logger class with timezone support for Brazil/São Paulo."""
    
    def __init__(self):
        self.logger = logging.getLogger("app_logger")
        self.logger.setLevel(logging.INFO)
        self._conf_logger()
    
    def _conf_logger(self):
        """Configure the logger with appropriate handlers and formatters."""
        # Prevent multiple handlers if logger was already configured
        if not self.logger.handlers:
            handler = StreamHandler()
            formatter = TzFormatter(
                fmt="%(asctime)s [%(levelname)s] %(message)s", 
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def get_logger(self):
        """Return the configured logger instance."""
        return self.logger
    
    # Proxy methods to make the class behave like a logger
    def info(self, msg, *args, **kwargs):
        """Log an info level message."""
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        """Log a warning level message."""
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        """Log an error level message."""
        self.logger.error(msg, *args, **kwargs)
    
    def debug(self, msg, *args, **kwargs):
        """Log a debug level message."""
        self.logger.debug(msg, *args, **kwargs)
    
    def critical(self, msg, *args, **kwargs):
        """Log a critical level message."""
        self.logger.critical(msg, *args, **kwargs)

class TzFormatter(Formatter):
    """Custom formatter that converts timestamps to America/Sao_Paulo timezone."""
    
    def formatTime(self, record, datefmt=None):
        """
        Format the creation time of the record to Brazil/São Paulo timezone.
        
        Args:
            record: The log record to format
            datefmt: Optional date format string
            
        Returns:
            Formatted timestamp string
        """
        # Convert timestamp to Brazil/São Paulo timezone
        dt = datetime.fromtimestamp(record.created, tz=timezone("America/Sao_Paulo"))
        
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


# Alternative: Export the class instance itself for method chaining
logger = CustomLogger()