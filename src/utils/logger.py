# my_logger.py

import logging
from pathlib import Path
from datetime import datetime

def setup_logger(
    name: str = __name__,
    log_dir: Path = None,
    log_prefix: str = "log",
    level: int = logging.INFO,
    fmt: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
) -> logging.Logger:
    """
    Create a logger that writes both to console and optionally to a timestamped log file.

    Parameters
    ----------
    name : str
        Name of the logger
    log_dir : Path or None
        Directory to store log file (creates dir if needed). If None, no file logging.
    log_prefix : str
        Prefix for the log filename
    level : int
        Logging level
    fmt : str
        Format string for log messages

    Returns
    -------
    logging.Logger
        Configured logger
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(fmt)

    # Avoid duplicate handlers if setup_logger called multiple times
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"{log_prefix}.log"
            log_path = log_dir / log_filename

            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            logger.info(f"Logging to file: {log_path}")

    return logger
