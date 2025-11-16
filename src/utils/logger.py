# # my_logger.py

# import logging
# from pathlib import Path
# from datetime import datetime

# def setup_logger(
#     name: str = __name__,
#     log_dir: Path = None,
#     log_prefix: str = "log",
#     level: int = logging.INFO,
#     fmt: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
# ) -> logging.Logger:
#     """
#     Create a logger that writes both to console and optionally to a timestamped log file.

#     Parameters
#     ----------
#     name : str
#         Name of the logger
#     log_dir : Path or None
#         Directory to store log file (creates dir if needed). If None, no file logging.
#     log_prefix : str
#         Prefix for the log filename
#     level : int
#         Logging level
#     fmt : str
#         Format string for log messages

#     Returns
#     -------
#     logging.Logger
#         Configured logger
#     """

#     logger = logging.getLogger(name)
#     logger.setLevel(level)

#     formatter = logging.Formatter(fmt)

#     # Avoid duplicate handlers if setup_logger called multiple times
#     if not logger.handlers:
#         # Console handler
#         console_handler = logging.StreamHandler()
#         console_handler.setFormatter(formatter)
#         logger.addHandler(console_handler)

#         if log_dir:
#             log_dir.mkdir(parents=True, exist_ok=True)
#             # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             log_filename = f"{log_prefix}.log"
#             log_path = log_dir / log_filename

#             file_handler = logging.FileHandler(log_path, encoding="utf-8")
#             file_handler.setFormatter(formatter)
#             logger.addHandler(file_handler)

#             logger.info(f"Logging to file: {log_path}")

#     return logger
# from loguru import logger
# import os
# from datetime import datetime

# LOG_DIR = "logs"
# if not os.path.exists(LOG_DIR):
#     os.makedirs(LOG_DIR)

# def init_logger(job_name="etl"):
#     # if len(logger._core.handlers) > 0:
#     #     return logger
#     # Prevent re-initialization
#     if getattr(logger, "_is_configured", False):
#         return logger

#     logger.remove()
#     # Human-readable log
#     logger.add(
#         f"{LOG_DIR}/{job_name}.log",
#         rotation="20 MB",
#         retention="20 days",
#         compression="zip",
#         level="INFO",
#         enqueue=True,
#         colorize=True,
#         format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} | {message}"
#     )

#     # JSON structured logs
#     logger.add(
#         f"{LOG_DIR}/{job_name}.json",
#         rotation="20 MB",
#         retention="10 days",
#         serialize=True,  # JSON format
#         compression="zip",
#         level="INFO",
#         enqueue=True,
#     )

#     # Console
#     logger.add(lambda msg: print(msg, end=""), level="INFO")

#     # logger.info(f"Logging initialized for job '{job_name}'")
#     logger._is_configured = True
#     return logger

from loguru import logger
import sys
import os
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_PATH = "logs/etl.json"
job_name="etl"
def init_logger() :
    # Prevent reconfiguring
    if getattr(logger, "_is_configured", False):
        return logger

    # Remove default handler
    logger.remove()

    # -------- CONSOLE (colorized) -------- #
    logger.add(
        f"{LOG_DIR}/{job_name}.log",
        enqueue=True,
        colorize=True,    # <--- THIS ENABLES COLORS
        level="INFO",
        backtrace=True,
        diagnose=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
               "<level>{level}</level> "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
               "<level>{message}</level>",
    )

    # -------- JSON FILE LOGS -------- #
    # logger.add(
    #     f"{LOG_DIR}/{job_name}.json",
    #     serialize=True,   # JSON
    #     enqueue=True,
    #     level="INFO",
    #     rotation="10 MB",
    #     retention="7 days"
    # )
    # # Human-readable log
    # logger.add(
    #     f"{LOG_DIR}/{job_name}.log",
    #     rotation="20 MB",
    #     retention="20 days",
    #     compression="zip",
    #     level="INFO",
    #     enqueue=True,
    #     colorize=True,
    #     format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} | {message}"
    # )

    # # JSON structured logs
    logger.add(
        f"{LOG_DIR}/{job_name}.json",
        rotation="20 MB",
        retention="10 days",
        serialize=True,  # JSON format
        compression="zip",
        level="INFO",
        enqueue=True,
    )
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    # mark as configured
    logger._is_configured = True
    return logger


etl_logger=init_logger()
