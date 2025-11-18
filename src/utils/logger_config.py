"""
Simple, professional logger for RAG pipeline
Easy to import and use anywhere in your project

Usage:
    from logger_config import logger
    
    logger.info("Application started")
    logger.debug("Debug information")
    logger.warning("Warning message")
    logger.error("Error occurred")
    logger.success("Task completed successfully")
"""

from loguru import logger
import sys
from pathlib import Path

# Create logs directory
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True, parents=True)


def setup_logger(job_name="etl_system"):
    """
    Initialize logger with simple, professional configuration
    Call this once at application startup
    """
    
    # Prevent re-initialization
    if getattr(logger, "_initialized", False):
        return logger
    
    # Remove default handler
    logger.remove()
    
    # Console output (colored)
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{file}</cyan>:<cyan>{function}</cyan>:{line} | <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # Main log file (human-readable)
    logger.add(
        LOG_DIR / f"{job_name}.log",
        rotation="20 MB",
        retention="30 days",
        compression="zip",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}"
    )
    
    # Error-only log file
    logger.add(
        LOG_DIR / f"{job_name}_errors.log",
        rotation="10 MB",
        retention="60 days",
        compression="zip",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}"
    )
    
    # JSON log file (for log analysis tools)
    logger.add(
        LOG_DIR / f"{job_name}.json",
        rotation="20 MB",
        retention="20 days",
        compression="zip",
        level="INFO",
        serialize=True
    )
    
    logger._initialized = True
    logger.info(f"Logger initialized: {job_name}")
    
    return logger

# Initialize once when module is imported
setup_logger()