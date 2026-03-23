import logging
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def get_logger(name: str) -> logging.Logger:
    """Create and return a logger with the specified name."""
    logger = logging.getLogger(name)

    #Prevent adding multiple handlers if logger already has handlers
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        #File handler
        file_handler = logging.FileHandler(LOG_DIR/ f"{name}.log", encoding="utf-8")
        file_handler.setLevel(logging.INFO)

        #Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        #Define formatter
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        #Attach both handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger