"""
Behaviour:
  * writes to stdout  -> `docker compose logs` works, no extra setup
  * LOG_LEVEL=DEBUG   -> turn on debug lines (default INFO)
  * LOG_FILE=1        -> also mirror to logs/<component>.log, so the
                         H1..H9 story can be grepped after a demo run

Call setup_logging() once per component at the top of main().
"""

import logging
import os
import sys

# 14:32:01.123 [manager] [H4] message...
_LOG_FORMAT = "%(asctime)s.%(msecs)03d [%(name)s] %(message)s"
_TIME_FORMAT = "%H:%M:%S"

# components already configured (guards against duplicate handlers)
_configured = set()


def setup_logging(component, level=None):
    """Configure and return the logger for one component.

    component : short name shown in brackets, e.g. "client", "manager",
                "worker-1", "dashboard"
    level     : optional "DEBUG"/"INFO"/... ; falls back to $LOG_LEVEL,
                then INFO.
    """
    logger = logging.getLogger(component)

    if component in _configured:          # already set up -> do nothing
        return logger
    _configured.add(component)

    chosen = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logger.setLevel(getattr(logging, chosen, logging.INFO))

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_TIME_FORMAT)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if os.environ.get("LOG_FILE", "0") == "1":
        os.makedirs("logs", exist_ok=True)
        logfile = logging.FileHandler(
            os.path.join("logs", f"{component}.log"), delay=True
        )
        logfile.setFormatter(formatter)
        logger.addHandler(logfile)

    logger.propagate = False   # don't double-print via the root logger
    return logger


def event(logger, tag, message):
    """Log one demo event with its H-tag so it is easy to grep.

    event(log, "H4", f"received job {job_id}")
        -> 14:32:01.123 [worker-1] [H4] received job job-17
    """
    logger.info(f"[{tag}] {message}")