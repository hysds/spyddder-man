#!/usr/bin/env python
import os
import sys
import logging
import argparse

from sciflo_util import run_sciflo


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


BASE_PATH = os.path.dirname(__file__)


def main(sfl_file, context_file):
    """Main."""

    sfl_file = os.path.abspath(sfl_file)
    context_file = os.path.abspath(context_file)
    logger.info("sfl_file: %s" % sfl_file)
    logger.info("context_file: %s" % context_file)
    return run_sciflo(sfl_file, ["context_file=%s" % context_file])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sfl_file", help="SciFlo workflow")
    parser.add_argument("context_file", help="HySDS context file")
    args = parser.parse_args()
    sys.exit(main(args.sfl_file, args.context_file))
