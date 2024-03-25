import glob
import logging
from pathlib import Path

import lxml.etree as ET


def main():
    for file in glob.glob("/data/rgd-knowledge-graph/pubtator3/local/aioner/*.bioc"):
        try:
            with open(file, "r") as f:
                ET.parse(f)
        except (ET.XMLSyntaxError):
            logging.error(f"Error parsing {file}")
            Path(file).unlink()


if __name__ == "__main__":
    main()
