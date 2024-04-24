import glob
import logging
from pathlib import Path

from bioc import biocxml

from xml.etree import ElementTree as ET
from tqdm import tqdm


def main():
    for file in tqdm(glob.glob("/data/rgd-knowledge-graph/pubtator3/local/aioner/*.bioc")):
        path = Path(file)
        pmid = path.stem
        try:
            with open(path, "r") as f:
                collection = biocxml.load(f)
        except ET.ParseError:
            logging.error(f"Error parsing {file}")
            path.unlink()

        edited = False
        for document in collection.documents:
            if not document.id:
                logging.error(f"Empty id in {file}")
                document.id = pmid
                edited = True
        if edited:
            with open(path, "w") as f:
                biocxml.dump(collection, f)


if __name__ == "__main__":
    main()
