import logging
from pathlib import Path

from bioc import biocxml
from bioc2pubtator import bioc2pubtator

from tqdm import tqdm


def main():
    local_path = Path("/data/rgd-knowledge-graph/pubtator3/local/")
    merged_path = local_path / "merged"
    merged_path.mkdir(exist_ok=True)

    pubtator_path = local_path / "pubtator"
    pubtator_path.mkdir(exist_ok=True)

    bioc_paths = list(merged_path.glob("*.bioc"))


    for bioc_file in tqdm(bioc_paths):
        logging.info(f"Converting {bioc_file}")
        with open(bioc_file) as f:
            collection = biocxml.load(f)
        pubtator_file = pubtator_path / Path(bioc_file).name.replace(".bioc", ".pubtator")

        assert len(collection.documents) == 1
        for doc in collection.documents:
            pubdoc = bioc2pubtator(doc)
            with open(pubtator_file, "w") as f:
                f.write(str(pubdoc))


if __name__ == "__main__":
    main()
