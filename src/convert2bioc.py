import logging
from pathlib import Path

from bioc import biocxml, pubtator, BioCCollection
from bioc.tools.pubtator2bioc import pubtator2bioc

from tqdm import tqdm


def main():
    local_path = Path("/data/rgd-knowledge-graph/pubtator3/local/")
    merged_path = local_path / "merged"
    biorex_path = local_path / "biorex"

    bioc_path = local_path / "pubtator3"
    bioc_path.mkdir(exist_ok=True)

    pubator_files = list(biorex_path.glob("*.pubtator"))

    for pubtator_file in tqdm(pubator_files):
        logging.info(f"Converting {pubtator_file}")
        with open(pubtator_file) as f:
            doc = pubtator.load(f)[0]

        doc = pubtator2bioc(doc)

        merged_file = merged_path / Path(pubtator_file).name.replace(".pubtator", ".bioc")
        with open(merged_file, "r") as f:
            collection = biocxml.load(f)
        document = collection.documents[0]

        role_lookup = {}
        for passage in document.passages:
            for annotation in passage.annotations:
                role_lookup[annotation.text] = annotation.infons["type"] + "|"
                if "identifier" in annotation.infons:
                    identifier = annotation.infons["identifier"]
                    if identifier == "-":
                        continue
                    role_lookup[annotation.text] += identifier
                    role_lookup[identifier] = annotation.infons["type"] + "|" + identifier

        document.relations = []

        for relation in doc.relations:
            refid = relation.nodes[0].refid
            if refid in role_lookup:
                concept_id = role_lookup[refid]
            else:
                continue
            a, b = concept_id.split("|")
            if not a or not b:
                continue
            relation.infons["role1"] = concept_id

            refid = relation.nodes[1].refid
            if refid in role_lookup:
                concept_id = role_lookup[refid]
            else:
                continue
            a, b = concept_id.split("|")
            if not a or not b:
                continue
            relation.infons["role2"] = concept_id

            document.relations.append(relation)

        bioc_file = bioc_path / Path(pubtator_file).name.replace(".pubtator", ".bioc")
        with open(bioc_file, "w") as f:
            biocxml.dump(collection, f)


if __name__ == "__main__":
    main()
