import logging
import pandas as pd
import bioc

from pathlib import Path
from tqdm import tqdm


def process_document_from_pubtator3_local(local_bioconcepts2pubtator3_path, local_relation2pubtator3_path, document):
    pmid = document.id
    annotations = []
    for passage in document.passages:
        for annotation in passage.annotations:
            annotations.append(
                {
                    "PMID": pmid,
                    "Type": annotation.infons["type"],
                    "Concept ID": annotation.infons.get("identifier"),
                    "Mentions": annotation.text,
                    "Resource": "PubTator3",
                }
            )
    if not annotations:
        (local_bioconcepts2pubtator3_path / f"{pmid}.tsv").touch()
        return
    annotation_df = pd.DataFrame(annotations)
    annotation_df = (
        annotation_df.groupby(["PMID", "Type", "Concept ID", "Resource"])["Mentions"]
        .apply(lambda x: "|".join(set(x)))
        .reset_index()
    )
    annotation_df.to_csv(local_bioconcepts2pubtator3_path / f"{pmid}.tsv", sep="\t", index=False)
    relations = []
    relation_type_map = {
        "Association": "associate",
        "Bind": "interact",
        "Cause": "cause",
        "Comparison": "compare",
        "Cotreatment": "cotreat",
        "Drug_Interaction": "drug_interact",
        "Inhibit": "inhibit",
        "Interact": "interact",
        "Negative_Correlation": "negative_correlate",
        "Positive_Correlation": "positive_correlate",
        "Prevent": "prevent",
        "Stimulate": "stimulate",
        "Treatment": "treat",
    }
    for relation in document.relations:
        try:
            relations.append(
                {
                    "PMID": pmid,
                    "Type": relation_type_map[relation.infons["type"]],
                    "1st": relation.infons["role1"],
                    "2nd": relation.infons["role2"],
                }
            )
        except KeyError as e:
            logging.error(pmid)
            raise e
    if not relations:
        return
    df = pd.DataFrame(relations)
    df.to_csv(local_relation2pubtator3_path / f"{pmid}.tsv", sep="\t", index=False)


def main():
    local_pubtator3_path = Path("/data/rgd-knowledge-graph/pubtator3/local/pubtator3")
    local_bioconcepts2pubtator3_path = Path("/data/rgd-knowledge-graph/pubtator3/local/bioconcepts2pubtator3")
    local_bioconcepts2pubtator3_path.mkdir(exist_ok=True)
    local_relation2pubtator3_path = Path("/data/rgd-knowledge-graph/pubtator3/local/relation2pubtator3")
    local_relation2pubtator3_path.mkdir(exist_ok=True)

    local_pubtator3_files = list(local_pubtator3_path.glob("*.bioc"))

    for bioc_file in tqdm(local_pubtator3_files):
        with open(bioc_file) as f:
            collection = bioc.load(f)
            for document in collection.documents:
                process_document_from_pubtator3_local(local_bioconcepts2pubtator3_path, local_relation2pubtator3_path, document)


if __name__ == "__main__":
    main()
