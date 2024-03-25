import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from bioc import BioCCollection, biocxml
from bioconverters import pmcxml2bioc, pubmedxml2bioc
from matplotlib_venn import venn3
from tqdm import tqdm


def convert_abstracts(input_dir: Path, output_dir: Path):
    logging.info(f"Converting abstracts from {input_dir} to BIOC format in {output_dir}")
    pmc_xmls = list(input_dir.glob("*.xml"))
    output_dir.mkdir(exist_ok=True)

    for pmc_xml in tqdm(pmc_xmls):
        path = Path(pmc_xml)
        path_bioc = output_dir / path.with_suffix(".bioc").name
        if path_bioc.exists():
            continue
        with open(pmc_xml, 'r') as fp:
            documents = list(pubmedxml2bioc(fp.read()))
        for document in documents:
            for passage in document.passages:
                passage.infons["type"] = passage.infons["section"]
        collection = BioCCollection.of_documents(*documents)

        with open(str(path_bioc), 'w') as fp:
            biocxml.dump(collection, fp)


def convert_pmc_xml(input_dir: Path, output_dir: Path):
    logging.info(f"Converting PMC XMLs from {input_dir} to BIOC XMLs in {output_dir}")
    pmc_xmls = list(input_dir.glob("*.xml"))
    output_dir.mkdir(exist_ok=True)

    for pmc_xml in tqdm(pmc_xmls):
        path = Path(pmc_xml)
        path_bioc = output_dir / path.with_suffix(".bioc").name
        if path_bioc.exists():
            continue
        with open(pmc_xml, 'r') as fp:
            doc = next(pmcxml2bioc(fp.read()))
        doc.encoding = 'utf-8'
        doc.standalone = True

        with open(str(path_bioc), 'w') as fp:
            biocxml.dump(doc, fp)


def get_rgd_df_pmids(rgd_csv: str):
    logging.info(f"Getting PMIDs from {rgd_csv}")
    rgd_df = pd.read_csv(rgd_csv, usecols=["PMID", "article_path"], dtype={"PMID": int, "article_path": str})
    return rgd_df, set(rgd_df["PMID"])


def get_relation2pubtator3_df_pmids(relation2pubtator3_csv: str):
    logging.info(f"Getting relations and PMIDs from {relation2pubtator3_csv}")
    relation2pubtator3_df = pd.read_csv(
        relation2pubtator3_csv, sep="\t", header=None, names=["PMID", "Type", "1st", "2nd"], dtype={"PMID": int}
    )
    relation2pubtator3_pmids = set(relation2pubtator3_df["PMID"])
    return relation2pubtator3_df, relation2pubtator3_pmids


def get_bioconcepts2pubtator3_pmids(bioconcepts2pubtator3_csv: str):
    logging.info(f"Getting PMIDs from {bioconcepts2pubtator3_csv}")
    bioconcepts2pubtator3_df = pd.read_csv(
        bioconcepts2pubtator3_csv, sep="\t", header=None, names=["PMID"], usecols=["PMID"], dtype={"PMID": int}
    )
    return set(bioconcepts2pubtator3_df["PMID"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rgd_csv", help="rgd csv", default="/data/pmc-open-access-subset/merged.csv")
    parser.add_argument(
        "--relation2pubtator3_csv", help="relation2pubtator3 csv", default="/data/PubTator3/relation2pubtator3"
    )
    parser.add_argument(
        "--bioconcepts2pubtator3_csv", help="bioconcepts2pubtator3 csv", default="/data/PubTator3/bioconcepts2pubtator3"
    )
    parser.add_argument(
        "--in_dir_pubmed_abstract", help="input directory", default="/data/pmc-open-access-subset/efetch/PubmedArticle"
    )
    parser.add_argument("--out_dir", help="output directory", default="/data/rgd-knowledge-graph/pubtator3")
    args = parser.parse_args()

    in_dir_pubmed_abstract = Path(args.in_dir_pubmed_abstract)

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    logs_path = Path("logs")
    logs_path.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(logs_path / datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log"))
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir_ftp = out_dir / "ftp"
    out_dir_ftp.mkdir(parents=True, exist_ok=True)
    out_dir_ftp_relation2pubtator3 = out_dir_ftp / "relation2pubtator3"
    out_dir_ftp_relation2pubtator3.mkdir(parents=True, exist_ok=True)
    out_dir_ftp_bioconcepts2pubtator3 = out_dir_ftp / "bioconcepts2pubtator3"
    out_dir_ftp_bioconcepts2pubtator3.mkdir(parents=True, exist_ok=True)
    out_dir_local = out_dir / "local"
    out_dir_local.mkdir(parents=True, exist_ok=True)
    out_dir_local_raw = out_dir_local / "raw"
    out_dir_local_raw.mkdir(parents=True, exist_ok=True)
    out_dir_local_raw_articles = out_dir_local_raw / "articles"
    out_dir_local_raw_articles.mkdir(parents=True, exist_ok=True)
    out_dir_local_raw_abstracts = out_dir_local_raw / "abstracts"
    out_dir_local_raw_abstracts.mkdir(parents=True, exist_ok=True)
    out_dir_local_bioc = out_dir_local / "bioc"
    out_dir_local_bioc.mkdir(parents=True, exist_ok=True)

    rgd_df, rgd_pmids = get_rgd_df_pmids(args.rgd_csv)
    relation2pubtator3_df, relation2pubtator3_pmids = get_relation2pubtator3_df_pmids(args.relation2pubtator3_csv)
    bioconcepts2pubtator3_pmids = get_bioconcepts2pubtator3_pmids(args.bioconcepts2pubtator3_csv)

    # plot_venn_diagram(out_dir, rgd_pmids, relation2pubtator3_pmids, bioconcepts2pubtator3_pmids)

    relevant_pmids = relation2pubtator3_pmids & rgd_pmids

    # extract_relations(out_dir_ftp_relation2pubtator3, relation2pubtator3_df, relevant_pmids)
    # total = len(relevant_pmids & bioconcepts2pubtator3_pmids)
    # extract_bioconcepts(args.bioconcepts2pubtator3_csv, out_dir_ftp_bioconcepts2pubtator3, relevant_pmids, total)
    relevant_but_not_in_bioconcepts2pubtator3 = relevant_pmids - bioconcepts2pubtator3_pmids
    articles_pmids = copy_raw_articles(out_dir_local_raw_articles, rgd_df, relevant_but_not_in_bioconcepts2pubtator3)
    remaining_pmids = relevant_but_not_in_bioconcepts2pubtator3 - articles_pmids
    logging.info(f"Remaining PMIDs: {len(remaining_pmids)}")
    copy_raw_dir(in_dir_pubmed_abstract, out_dir_local_raw_abstracts, remaining_pmids)

    convert_pmc_xml(out_dir_local_raw_articles, out_dir_local_bioc)
    convert_abstracts(out_dir_local_raw_abstracts, out_dir_local_bioc)

def copy_raw_articles(out_dir_local_raw_articles, rgd_df, pmids: set):
    logging.info(
        f"Copying {len(pmids)} articles to {out_dir_local_raw_articles}"
    )
    df = rgd_df[rgd_df["PMID"].isin(pmids)]
    df = df[df["article_path"].notnull()]
    copied_pmids = set()
    for pmid, article_path in tqdm(zip(df["PMID"], df["article_path"]), total=len(df)):
        copied_pmid = copy_raw(article_path, out_dir_local_raw_articles, pmid)
        if copied_pmid:
            copied_pmids.add(pmid)
    return copied_pmids


def copy_raw_dir(in_dir_pubmed_article: Path, out_dir_local: Path, relevant_but_not_in_bioconcepts2pubtator3: set):
    logging.info(
        f"Copying {len(relevant_but_not_in_bioconcepts2pubtator3)} from {in_dir_pubmed_article} to {out_dir_local}"
    )
    out_dir_local.mkdir(exist_ok=True)
    articles_pmids = set()
    for pmid in tqdm(relevant_but_not_in_bioconcepts2pubtator3):
        in_pubmed_path = in_dir_pubmed_article / f"{pmid}.xml"
        pmid_copied = copy_raw(in_pubmed_path, out_dir_local, pmid)
        if pmid_copied:
            articles_pmids.add(pmid_copied)
    logging.info(f"Copied {len(articles_pmids)} to {out_dir_local}")
    return articles_pmids


def copy_raw(in_pubmed_path: Path, out_dir_local: Path, pmid: int):
    try:
        out_pubmed_path = out_dir_local / f"{pmid}.xml"
        if not out_pubmed_path.exists():
            shutil.copy(in_pubmed_path, out_pubmed_path)
        return pmid
    except FileNotFoundError:
        logging.warning(f"Could not find {in_pubmed_path}")


def extract_bioconcepts(
    bioconcepts2pubtator3_csv: Path, out_dir_ftp_bioconcepts2pubtator3: Path, relevant_pmids: set, total: int
):
    logging.info(f"Extracting {len(relevant_pmids)} relevant PMID bioconcepts to {out_dir_ftp_bioconcepts2pubtator3}")
    with tqdm(total=total) as pbar, pd.read_csv(
        bioconcepts2pubtator3_csv,
        sep="\t",
        header=None,
        names=["PMID", "Type", "Concept ID", "Mentions", "Resource"],
        dtype={"PMID": int},
        chunksize=1e6,
    ) as reader:
        buffer_df = pd.DataFrame()
        for chunk in reader:
            chunk_pmids = set(chunk["PMID"])
            buffer_df = pd.concat([buffer_df, chunk[chunk["PMID"].isin(relevant_pmids)]])
            to_export = buffer_df[~buffer_df["PMID"].isin(chunk_pmids)]
            buffer_df = buffer_df[buffer_df["PMID"].isin(chunk_pmids)]
            group_by_pmid_to_tsv(out_dir_ftp_bioconcepts2pubtator3, to_export, False)
            pbar.update(len(to_export['PMID'].unique()))
        group_by_pmid_to_tsv(out_dir_ftp_bioconcepts2pubtator3, buffer_df, progress_bar=False)
        pbar.update(len(buffer_df['PMID'].unique()))


def group_by_pmid_to_tsv(out_dir: Path, df, progress_bar=True):
    iter = df.groupby("PMID")
    if progress_bar:
        iter = tqdm(iter)
    for pmid, group in iter:
        path = out_dir / f"{pmid}.tsv"
        if path.exists():
            continue
        group.to_csv(path, sep="\t", index=False)


def extract_relations(out_dir_ftp_relation2pubtator3: Path, relation2pubtator3_df: pd.DataFrame, relevant_pmids: set):
    logging.info(f"Extracting {len(relevant_pmids)} relevant PMID relations to {out_dir_ftp_relation2pubtator3}")
    rgd_relation2pubtator3_df = relation2pubtator3_df[relation2pubtator3_df["PMID"].isin(relevant_pmids)]
    group_by_pmid_to_tsv(out_dir_ftp_relation2pubtator3, rgd_relation2pubtator3_df)


def plot_venn_diagram(out_dir: Path, rgd_pmids: set, relation2pubtator3_pmids: set, bioconcepts2pubtator3_pmids: set):
    venn_diagram_path = out_dir / "venn.png"
    logging.info(f"Plotted venn diagram of PMIDs to {venn_diagram_path}")
    labels = ("bioconcepts2pubtator3", "rgd", "relation2pubtator3")
    plt.figure(figsize=(10, 10))
    venn3([bioconcepts2pubtator3_pmids, rgd_pmids, relation2pubtator3_pmids], labels)
    plt.savefig(venn_diagram_path)


if __name__ == "__main__":
    main()
