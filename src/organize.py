import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib_venn import venn3
from tqdm import tqdm


def get_rgd_pmids(rgd_csv: str):
    logging.info(f"Getting PMIDs from {rgd_csv}")
    rgd_df = pd.read_csv(rgd_csv, usecols=["PMID"], dtype={"PMID": int})
    return set(rgd_df["PMID"])


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
        "--in_dir_pubmed", help="input directory", default="/data/pmc-open-access-subset/efetch/PubmedArticle"
    )
    parser.add_argument("--out_dir", help="output directory", default="/data/rgd-knowledge-graph/pubtator3")
    args = parser.parse_args()

    in_dir_pubmed = Path(args.in_dir_pubmed)

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

    rgd_pmids = get_rgd_pmids(args.rgd_csv)
    relation2pubtator3_df, relation2pubtator3_pmids = get_relation2pubtator3_df_pmids(args.relation2pubtator3_csv)
    bioconcepts2pubtator3_pmids = get_bioconcepts2pubtator3_pmids(args.bioconcepts2pubtator3_csv)

    # plot_venn_diagram(out_dir, rgd_pmids, relation2pubtator3_pmids, bioconcepts2pubtator3_pmids)

    relevant_pmids = relation2pubtator3_pmids & rgd_pmids

    # extract_relations(out_dir_ftp_relation2pubtator3, relation2pubtator3_df, relevant_pmids)
    # total = len(relevant_pmids & bioconcepts2pubtator3_pmids)
    # extract_bioconcepts(args.bioconcepts2pubtator3_csv, out_dir_ftp_bioconcepts2pubtator3, relevant_pmids, total)
    relevant_but_not_in_bioconcepts2pubtator3 = relevant_pmids - bioconcepts2pubtator3_pmids
    copy_pubmed_articles(in_dir_pubmed, out_dir_local, relevant_but_not_in_bioconcepts2pubtator3)


def copy_pubmed_articles(in_dir_pubmed: Path, out_dir_local: Path, relevant_but_not_in_bioconcepts2pubtator3: set):
    logging.info(
        f"Copying {len(relevant_but_not_in_bioconcepts2pubtator3)} PubMedArticles from {in_dir_pubmed} to {out_dir_local}"
    )
    for pmid in tqdm(relevant_but_not_in_bioconcepts2pubtator3):
        try:
            in_pubmed_path = in_dir_pubmed / f"{pmid}.xml"
            out_pubmed_path = out_dir_local / f"{pmid}.xml"
            shutil.copy(in_pubmed_path, out_pubmed_path)
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
