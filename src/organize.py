import argparse
import gzip
import logging
import shutil
from datetime import datetime
from pathlib import Path

import bioc
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bioc import BioCCollection, biocxml
from bioconverters import pmcxml2bioc, pubmedxml2bioc
from matplotlib_venn import venn3
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map


def convert_abstracts(input_dir: Path, output_dir: Path):
    logging.info(f"Converting abstracts from {input_dir} to BIOC format in {output_dir}")
    pmc_xmls = list(input_dir.glob("*.xml"))
    output_dir.mkdir(exist_ok=True)

    for pmc_xml in tqdm(pmc_xmls):
        path_bioc = output_dir / pmc_xml.with_suffix(".bioc").name
        if path_bioc.exists():
            continue
        source = str(pmc_xml)
        import io
        with open(source) as f:
            text = f.read()
            text = text.replace("&plusmn;", "±")
            string_io = io.StringIO(text)
            documents = list(pubmedxml2bioc(string_io))
        if not documents:
            logging.warning(f"Could not convert {pmc_xml}")
        for document in documents:
            for passage in document.passages:
                passage.infons["type"] = passage.infons["section"]
        collection = BioCCollection.of_documents(*documents)

        with open(str(path_bioc), "w") as fp:
            biocxml.dump(collection, fp)


def convert_pmc_xml(input_dir: Path, output_dir: Path):
    logging.info(f"Converting PMC XMLs from {input_dir} to BIOC XMLs in {output_dir}")
    pmc_xmls = list(input_dir.glob("*.xml"))
    output_dir.mkdir(exist_ok=True)

    process_map(convert_pmc_xml_single, pmc_xmls, [output_dir] * len(pmc_xmls), chunksize=1, max_workers=24)


def convert_pmc_xml_single(pmc_xml: Path, output_dir: Path):
    path_bioc = output_dir / pmc_xml.with_suffix(".bioc").name
    if path_bioc.exists():
        return
    docs = pmcxml2bioc(str(pmc_xml))
    if not docs:
        logging.warning(f"Could not convert {pmc_xml}")
    for doc in docs:
        doc.encoding = "utf-8"
        doc.standalone = True

        with open(str(path_bioc), "w") as fp:
            biocxml.dump(doc, fp)
        break


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
    # pmids = set()
    # with open(bioconcepts2pubtator3_csv) as f, tqdm(total=None) as pbar:
    #     for line in f:
    #         pmid = int(line.split("\t")[0])
    #         pmids.add(pmid)
    #         inc = len(pmids) - pbar.n
    #         pbar.update(inc)
    pmids = {38060310}
    pmids = range(1, max(pmids) + 1)
    return pmids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rgd_csv", help="rgd csv", default="/data/merged.csv")
    parser.add_argument(
        "--relation2pubtator3_csv", help="relation2pubtator3 csv", default="/data/PubTator3/relation2pubtator3"
    )
    parser.add_argument(
        "--bioconcepts2pubtator3_csv", help="bioconcepts2pubtator3 csv", default="/data/PubTator3/bioconcepts2pubtator3"
    )
    parser.add_argument("--in_dir_pubmed_abstract", help="input directory", default="/data/Archive/pubmed/Archive")
    parser.add_argument("--out_dir", help="output directory", default="/data/rd-knowledge-graph/pubtator3")
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
    api_path = out_dir / "api"
    api_path.mkdir(exist_ok=True)
    api_bioconcepts2pubtator3_path = api_path / "bioconcepts2pubtator3"
    api_bioconcepts2pubtator3_path.mkdir(exist_ok=True)
    api_relation2pubtator3_path = api_path / "relation2pubtator3"
    api_relation2pubtator3_path.mkdir(exist_ok=True)

    rgd_df, rgd_pmids = get_rgd_df_pmids(args.rgd_csv)
    logging.info(f"Length of rgd_df: {len(rgd_df)}")
    logging.info(f"rgd_pmid len: {len(rgd_pmids)}")
    logging.info(f"rgd_pmid max: {max(rgd_pmids)}")
    relation2pubtator3_df, relation2pubtator3_pmids = get_relation2pubtator3_df_pmids(args.relation2pubtator3_csv)
    bioconcepts2pubtator3_pmids = get_bioconcepts2pubtator3_pmids(args.bioconcepts2pubtator3_csv)
    print(f"bioconcepts2pubtator3_pmids max {max(bioconcepts2pubtator3_pmids)}")
    # plot_venn_diagram(out_dir, rgd_pmids, relation2pubtator3_pmids, bioconcepts2pubtator3_pmids)

    relevant_pmids = relation2pubtator3_pmids & rgd_pmids

    # extract_relations(out_dir_ftp_relation2pubtator3, relation2pubtator3_df, relevant_pmids)
    # total = len([pmid for pmid in relevant_pmids if pmid in bioconcepts2pubtator3_pmids])
    # extract_bioconcepts(args.bioconcepts2pubtator3_csv, out_dir_ftp_bioconcepts2pubtator3, relevant_pmids, total)
    relevant_in_ftp  = {pmid for pmid in rgd_pmids if pmid in bioconcepts2pubtator3_pmids}
    logging.info(f"Relevant PMIDs in FTP: {len(relevant_in_ftp)}")
    relevant_but_not_in_ftp = {pmid for pmid in rgd_pmids if pmid not in bioconcepts2pubtator3_pmids}
    pubtator3_api_pmids = pull_from_pubtator3_api_batched(
        relevant_but_not_in_ftp, api_bioconcepts2pubtator3_path, api_relation2pubtator3_path
    )
    relevant_but_not_in_pubtator3 = relevant_but_not_in_ftp - pubtator3_api_pmids

    articles_pmids = copy_raw_articles(out_dir_local_raw_articles, rgd_df, relevant_but_not_in_pubtator3)
    remaining_pmids = relevant_but_not_in_pubtator3 - articles_pmids
    abstract_pmids = copy_raw_dir(in_dir_pubmed_abstract, out_dir_local_raw_abstracts, remaining_pmids)
    logging.info(f"Max Abstract PMID: {max(abstract_pmids)}")

    convert_pmc_xml(out_dir_local_raw_articles, out_dir_local_bioc)
    convert_abstracts(out_dir_local_raw_abstracts, out_dir_local_bioc)


def copy_raw_articles(out_dir_local_raw_articles, rgd_df, pmids: set):
    df = rgd_df[rgd_df["PMID"].isin(pmids)]
    df = df[df["article_path"].notnull()]

    logging.info(f"Cleaning {out_dir_local_raw_articles}")
    cleaned = 0
    for path in out_dir_local_raw_articles.glob("*.xml"):
        pmid = int(path.stem)
        if pmid not in pmids:
            path.unlink()
            cleaned += 1
    logging.info(f"Cleaned {cleaned} irrelevant PMID articles from {out_dir_local_raw_articles}")

    logging.info(f"Copying {len(df)} articles to {out_dir_local_raw_articles}")
    copied_pmids = set()
    for pmid, article_path in tqdm(zip(df["PMID"], df["article_path"]), total=len(df)):
        copied_pmid = copy_raw(Path(article_path), out_dir_local_raw_articles, pmid)
        if copied_pmid:
            copied_pmids.add(pmid)
    return copied_pmids


def copy_raw_dir(in_dir_pubmed_article: Path, out_dir_local: Path, pmids: set):
    logging.info(
        f"Cleaning {out_dir_local}"
    )
    cleaned = 0
    for path in out_dir_local.glob("*.xml"):
        pmid = int(path.stem)
        if pmid not in pmids:
            path.unlink()
            cleaned += 1
    logging.info(f"Cleaned {cleaned} irrelevant PMID articles from {out_dir_local}")

    logging.info(
        f"Copying {len(pmids)} from {in_dir_pubmed_article} to {out_dir_local}"
    )
    out_dir_local.mkdir(exist_ok=True)
    articles_pmids = set()
    for pmid in tqdm(pmids):
        padded_pmid = f"{pmid:08d}"
        in_pubmed_path = (
            in_dir_pubmed_article / padded_pmid[0:2] / padded_pmid[2:4] / padded_pmid[4:6] / f"{pmid}.xml.gz"
        )
        pmid_copied = copy_raw(in_pubmed_path, out_dir_local, pmid)
        if pmid_copied:
            articles_pmids.add(pmid_copied)
    logging.info(f"Copied {len(articles_pmids)} to {out_dir_local}")
    return articles_pmids


def copy_raw(in_pubmed_path: Path, out_dir_local: Path, pmid: int):
    try:
        out_pubmed_path = out_dir_local / f"{pmid}.xml"
        if out_pubmed_path.stat().st_size == 0:
            out_pubmed_path.unlink()
        if not out_pubmed_path.exists():
            if in_pubmed_path.suffix == ".gz":
                with gzip.open(in_pubmed_path, "rb") as f_in:
                    with open(out_pubmed_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                with open(in_pubmed_path, "rb") as f_in:
                    with open(out_pubmed_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
        return pmid
    except FileNotFoundError:
        logging.warning(f"Could not find {in_pubmed_path}")


def extract_bioconcepts(
    bioconcepts2pubtator3_csv: Path, out_dir_ftp_bioconcepts2pubtator3: Path, relevant_pmids: set, total: int
):
    logging.info(f"Cleaning {out_dir_ftp_bioconcepts2pubtator3}")
    cleaned = 0
    for path in tqdm(out_dir_ftp_bioconcepts2pubtator3.glob("*.tsv")):
        if int(path.stem) not in relevant_pmids:
            path.unlink()
            cleaned += 1
    logging.info(f"Cleaned {cleaned} irrelevant PMID bioconcepts from {out_dir_ftp_bioconcepts2pubtator3}")
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
            pbar.update(len(to_export["PMID"].unique()))
        group_by_pmid_to_tsv(out_dir_ftp_bioconcepts2pubtator3, buffer_df, progress_bar=False)
        pbar.update(len(buffer_df["PMID"].unique()))


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def pull_from_pubtator3_api_batched(pmids: set, api_bioconcepts2pubtator3_path, api_relation2pubtator3_path):
    PUBTATOR3_PMID_CUTOFF = 38506922
    fetchable_pmids = {pmid for pmid in pmids if pmid <= PUBTATOR3_PMID_CUTOFF}
    logging.info(f"{len(fetchable_pmids)} PMIDs fetchable from PubTator3 API")
    fetched_paths = set(api_bioconcepts2pubtator3_path.glob("*.tsv"))
    fetched_pmids = set(map(lambda x: int(x.stem), fetched_paths))
    logging.info(f"Cleaning {api_bioconcepts2pubtator3_path}")
    cleaned = 0
    for pmid in fetched_pmids - fetchable_pmids:
        path = api_bioconcepts2pubtator3_path / f"{pmid}.tsv"
        path.unlink()
        cleaned += 1
    logging.info(f"Cleaned {cleaned} irrelevant PMID bioconcepts from {api_bioconcepts2pubtator3_path}")
    logging.info(f"Aleady fetched {len(fetched_pmids)} PMIDs from PubTator3 API")
    not_fetched_pmids = fetchable_pmids - fetched_pmids
    not_fetched_pmids = list(map(str, not_fetched_pmids))
    logging.info(f"Fetching {len(not_fetched_pmids)} PMIDs from PubTator3 API")
    for pmid_batch in tqdm(batch(not_fetched_pmids, 100), total=len(not_fetched_pmids) // 100):
        pull_from_pubtator3_api(pmid_batch, api_bioconcepts2pubtator3_path, api_relation2pubtator3_path)
    return fetchable_pmids


def pull_from_pubtator3_api(pmids: set, api_bioconcepts2pubtator3_path, api_relation2pubtator3_path):
    url = f"https://www.ncbi.nlm.nih.gov/research/pubtator3-api/publications/export/biocxml?pmids={','.join(pmids)}&full=true"
    logging.info(url)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to pull {pmids} from PubTator3 API")
    collection = bioc.loads(response.text)
    for document in collection.documents:
        process_document_from_pubtator3_api(api_bioconcepts2pubtator3_path, api_relation2pubtator3_path, document)


def process_document_from_pubtator3_api(api_bioconcepts2pubtator3_path, api_relation2pubtator3_path, document):
    pmid = document.id
    annotations = []
    for passage in document.passages:
        if article_id_pmid := passage.infons.get("article-id_pmid"):
            pmid = article_id_pmid
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
        (api_bioconcepts2pubtator3_path / f"{pmid}.tsv").touch()
        return
    annotation_df = pd.DataFrame(annotations)
    annotation_df = (
        annotation_df.groupby(["PMID", "Type", "Concept ID", "Resource"])["Mentions"]
        .apply(lambda x: "|".join(set(x)))
        .reset_index()
    )
    annotation_df.to_csv(api_bioconcepts2pubtator3_path / f"{pmid}.tsv", sep="\t", index=False)
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
    df.to_csv(api_relation2pubtator3_path / f"{pmid}.tsv", sep="\t", index=False)


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
    logging.info(f"Cleaning {out_dir_ftp_relation2pubtator3}")
    cleaned = 0
    for path in tqdm(out_dir_ftp_relation2pubtator3.glob("*.tsv")):
        if int(path.stem) not in relevant_pmids:
            path.unlink()
            cleaned += 1
    logging.info(f"Cleaned {cleaned} irrelevant PMID relations from {out_dir_ftp_relation2pubtator3}")
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
