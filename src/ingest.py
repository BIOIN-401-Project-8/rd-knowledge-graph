import argparse
import asyncio
import glob
import itertools
import logging
import os
from datetime import datetime
from pathlib import Path

import lxml.etree as ET
import neo4j
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map


async def run_relation_query(session: neo4j.AsyncSession, file: str, date_pmid_lookup: dict):
    df = pd.read_csv(file, sep="\t")
    df[["1st Type", "1st Concept ID"]] = df["1st"].str.split("|", expand=True)
    df[["2nd Type", "2nd Concept ID"]] = df["2nd"].str.split("|", expand=True)
    df.drop(columns=["1st", "2nd"], inplace=True)

    escape_quote = lambda x: x.replace("'", "\\'")
    df["1st Concept ID"] = df["1st Concept ID"].apply(escape_quote)
    df["2nd Concept ID"] = df["2nd Concept ID"].apply(escape_quote)
    df["PubDate"] = df["PMID"].map(date_pmid_lookup)
    # queries = []
    # for i, row in df.iterrows():
    #     queries.append(
    #         f"""
    #             MATCH (a{i}:`{row['1st Type']}`:Pubtator3 {{ConceptID: '{row['1st Concept ID']}', source: '{source}'}})
    #             MATCH (b{i}:`{row['2nd Type']}`:Pubtator3 {{ConceptID: '{row['2nd Concept ID']}', source: '{source}'}})
    #             MERGE (a{i})-[:`{row['Type']}_PubTator3` {{PMID: {row['PMID']}, PubDate: {row['PubDate']}, source: '{source}'}}]->(b{i})
    #         """
    #     )
    for node_1st_type, node_2nd_type, relation_type, df_type in df.groupby(["1st Type", "2nd Type"]):
        query = f"""
            CALL apoc.periodic.iterate(
                "UNWIND $rows as row RETURN row",
                "MATCH (a:`{node_1st_type}`:Pubtator3 {{ConceptID: row['1st Concept ID']}})
                MATCH (b:`{node_2nd_type}`:Pubtator3 {{ConceptID: row['2nd Concept ID']}})
                MERGE (a)-[:`{relation_type}_PubTator3` {{PMID: row['PMID'], PubDate: row['PubDate']}}]->(b)",
                {{batchSize: 1000, batchMode: "BATCH", parallel: true, params: {{rows: $rows}}}}
            )
        """
        await run_query(session, query, rows=df_type.to_dict("records"))


async def run_relation_queries(session: neo4j.AsyncSession, input_dirs: list[str]):
    files = []
    for input_dir in input_dirs:
        for file in tqdm(glob.glob(f"{input_dir}/*.tsv")):
            files.append(file)
    pmids = [Path(file.stem) for file in files]
    date_pmid_lookup = get_pmid_date_lookup(pmids)
    for file in tqdm(files):
        await run_relation_query(session, file, date_pmid_lookup)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def item_to_list(x):
    if isinstance(x, str):
        return x.split("|")
    elif isinstance(x, list):
        return x
    else:
        return [x]


def unique_list(xs):
    return sorted(
        set(
            itertools.chain.from_iterable(

                map(item_to_list, xs.dropna())
            )
        )
    )


def agg_bioconcepts(df: pd.DataFrame):
    df = df.groupby("Concept ID").agg(
        {
            "PMID": unique_list,
            "Type": "first",
            "Mentions": unique_list,
            "Resource": unique_list,
        }
    )
    df = df.reset_index()
    return df



def get_pmid_date_lookup(pmids):
    lookup_table = {}
    for pmid in tqdm(pmids):
        date = None
        try:
            with open(f"/data/pmc-open-access-subset/efetch/PubmedArticle/{pmid}.xml") as f:
                tree = ET.parse(f)
                root = tree.getroot()
                published_date = root.find(".//PubDate")
                from datetime import datetime
                if published_date is not None:
                    try:
                        year = published_date.find('Year')
                        year = year.text if year is not None else None
                        month = published_date.find('Month')
                        month = month.text if month is not None else None
                        day = published_date.find('Day')
                        day = day.text if day is not None else None
                        if year and month and day:
                            date = datetime.strptime(
                                f"{year}-{month}-{day}",
                                "%Y-%b-%d",
                            )
                        elif year and month:
                            date = datetime.strptime(
                                f"{year}-{month}",
                                "%Y-%b",
                            )
                        elif year:
                            date = datetime.strptime(
                                f"{year}",
                                "%Y",
                            )
                    except AttributeError:
                        logging.exception(f"Error parsing date for {pmid}")
        except FileNotFoundError:
            pass
        lookup_table[pmid] = datetime.strftime(date, "%Y-%m-%d") if date else None
    return lookup_table


def load_bioconcepts_queries_df(file: str):
    df_file = pd.read_csv(file, sep="\t")
    df_file = df_file[df_file["Concept ID"] != "-"]
    return df_file


async def run_bioconcepts_queries(session: neo4j.AsyncSession, input_dirs: list[str]):
    files = []
    for input_dir in input_dirs:
        files.extend(glob.glob(f"{input_dir}/*.tsv"))
    files = sorted(files)
    df = pd.DataFrame()
    for files_batch in tqdm(batch(files, n=32000), total=len(files) // 32000 + 1):
        dfs = process_map(load_bioconcepts_queries_df, files_batch, chunksize=1000, max_workers=32, disable=True)
        df = pd.concat([df] + dfs)
        del dfs
        df = agg_bioconcepts(df)
    df.to_csv("/data/rgd-knowledge-graph/aggbioconcepts2pubtator3.tsv", sep="\t", index=False)
    for node_type, df_type in tqdm(df.groupby("Type")):
        query = f"""
            CALL apoc.periodic.iterate(
                "UNWIND $rows as row RETURN row",
                "MERGE (a:`Pubtator3`:`{node_type}` {{ConceptID: row['Concept ID'], Mentions: row['Mentions'], PMID: row['PMID'], Resource: row['Resource']}})",
                {{batchSize: 1000, batchMode: "BATCH", parallel: true, params: {{rows: $rows}}}}
            )
        """
        await run_query(session, query, rows=df_type.to_dict("records"))


async def run_query(session: neo4j.AsyncSession, query: str, **kwargs):
    logging.debug(query)
    await session.run(query, **kwargs)


async def main():
    parser = argparse.ArgumentParser(description="ingest")
    parser.add_argument("--neo4j_uri", help="neo4j uri", default="bolt://neo4j:7687")
    parser.add_argument("--neo4j_user", help="neo4j user", default="neo4j")
    parser.add_argument("--neo4j_password", help="neo4j password", default=os.environ.get("NEO4J_PASSWORD"))
    parser.add_argument("--neo4j_database", help="neo4j database", default="neo4j")
    parser.add_argument(
        "--input_relation_dirs",
        nargs="+",
        help="input directory",
        default=["/data/rgd-knowledge-graph/pubtator3/ftp/relation2pubtator3"],
    )
    parser.add_argument(
        "--input_bioconcepts_dirs",
        nargs="+",
        help="input directory",
        default=["/data/rgd-knowledge-graph/pubtator3/ftp/bioconcepts2pubtator3"],
    )
    args = parser.parse_args()

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logs_path = Path("logs")
    logs_path.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG, format=log_format, filename=logs_path / datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console)

    async with neo4j.AsyncGraphDatabase.driver(
        uri=args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password), database=args.neo4j_database
    ) as driver:
        async with driver.session(database=args.neo4j_database) as session:
            # await run_bioconcepts_queries(session, args.input_bioconcepts_dirs)
            await run_relation_queries(session, args.input_relation_dirs)


if __name__ == "__main__":
    asyncio.run(main())
