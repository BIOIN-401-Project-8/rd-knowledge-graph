import argparse
import glob
import logging
import os
from datetime import datetime
from pathlib import Path

import neo4j
import pandas as pd
from tqdm import tqdm


def get_relation_queries(file: str, source: str):
    df = pd.read_csv(file, sep="\t")
    df[["1st Type", "1st Concept ID"]] = df["1st"].str.split("|", expand=True)
    df[["2nd Type", "2nd Concept ID"]] = df["2nd"].str.split("|", expand=True)
    df.drop(columns=["1st", "2nd"], inplace=True)

    queries = []
    for i, row in df.iterrows():
        queries.append(
            f"""
                MERGE (a{i}:`{row['1st Type']}`:Pubtator3 {{ConceptID: '{row['1st Concept ID']}', source: '{source}'}})
                MERGE (b{i}:`{row['2nd Type']}`:Pubtator3 {{ConceptID: '{row['2nd Concept ID']}', source: '{source}'}})
                MERGE (a{i})-[:`{row['Type']}_PubTator3` {{PMID: {row['PMID']}, source: '{source}'}}]->(b{i})
            """
        )
    return queries


def main():
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
    args = parser.parse_args()

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logs_path = Path("logs")
    logs_path.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG, format=log_format, filename=logs_path / datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log"))
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console)

    with neo4j.GraphDatabase.driver(
        uri=args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password), database=args.neo4j_database
    ) as driver:
        with driver.session() as session:
            for input_dir in args.input_relation_dirs:
                for file in tqdm(glob.glob(f"{input_dir}/*.tsv")):
                    queries = get_relation_queries(file, input_dir)
                    for query in queries:
                        logging.debug(query)
                        session.run(query)


if __name__ == "__main__":
    main()
