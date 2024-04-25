import glob
import logging
from pathlib import Path

from bioc import biocxml

from xml.etree import ElementTree as ET
from tqdm import tqdm
from itertools import chain
from collections import defaultdict

def main():
    local_path = Path("/data/rgd-knowledge-graph/pubtator3/local/")
    merged_path = local_path / "merged"
    merged_path.mkdir(exist_ok=True)

    aioner = local_path / "aioner"
    aioner_paths = list(aioner.glob("*.bioc"))
    aioner_pmids = set([path.stem for path in aioner_paths])
    print(f"Found {len(aioner_pmids)} aioner pmids")

    gnorm2_path = local_path / "gnorm2"
    gnorm2_paths = list(gnorm2_path.glob("*.bioc"))
    gnorm2_pmids = set([path.stem for path in gnorm2_paths])
    print(f"Found {len(gnorm2_pmids)} gnorm2 pmids")

    nlmchem_path = local_path / "nlmchem"
    nlmchem_paths = list(nlmchem_path.glob("*.bioc"))
    nlmchem_pmids = set([path.stem for path in nlmchem_paths])
    print(f"Found {len(nlmchem_pmids)} nlmchem pmids")

    taggerone_cellline_path = local_path / "taggerone-cellline"
    taggerone_cellline_paths = list(taggerone_cellline_path.glob("*.bioc"))
    taggerone_cellline_pmids = set([path.stem for path in taggerone_cellline_paths])
    print(f"Found {len(taggerone_cellline_pmids)} taggerone cellline pmids")

    taggerone_disease_path = local_path / "taggerone-disease"
    taggerone_disease_paths = list(taggerone_disease_path.glob("*.bioc"))
    taggerone_disease_pmids = set([path.stem for path in taggerone_disease_paths])
    print(f"Found {len(taggerone_disease_pmids)} taggerone disease pmids")

    tmvar3_path = local_path / "tmvar3"
    tmvar3_paths = list(tmvar3_path.rglob("*.bioc.BioC.XML"))
    tmvar3_pmids = set([path.name.split(".")[0] for path in tmvar3_paths])
    print(f"Found {len(tmvar3_pmids)} tmvar3 pmids")

    pmids = gnorm2_pmids & nlmchem_pmids & taggerone_cellline_pmids & taggerone_disease_pmids & tmvar3_pmids
    print(f"Found {len(pmids)} common pmids")

    pmids = sorted(list(pmids))

    for pmid in tqdm(pmids):
        aioner_bioc = aioner / f"{pmid}.bioc"
        gnorm2_bioc = gnorm2_path / f"{pmid}.bioc"
        nlmchem_bioc = nlmchem_path / f"{pmid}.bioc"
        taggerone_cellline_bioc = taggerone_cellline_path / f"{pmid}.bioc"
        taggerone_disease_bioc = taggerone_disease_path / f"{pmid}.bioc"
        tmvar3_bioc = tmvar3_path / f"{pmid}.bioc.BioC.XML"
        merged_bioc = merged_path / f"{pmid}.bioc"
        with open(aioner_bioc, "r") as f:
            aioner_collection = biocxml.load(f)
        with open(gnorm2_bioc, "r") as f:
            gnorm2_collection = biocxml.load(f)
        with open(nlmchem_bioc, "r") as f:
            nlmchem_collection = biocxml.load(f)
        with open(taggerone_cellline_bioc, "r") as f:
            taggerone_cellline_collection = biocxml.load(f)
        with open(taggerone_disease_bioc, "r") as f:
            taggerone_disease_collection = biocxml.load(f)
        with open(tmvar3_bioc, "r") as f:
            tmvar3_collection = biocxml.load(f)
        for aioner_document, gnorm2_document, nlmchem_document, taggerone_cellline_document, taggerone_disease_document, tmvar3_document in zip(
            aioner_collection.documents,
            gnorm2_collection.documents,
            nlmchem_collection.documents,
            taggerone_cellline_collection.documents,
            taggerone_disease_collection.documents,
            tmvar3_collection.documents,
        ):
            for aioner_passage, gnorm2_passage, nlmchem_passage, taggerone_cellline_passage, taggerone_disease_passage, tmvar3_passage in zip(
                aioner_document.passages,
                gnorm2_document.passages,
                nlmchem_document.passages,
                taggerone_cellline_document.passages,
                taggerone_disease_document.passages,
                tmvar3_document.passages,
            ):
                annotations_grouped_by_type = defaultdict(list)
                for annotation in aioner_passage.annotations:
                    annotations_grouped_by_type[annotation.infons.get("type")].append(annotation)

                gnorm2_gene_annotations_queue = list(gnorm2_passage.annotations)
                pointer_last_matched = 0
                for aioner_annotation in annotations_grouped_by_type["Gene"]:
                    pointer_scanning = pointer_last_matched
                    while pointer_scanning < len(gnorm2_gene_annotations_queue):
                        gnorm2_annotation = gnorm2_gene_annotations_queue[pointer_scanning]
                        if gnorm2_annotation.locations[0].offset == aioner_annotation.locations[0].offset and gnorm2_annotation.locations[0].length == aioner_annotation.locations[0].length and gnorm2_annotation.infons.get("type") == "Gene" and "NCBI Gene" in gnorm2_annotation.infons:
                            aioner_annotation.infons["NCBI Gene"] = gnorm2_annotation.infons["NCBI Gene"]
                            pointer_last_matched = pointer_scanning + 1
                            break
                        pointer_scanning += 1

                gnorm2_species_annotations_queue = list(gnorm2_passage.annotations)
                pointer_last_matched = 0
                for aioner_annotation in annotations_grouped_by_type["Species"]:
                    pointer_scanning = pointer_last_matched
                    while pointer_scanning < len(gnorm2_species_annotations_queue):
                        gnorm2_annotation = gnorm2_species_annotations_queue[pointer_scanning]
                        if gnorm2_annotation.locations[0].offset == aioner_annotation.locations[0].offset and gnorm2_annotation.locations[0].length == aioner_annotation.locations[0].length and gnorm2_annotation.infons.get("type") == "Species" and "NCBI Taxonomy" in gnorm2_annotation.infons:
                            aioner_annotation.infons["NCBI Taxonomy"] = gnorm2_annotation.infons["NCBI Taxonomy"]
                            pointer_last_matched = pointer_scanning + 1
                            break
                        pointer_scanning += 1

                chemical_annotations_queue = list(nlmchem_passage.annotations)
                pointer_last_matched = 0
                for aioner_annotation in annotations_grouped_by_type["Chemical"]:
                    pointer_scanning = pointer_last_matched
                    while pointer_scanning < len(chemical_annotations_queue):
                        chemical_annotation = chemical_annotations_queue[pointer_scanning]
                        if chemical_annotation.locations[0].offset == aioner_annotation.locations[0].offset and chemical_annotation.locations[0].length == aioner_annotation.locations[0].length and chemical_annotation.infons.get("type") == "Chemical" and "identifier" in chemical_annotation.infons:
                            aioner_annotation.infons["identifier"] = chemical_annotation.infons["identifier"]
                            pointer_last_matched = pointer_scanning + 1
                            break
                        pointer_scanning += 1

                taggerone_cellline_annotations_queue = list(taggerone_cellline_passage.annotations)
                pointer_last_matched = 0
                for aioner_annotation in annotations_grouped_by_type["CellLine"]:
                    pointer_scanning = pointer_last_matched
                    while pointer_scanning < len(taggerone_cellline_annotations_queue):
                        taggerone_cellline_annotation = taggerone_cellline_annotations_queue[pointer_scanning]
                        if taggerone_cellline_annotation.locations[0].offset == aioner_annotation.locations[0].offset and taggerone_cellline_annotation.locations[0].length == aioner_annotation.locations[0].length and taggerone_cellline_annotation.infons.get("type") == "CellLine" and "identifier" in taggerone_cellline_annotation.infons:
                            aioner_annotation.infons["identifier"] = taggerone_cellline_annotation.infons["identifier"]
                            pointer_last_matched = pointer_scanning + 1
                            break
                        pointer_scanning += 1

                taggerone_disease_annotations_queue = list(taggerone_disease_passage.annotations)
                pointer_last_matched = 0
                for aioner_annotation in annotations_grouped_by_type["Disease"]:
                    pointer_scanning = pointer_last_matched
                    while pointer_scanning < len(taggerone_disease_annotations_queue):
                        taggerone_disease_annotation = taggerone_disease_annotations_queue[pointer_scanning]
                        if taggerone_disease_annotation.locations[0].offset == aioner_annotation.locations[0].offset and taggerone_disease_annotation.locations[0].length == aioner_annotation.locations[0].length and taggerone_disease_annotation.infons.get("type") == "Disease" and "identifier" in taggerone_disease_annotation.infons:
                            aioner_annotation.infons["identifier"] = taggerone_disease_annotation.infons["identifier"]
                            pointer_last_matched = pointer_scanning + 1
                            break
                        pointer_scanning += 1

                tmvar3_annotations_queue = list(tmvar3_passage.annotations)
                pointer_last_matched = 0
                for aioner_annotation in annotations_grouped_by_type["Variant"]:
                    pointer_scanning = pointer_last_matched
                    while pointer_scanning < len(tmvar3_annotations_queue):
                        tmvar3_annotation = tmvar3_annotations_queue[pointer_scanning]
                        if tmvar3_annotation.locations[0].offset == aioner_annotation.locations[0].offset and tmvar3_annotation.locations[0].length == aioner_annotation.locations[0].length and "Mutation" in tmvar3_annotation.infons.get("type", "") and "Identifier" in tmvar3_annotation.infons:
                            aioner_annotation.infons["identifier"] = tmvar3_annotation.infons["Identifier"]
                            pointer_last_matched = pointer_scanning + 1
                            break
                        pointer_scanning += 1

        with open(merged_bioc, "w") as f:
            biocxml.dump(aioner_collection, f)


if __name__ == "__main__":
    main()
