#!/bin/bash
python3 src/organize.py
docker compose run aioner bash -c "cd src && python AIONER_Run.py -i /data/rgd-knowledge-graph/pubtator3/local/bioc/ -m ../pretrained_models/AIONER/Bioformer-softmax-AIONER/Bioformer-softmax-AIONER -v ../vocab/AIO_label.vocab -e ALL -o /data/rgd-knowledge-graph/pubtator3/local/aioner"
python3 src/clean.py
docker compose run taggerone ./run_CellLine_BioCXML.sh /data/rgd-knowledge-graph/pubtator3/local/aioner TaggerOne-0.3.0/data /data/rgd-knowledge-graph/pubtator3/local/taggerone-cellline
docker compose run taggerone ./run_Disease_BioCXML.sh /data/rgd-knowledge-graph/pubtator3/local/aioner TaggerOne-0.3.0/data /data/rgd-knowledge-graph/pubtator3/local/taggerone-disease
docker compose run gnorm2 python3 run_batches.py /data/rgd-knowledge-graph/pubtator3/local/aioner /data/rgd-knowledge-graph/pubtator3/local/gnorm2 --batch_size 128
docker compose run nlmchem bash -c "./run_Chemical_BioCXML.sh /data/rgd-knowledge-graph/pubtator3/local/aioner CHEM_NORM/data/abbr_frequency_2020.json.gz /data/rgd-knowledge-graph/pubtator3/local/nlmchem"
mkdir -p /data/rgd-knowledge-graph/pubtator3/local/gnormplus
docker compose run gnorm2 java -Xmx60G -Xms30G -jar GNormPlus.jar /data/rgd-knowledge-graph/pubtator3/local/aioner /data/rgd-knowledge-graph/pubtator3/local/gnormplus setup.txt
mkdir -p /data/rgd-knowledge-graph/pubtator3/local/tmvar3
docker compose run tmvar3 java -Xmx5G -Xms5G -jar tmVar.jar /data/rgd-knowledge-graph/pubtator3/local/gnormplus /data/rgd-knowledge-graph/pubtator3/local/tmvar3
python3 src/merge.py
docker compose run biorex python ./scripts/run_test_pred_bulk.py /data/rgd-knowledge-graph/pubtator3/local/nlmchem /data/rgd-knowledge-graph/pubtator3/local/biorex
python3 src/ingest.py
