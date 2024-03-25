#!/bin/bash
docker compose run aioner bash -c "cd src && python AIONER_Run.py -i /data/pmc-open-access-subset/abstracts-bioc/ -m /data/Bioformer-softmax-AIONER/Bioformer-softmax-AIONER -v ../vocab/AIO_label.vocab -e ALL -o /data/pmc-open-access-subset/abstracts-ner"
docker compose run taggerone ./run_Disease_BioCXML.sh /data/pmc-open-access-subset/abstracts-ner TaggerOne-0.3.0/data /data/pmc-open-access-subset/abstracts-taggerone-cellline
docker compose run taggerone ./run_Disease_BioCXML.sh /data/pmc-open-access-subset/abstracts-taggerone-cellline TaggerOne-0.3.0/data /data/pmc-open-access-subset/abstracts-taggerone-disease
docker compose run gnorm2 ./GNorm2.sh /data/pmc-open-access-subset/abstracts-taggerone-disease /data/pmc-open-access-subset/abstracts-gnorm2
docker compose run nlmchem ./run_Chemical_BioCXML.sh /data/pmc-open-access-subset/abstracts-taggerone-disease CHEM_NORM/data/abbr_frequency_2020.json.gz /data/pmc-open-access-subset/abstracts-nlmchem
docker compose run biorex python ./scripts/run_test_pred_bulk.py /data/pmc-open-access-subset/abstracts-pubtator /data/pmc-open-access-subset/abstracts-biorex
