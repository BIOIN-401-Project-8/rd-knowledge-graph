# rgd-knowledge-graph
Rare genetic diseases knowledge graph

## Usage

0. Create a .env file to mount the desired volume. For example, to mount
   `/mnt/deepmind/rd-data` on your local machine to `/data` in the container.
```bash
DATA_PATH=/mnt/deepmind/rd-data
```

1. Run the scripts
```bash
docker compose run devcontainer python3 src/organize.py
source run.sh
python ingest.py
```
