# users-shards

Database shards for a real-time multi-field search API served from Tencent EdgeOne.
No localhost dependency — the build runs in the cloud and the API runs at the edge.

## Partitioned build (current approach)

* `PARTS_README.md` — full design, sizing and workflow
* `parts_builder.py` — streams `users_data.zip`, splits it into N self-contained
  searchable parts (one per hosting account), writes block-gzip bucket files
* `parts-edge.js` — EdgeOne handler that fans out to all parts and returns full
  records (search by mobile / email / name)
* `colab_parts.ipynb` — Google Colab notebook that runs the build in the cloud

### Quick start

1. Open `colab_parts.ipynb` in Colab.
2. Paste a fresh direct link to `users_data.zip`, set the number of parts, and (if
   you want) one upload command per account.
3. Run the cells. Each part is uploaded to its own account and deleted locally.
4. Put the part base URLs into `parts-edge.js` (`DEFAULT_PART_BASES`) and deploy
   `edgeone-parts-deploy.zip`.

## Legacy

* `colab_sharder.py` — earlier single-index sharder (superseded by `parts_builder.py`).
