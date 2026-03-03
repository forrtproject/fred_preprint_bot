# flora_preprint_notifier — Claude Notes

## Running the pipeline locally

Always pre-load `.env` before running the pipeline, because `extract_author_list.py`
initialises `_api_cache = ApiCacheRepo()` at module import time — before the normal
dotenv loading in `iter_preprints.py` kicks in. Without pre-loading, DynamoDB calls
hit the wrong table name and throw `ResourceNotFoundException`.

```bash
set -a && source .env && set +a && python -m osf_sync.pipeline run --stage author ...
```

`set -a` makes every variable defined by `source .env` automatically exported to child
processes (including Python's `os.environ`). `set +a` turns that off again afterwards.
