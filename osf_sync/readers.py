import json, gzip
from typing import Iterable, Dict, Optional

def iter_ndjson(path: str) -> Iterable[Dict]:
    opener = gzip.open if path.endswith(".gz") else open
    mode = "rt"
    with opener(path, mode, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

# Reuse your existing generator to fetch live from OSF.
from your_module_with_iter_preprints import iter_preprints_batches  # adjust import

def iter_osf_api(since_date: str, subject_text: Optional[str] = None, sort: str = "date_published"):
    for batch in iter_preprints_batches(since_date, subject_text=subject_text, batch_size=1000, sort=sort):
        for obj in batch:
            yield obj