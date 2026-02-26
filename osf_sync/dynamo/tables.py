from .client import get_dynamo_resource
from botocore.exceptions import ClientError
from copy import deepcopy
import logging
import os
import time

log = logging.getLogger(__name__)

PREPRINTS_TABLE = os.environ.get("DDB_TABLE_PREPRINTS", "preprints")
REFERENCES_TABLE = os.environ.get("DDB_TABLE_REFERENCES", "preprint_references")
TEI_TABLE = os.environ.get("DDB_TABLE_TEI", "preprint_tei")
SYNCSTATE_TABLE = os.environ.get("DDB_TABLE_SYNCSTATE", "sync_state")
API_CACHE_TABLE = os.environ.get("DDB_TABLE_API_CACHE", "api_cache")
EXCLUDED_PREPRINTS_TABLE = os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "excluded_preprints")
TRIAL_AUTHOR_NODES_TABLE = os.environ.get("DDB_TABLE_TRIAL_AUTHOR_NODES", "trial_author_nodes")
TRIAL_AUTHOR_TOKENS_TABLE = os.environ.get("DDB_TABLE_TRIAL_AUTHOR_TOKENS", "trial_author_tokens")
TRIAL_CLUSTERS_TABLE = os.environ.get("DDB_TABLE_TRIAL_CLUSTERS", "trial_clusters")
TRIAL_ASSIGNMENTS_TABLE = os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "trial_preprint_assignments")
EMAIL_SUPPRESSION_TABLE = os.environ.get("DDB_TABLE_EMAIL_SUPPRESSION", "email_suppression")
BILLING_MODE = (os.environ.get("DDB_BILLING_MODE", "PAY_PER_REQUEST") or "PAY_PER_REQUEST").strip().upper()

TABLES = {
  PREPRINTS_TABLE: {
    "KeySchema":[{"AttributeName":"osf_id","KeyType":"HASH"}],
    "AttributeDefinitions":[
        {"AttributeName":"osf_id","AttributeType":"S"},
        {"AttributeName":"date_published","AttributeType":"S"},
        {"AttributeName":"pdf_downloaded_at","AttributeType":"S"},
        {"AttributeName":"queue_pdf","AttributeType":"S"},
        {"AttributeName":"queue_grobid","AttributeType":"S"},
        {"AttributeName":"queue_extract","AttributeType":"S"},
        {"AttributeName":"queue_email","AttributeType":"S"}
    ],
    "GlobalSecondaryIndexes":[
        { "IndexName":"by_queue_pdf",
          "KeySchema":[
              {"AttributeName":"queue_pdf","KeyType":"HASH"},
              {"AttributeName":"date_published","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        },
        { "IndexName":"by_queue_grobid",
          "KeySchema":[
              {"AttributeName":"queue_grobid","KeyType":"HASH"},
              {"AttributeName":"pdf_downloaded_at","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        },
        { "IndexName":"by_queue_extract",
          "KeySchema":[
              {"AttributeName":"queue_extract","KeyType":"HASH"},
              {"AttributeName":"date_published","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        },
        { "IndexName":"by_queue_email",
          "KeySchema":[
              {"AttributeName":"queue_email","KeyType":"HASH"},
              {"AttributeName":"date_published","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        }
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  REFERENCES_TABLE: {
    "KeySchema":[
        {"AttributeName":"osf_id","KeyType":"HASH"},
        {"AttributeName":"ref_id","KeyType":"RANGE"}
    ],
    "AttributeDefinitions":[
        {"AttributeName":"osf_id","AttributeType":"S"},
        {"AttributeName":"ref_id","AttributeType":"S"}
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  TEI_TABLE: {
    "KeySchema":[{"AttributeName":"osf_id","KeyType":"HASH"}],
    "AttributeDefinitions":[{"AttributeName":"osf_id","AttributeType":"S"}],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  SYNCSTATE_TABLE: {
    "KeySchema":[{"AttributeName":"source_key","KeyType":"HASH"}],
    "AttributeDefinitions":[{"AttributeName":"source_key","AttributeType":"S"}],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  EXCLUDED_PREPRINTS_TABLE: {
    "KeySchema":[{"AttributeName":"osf_id","KeyType":"HASH"}],
    "AttributeDefinitions":[
        {"AttributeName":"osf_id","AttributeType":"S"},
        {"AttributeName":"excluded_at","AttributeType":"S"},
        {"AttributeName":"exclusion_reason","AttributeType":"S"}
    ],
    "GlobalSecondaryIndexes":[
        { "IndexName":"by_reason",
          "KeySchema":[
              {"AttributeName":"exclusion_reason","KeyType":"HASH"},
              {"AttributeName":"excluded_at","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        }
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  API_CACHE_TABLE: {
    "KeySchema":[{"AttributeName":"cache_key","KeyType":"HASH"}],
    "AttributeDefinitions":[{"AttributeName":"cache_key","AttributeType":"S"}],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  TRIAL_AUTHOR_NODES_TABLE: {
    "KeySchema":[{"AttributeName":"node_id","KeyType":"HASH"}],
    "AttributeDefinitions":[
        {"AttributeName":"node_id","AttributeType":"S"},
        {"AttributeName":"cluster_id","AttributeType":"S"}
    ],
    "GlobalSecondaryIndexes":[
        { "IndexName":"by_cluster",
          "KeySchema":[
              {"AttributeName":"cluster_id","KeyType":"HASH"},
              {"AttributeName":"node_id","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        }
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  TRIAL_AUTHOR_TOKENS_TABLE: {
    "KeySchema":[{"AttributeName":"token","KeyType":"HASH"}],
    "AttributeDefinitions":[
        {"AttributeName":"token","AttributeType":"S"},
        {"AttributeName":"node_id","AttributeType":"S"}
    ],
    "GlobalSecondaryIndexes":[
        { "IndexName":"by_node",
          "KeySchema":[
              {"AttributeName":"node_id","KeyType":"HASH"},
              {"AttributeName":"token","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        }
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  TRIAL_CLUSTERS_TABLE: {
    "KeySchema":[{"AttributeName":"cluster_id","KeyType":"HASH"}],
    "AttributeDefinitions":[
        {"AttributeName":"cluster_id","AttributeType":"S"},
        {"AttributeName":"stratum","AttributeType":"S"}
    ],
    "GlobalSecondaryIndexes":[
        { "IndexName":"by_stratum",
          "KeySchema":[
              {"AttributeName":"stratum","KeyType":"HASH"},
              {"AttributeName":"cluster_id","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        }
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  TRIAL_ASSIGNMENTS_TABLE: {
    "KeySchema":[{"AttributeName":"preprint_id","KeyType":"HASH"}],
    "AttributeDefinitions":[
        {"AttributeName":"preprint_id","AttributeType":"S"},
        {"AttributeName":"status","AttributeType":"S"},
        {"AttributeName":"assigned_at","AttributeType":"S"}
    ],
    "GlobalSecondaryIndexes":[
        { "IndexName":"by_status",
          "KeySchema":[
              {"AttributeName":"status","KeyType":"HASH"},
              {"AttributeName":"assigned_at","KeyType":"RANGE"}
          ],
          "Projection":{"ProjectionType":"ALL"},
          "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
        }
    ],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  },
  EMAIL_SUPPRESSION_TABLE: {
    "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
    "AttributeDefinitions":[{"AttributeName":"email","AttributeType":"S"}],
    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}
  }
}

TABLE_TTLS = {
    API_CACHE_TABLE: "expires_at",
}


def _normalize_billing_mode(raw: str) -> str:
    mode = (raw or "").strip().upper()
    if mode in {"PAY_PER_REQUEST", "PROVISIONED"}:
        return mode
    return "PAY_PER_REQUEST"


def _apply_billing_mode_to_spec(spec: dict, billing_mode: str) -> dict:
    out = deepcopy(spec)
    if billing_mode == "PAY_PER_REQUEST":
        out.pop("ProvisionedThroughput", None)
        for gsi in out.get("GlobalSecondaryIndexes", []) or []:
            gsi.pop("ProvisionedThroughput", None)
    else:
        out.setdefault("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5})
        for gsi in out.get("GlobalSecondaryIndexes", []) or []:
            gsi.setdefault("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5})
    return out


def ensure_tables():
    billing_mode = _normalize_billing_mode(BILLING_MODE)
    ddb = get_dynamo_resource()
    existing = {t.name for t in ddb.tables.all()}
    for name, spec in TABLES.items():
        if name not in existing:
            params = {"TableName": name, **_apply_billing_mode_to_spec(spec, billing_mode), "BillingMode": billing_mode}
            try:
                ddb.create_table(**params).wait_until_exists()
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceInUseException":
                    raise
        # Always ensure GSIs exist even for pre-existing tables
        _ensure_gsis(ddb, name, spec, billing_mode=billing_mode)
        ttl_attr = TABLE_TTLS.get(name)
        if ttl_attr:
            _ensure_ttl(ddb, name, ttl_attr)


def _ensure_gsis(ddb, table_name: str, spec: dict, *, billing_mode: str) -> None:
    """
    Ensure the table has the GSIs defined in TABLES[table_name].
    Adds any missing GSIs automatically (sequentially). No-op if all present.
    """
    client = ddb.meta.client
    try:
        desc = client.describe_table(TableName=table_name)["Table"]
    except ClientError:
        return

    existing = {g["IndexName"] for g in (desc.get("GlobalSecondaryIndexes") or [])}
    want = [g for g in spec.get("GlobalSecondaryIndexes", [])]
    missing = [g for g in want if g["IndexName"] not in existing]
    if not missing:
        return

    # Attribute definitions currently on the table
    have_attrs = {a["AttributeName"] for a in (desc.get("AttributeDefinitions") or [])}
    spec_attrs = {a["AttributeName"]: a["AttributeType"] for a in spec.get("AttributeDefinitions", [])}

    for gsi in missing:
        gsi_create = deepcopy(gsi)
        if billing_mode == "PAY_PER_REQUEST":
            gsi_create.pop("ProvisionedThroughput", None)
        else:
            gsi_create.setdefault("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5})

        # Build AttributeDefinitions for any new key attributes
        needed_attr_names = [k["AttributeName"] for k in gsi_create.get("KeySchema", [])]
        new_defs = []
        for attr in needed_attr_names:
            if attr not in have_attrs and attr in spec_attrs:
                new_defs.append({"AttributeName": attr, "AttributeType": spec_attrs[attr]})

        params = {
            "TableName": table_name,
            "GlobalSecondaryIndexUpdates": [{"Create": gsi_create}],
        }
        if new_defs:
            params["AttributeDefinitions"] = new_defs

        try:
            client.update_table(**params)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"ResourceInUseException", "ValidationException"}:
                # Likely already creating or already exists due to race; skip to next
                continue
            raise

        # Optionally, wait briefly for index to progress on local dev
        # Avoid long waits on AWS; just short sleep
        time.sleep(0.2)


def _ensure_ttl(ddb, table_name: str, ttl_attr: str) -> None:
    client = ddb.meta.client
    try:
        desc = client.describe_time_to_live(TableName=table_name).get("TimeToLiveDescription", {})
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"ValidationException", "ResourceNotFoundException"}:
            # DynamoDB Local may not support TTL or table may not be ready yet.
            log.warning("TTL not available for table", extra={"table": table_name, "error": code})
            return
        raise

    status = desc.get("TimeToLiveStatus")
    attr = desc.get("AttributeName")
    if status in {"ENABLED", "ENABLING"} and attr == ttl_attr:
        return

    try:
        client.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl_attr},
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"ValidationException", "ResourceInUseException"}:
            log.warning("TTL update skipped", extra={"table": table_name, "error": code})
            return
        raise
