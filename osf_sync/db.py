from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://osf:osfpass@postgres:5432/osfdb")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

DDL = """
CREATE TABLE IF NOT EXISTS preprints (
    osf_id              text PRIMARY KEY,
    type                text,
    title               text,
    description         text,
    doi                 text,
    date_created        timestamptz,
    date_modified       timestamptz,
    date_published      timestamptz,
    is_published        boolean,
    version             int,
    is_latest_version   boolean,
    reviews_state       text,
    tags                text[],
    subjects            jsonb,
    license_record      jsonb,
    provider_id         text,
    -- NOTE: if the table existed before, this column may be missing; migration below adds it.
    primary_file_id     text,
    links               jsonb,
    raw                 jsonb,
    -- download tracking (may be missing; migration adds them)
    pdf_downloaded      boolean DEFAULT false,
    pdf_downloaded_at   timestamptz,
    pdf_path            text,
    updated_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_preprints_date_modified ON preprints (date_modified);
CREATE INDEX IF NOT EXISTS idx_preprints_provider ON preprints (provider_id);
CREATE INDEX IF NOT EXISTS idx_preprints_tags_gin ON preprints USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_preprints_subjects_gin ON preprints USING GIN (subjects);

CREATE TABLE IF NOT EXISTS sync_state (
    source_key      text PRIMARY KEY,
    last_seen_published timestamptz,
    last_run_at     timestamptz DEFAULT now()
);
"""

MIGRATIONS = [
    # Add columns if they don’t exist (safe to run repeatedly)
    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS primary_file_id   text",
    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS pdf_downloaded    boolean DEFAULT false",
    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS pdf_downloaded_at timestamptz",
    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS pdf_path          text",
    # Optional: helpful index if you’ll query by download status
    "CREATE INDEX IF NOT EXISTS idx_preprints_pdf_needed ON preprints (pdf_downloaded) WHERE pdf_downloaded = false",

    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS tei_generated    boolean DEFAULT false",
    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS tei_generated_at timestamptz",
    "ALTER TABLE preprints ADD COLUMN IF NOT EXISTS tei_path         text",
    "CREATE INDEX IF NOT EXISTS idx_preprints_tei_needed ON preprints (tei_generated) WHERE tei_generated = false",
]

def init_db():
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL)
        for stmt in MIGRATIONS:
            conn.exec_driver_sql(stmt)