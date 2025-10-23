import argparse
from osf_sync.celery_app import app

def cmd_fetch_one(args):
    from osf_sync.celery_app import app
    if args.id:
        r = app.send_task("osf_sync.tasks.sync_one_by_id",
                          kwargs={"osf_id": args.id, "run_pdf_and_grobid": not args.metadata_only})
    else:
        r = app.send_task("osf_sync.tasks.sync_one_by_doi",
                          kwargs={"doi_or_url": args.doi, "run_pdf_and_grobid": not args.metadata_only})
    print("enqueued:", r.id)

def cmd_sync_from_date(args):
    r = app.send_task("osf_sync.tasks.sync_from_date_to_now",
                      kwargs={"start_date": args.start, "subject_text": args.subject})
    print("enqueued:", r.id)

def cmd_enqueue_grobid(args):
    r = app.send_task("osf_sync.tasks.enqueue_grobid",
                      kwargs={"limit": args.limit})
    print("enqueued:", r.id)

def cmd_enqueue_pdf(args):
    r = app.send_task("osf_sync.tasks.enqueue_pdf_downloads",
                      kwargs={"limit": args.limit})
    print("enqueued:", r.id)

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("sync-from-date")
    p1.add_argument("--start", required=True)
    p1.add_argument("--subject", default=None)
    p1.set_defaults(func=cmd_sync_from_date)

    p2 = sub.add_parser("enqueue-grobid")
    p2.add_argument("--limit", type=int, default=10)
    p2.set_defaults(func=cmd_enqueue_grobid)

    p3 = sub.add_parser("enqueue-pdf")
    p3.add_argument("--limit", type=int, default=50)
    p3.set_defaults(func=cmd_enqueue_pdf)

    p4 = sub.add_parser("fetch-one", help="Fetch one preprint by OSF id or DOI; optionally download PDF & run GROBID")
    g = p4.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", help="OSF preprint id, e.g. 7wnsz_v2")
    g.add_argument("--doi", help="DOI or https://doi.org/... link")
    p4.add_argument("--metadata-only", action="store_true", help="Only upsert metadata; skip PDF & GROBID")
    p4.set_defaults(func=cmd_fetch_one)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()