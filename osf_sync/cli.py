import argparse
from osf_sync.celery_app import app

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

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()