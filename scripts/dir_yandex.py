import argparse, os, sqlite3
DB = os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))
def run():
  ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest="cmd", required=True)
  a = sub.add_parser("add");     a.add_argument("--id",type=int,required=True); a.add_argument("--yname",required=True); a.add_argument("--mailbox",default="INBOX"); a.add_argument("--enable",type=int,choices=[0,1],default=1)
  e = sub.add_parser("enable");  e.add_argument("--id",type=int,required=True); e.add_argument("--on",type=int,choices=[0,1],required=True)
  m = sub.add_parser("mailbox"); m.add_argument("--id",type=int,required=True); m.add_argument("--mailbox",required=True)
  l = sub.add_parser("list")
  args = ap.parse_args(); con = sqlite3.connect(DB); cur = con.cursor()
  if args.cmd=="add":
    cur.execute("""INSERT INTO campaign_yandex(campaign_id,enabled,yandex_name,yandex_mailbox,updated_at)
                   VALUES (?,?,?,?,datetime('now'))
                   ON CONFLICT(campaign_id) DO UPDATE SET
                     enabled=excluded.enabled,yandex_name=excluded.yandex_name,yandex_mailbox=excluded.yandex_mailbox,updated_at=datetime('now')""",
                (args.id,args.enable,args.yname.strip(),args.mailbox.strip())); con.commit(); print("OK add:",args.id,args.yname,args.mailbox)
  elif args.cmd=="enable":
    cur.execute("UPDATE campaign_yandex SET enabled=?, updated_at=datetime('now') WHERE campaign_id=?",(args.on,args.id)); con.commit(); print("OK enable:",args.id,args.on)
  elif args.cmd=="mailbox":
    cur.execute("UPDATE campaign_yandex SET yandex_mailbox=?, updated_at=datetime('now') WHERE campaign_id=?",(args.mailbox.strip(),args.id)); con.commit(); print("OK mailbox:",args.id,args.mailbox)
  elif args.cmd=="list":
    for r in cur.execute("SELECT campaign_id,enabled,yandex_name,yandex_mailbox,updated_at FROM campaign_yandex ORDER BY campaign_id"): print(r)
  con.close()
if __name__=="__main__": run()
