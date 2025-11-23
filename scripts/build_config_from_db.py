import os, yaml, sqlite3
CFG="config.yaml"; DB=os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))
cfg = yaml.safe_load(open(CFG,"r",encoding="utf-8")) or {}
imap = cfg.get("imap") or {}
rows = sqlite3.connect(DB).execute("SELECT campaign_id,yandex_name,yandex_mailbox FROM campaign_yandex WHERE enabled=1 ORDER BY campaign_id").fetchall()
yc = []
for cid,name,mb in rows:
  item = {"id": int(cid), "yandex_name": str(name)}
  if mb and mb.strip().upper()!="INBOX": item["mailbox"]=mb.strip()
  yc.append(item)
cfg["imap"]=imap; cfg["yandex_campaigns"]=yc
yaml.safe_dump(cfg, open(CFG,"w",encoding="utf-8"), allow_unicode=True, sort_keys=False)
print("config.yaml rebuilt:", len(yc), "campaigns")
