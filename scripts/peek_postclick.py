import os, sqlite3
db=os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))
con=sqlite3.connect(db); cur=con.cursor(); print("DB:",db)
print("\n-- yandex_daily_metrics (last 10) --")
for r in cur.execute("""SELECT campaign_id,report_date,visits,visitors,bounce_rate,page_depth,avg_time_sec
                        FROM yandex_daily_metrics ORDER BY report_date DESC, campaign_id LIMIT 10"""): print(r)
print("\n-- campaign_kpis_daily (last 10) --")
for r in cur.execute("""SELECT campaign_id,report_date,clicks,visits,printf('%.2f',reachability)
                        FROM campaign_kpis_daily ORDER BY report_date DESC, campaign_id LIMIT 10"""): print(r)
con.close()
