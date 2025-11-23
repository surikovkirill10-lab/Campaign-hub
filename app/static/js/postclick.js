(function () {
  function toHMS(sec) {
    if (sec == null) return "—";
    var s = Math.max(0, Math.floor(sec));
    var h = Math.floor(s/3600), m = Math.floor((s%3600)/60), ss = s%60;
    return String(h).padStart(2,"0")+":"+String(m).padStart(2,"0")+":"+String(ss).padStart(2,"0");
  }
  function pct(x) {
    if (x == null || isNaN(x)) return "—";
    return (x*100).toFixed(2)+"%";
  }

  function inject(cid) {
    // находим таблицу с дневными строками — ищем заголовок "Дата"
    var tables = document.querySelectorAll("table");
    var tbl = null;
    outer: for (var t of tables) {
      var ths = t.querySelectorAll("thead th");
      for (var th of ths) {
        if (th.textContent.trim().toLowerCase() === "дата") { tbl = t; break outer; }
      }
    }
    if (!tbl) return;

    // Добавляем заголовки, если их ещё нет
    var headRow = tbl.querySelector("thead tr");
    if (!headRow) return;
    if (!headRow.querySelector('th[data-pc="visits"]')) {
      ["Визиты","Доходимость","Отказы","Глубина","Время"].forEach(function(name, idx){
        var th = document.createElement("th");
        th.textContent = name;
        th.setAttribute("data-pc", ["visits","reach","bounce","depth","time"][idx]);
        headRow.appendChild(th);
      });
    }

    // Забираем JSON
    fetch("/postclick/"+cid+".json", {cache:"no-store"})
      .then(r => r.json())
      .then(data => {
        if (!data || !Array.isArray(data.rows)) return;
        var map = {};
        data.rows.forEach(function(r){ map[r.date] = r; });

        // Проходим по строкам тела, первая ячейка — дата
        var bodyRows = tbl.querySelectorAll("tbody tr");
        bodyRows.forEach(function(tr){
          var dtd = tr.querySelector("td");
          if (!dtd) return;
          var date = dtd.textContent.trim();
          var r = map[date];

          // Удалим старые ячейки пост-клика (если перезагрузили)
          tr.querySelectorAll('td[data-pc]').forEach(function(td){ td.remove(); });

          var cells = [];
          function td(v, key){
            var el = document.createElement("td"); el.setAttribute("data-pc", key);
            el.textContent = v;
            el.style.whiteSpace = "nowrap";
            tr.appendChild(el);
          }
          if (r) {
            td((r.visits!=null? Math.round(r.visits): "—"), "visits");
            td((r.reachability!=null? (r.reachability*100).toFixed(2)+"%":"—"), "reach");
            td(pct(r.bounce_rate), "bounce");
            td((r.page_depth!=null? r.page_depth.toFixed(2): "—"), "depth");
            td(toHMS(r.avg_time_sec), "time");
          } else {
            td("—","visits"); td("—","reach"); td("—","bounce"); td("—","depth"); td("—","time");
          }
        });
      })
      .catch(function(e){ /* молча */ });
  }

  // Читаем campaign-id из data-атрибута, если он проставлен в разметке
  var el = document.querySelector('[data-campaign-id]');
  var cid = el ? el.getAttribute('data-campaign-id') : null;

  // Фолбэк: попробуем вытащить ID из таблицы/шапки (в левом столбце ID наверху)
  if (!cid) {
    var idCell = document.querySelector("table tbody tr td");
    // оставим пусто, если не нашли — тогда просто не инжектим
  }

  // если удалось — запускаем
  if (cid) inject(cid);
})();
