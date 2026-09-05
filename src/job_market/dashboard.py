DASHBOARD_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Job Market Observer</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4"></script>
  <style>
    :root { color-scheme: dark; --bg:#0b1020; --card:#141b2d; --muted:#91a0bd; --accent:#7c9cff; }
    * { box-sizing:border-box } body { margin:0; font:15px system-ui; background:var(--bg); color:#eef2ff }
    main { max-width:1100px; margin:auto; padding:42px 22px } h1 { margin:0 0 8px; font-size:34px }
    header p { color:var(--muted); margin:0 0 28px } .grid { display:grid; grid-template-columns:2fr 1fr; gap:18px }
    .card { background:var(--card); border:1px solid #26314d; border-radius:16px; padding:20px; min-height:320px }
    h2 { font-size:17px; margin:0 0 18px } ol { padding-left:24px } li { padding:6px; color:#cad4ea }
    a { color:var(--accent) } @media(max-width:750px){.grid{grid-template-columns:1fr}}
  </style>
</head>
<body><main>
  <header><h1>Job Market Observer</h1><p>Спрос на специалистов по данным сохранённых наблюдений</p></header>
  <section class="grid">
    <div class="card"><h2>Новые вакансии по дням</h2><canvas id="demand"></canvas></div>
    <div class="card"><h2>Популярные технологии</h2><ol id="skills"><li>Загрузка…</li></ol></div>
  </section>
  <p><a href="/docs">Открыть документацию REST API →</a></p>
</main><script>
Promise.all([
  fetch('/api/v1/stats/demand/history?days=30').then(r=>r.json()),
  fetch('/api/v1/stats/technologies?limit=10').then(r=>r.json())
]).then(([history, skills]) => {
  new Chart(document.getElementById('demand'), {type:'line', data:{labels:history.map(x=>x.day), datasets:[{
    data:history.map(x=>x.vacancies), borderColor:'#7c9cff', backgroundColor:'#7c9cff33', fill:true, tension:.3
  }]}, options:{plugins:{legend:{display:false}}, scales:{y:{beginAtZero:true}}}});
  document.getElementById('skills').innerHTML = skills.map(x=>`<li>${x.name} — ${x.count}</li>`).join('') || '<li>Пока нет данных</li>';
}).catch(() => document.getElementById('skills').innerHTML='<li>Не удалось загрузить данные</li>');
</script></body></html>"""
