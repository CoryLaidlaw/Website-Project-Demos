/**
 * Model comparison console — loads precomputed JSON from ../data/
 */
(function () {
  const RESULTS_URL = '../data/model-eval-results.json';
  const META_URL = '../data/model-eval-meta.json';

  const MODEL_COLORS = ['#00ff9d', '#7bff61', '#38bdf8', '#a78bfa', '#f59e0b'];

  let chart = null;

  function chartAnim() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches ? false : 400;
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function buildGlossary(metricDefinitions) {
    const dl = document.getElementById('glossaryBody');
    if (!dl || !metricDefinitions) return;
    dl.innerHTML = '';
    Object.entries(metricDefinitions).forEach(([key, desc]) => {
      const dt = document.createElement('dt');
      dt.textContent = key;
      const dd = document.createElement('dd');
      dd.textContent = desc;
      dl.appendChild(dt);
      dl.appendChild(dd);
    });
  }

  function buildToggles(models, onChange) {
    const host = document.getElementById('modelToggles');
    if (!host) return;
    host.innerHTML = '';
    models.forEach((m, i) => {
      const id = `toggle-${m.id}`;
      const label = document.createElement('label');
      const input = document.createElement('input');
      input.type = 'checkbox';
      input.id = id;
      input.checked = true;
      input.dataset.modelId = m.id;
      input.addEventListener('change', onChange);
      const span = document.createElement('span');
      span.textContent = m.display_name;
      span.style.borderBottom = `2px solid ${MODEL_COLORS[i % MODEL_COLORS.length]}`;
      label.appendChild(input);
      label.appendChild(span);
      host.appendChild(label);
    });
  }

  function visibleModelIds(models) {
    const ids = new Set();
    document.querySelectorAll('#modelToggles input[type="checkbox"]').forEach((input) => {
      if (input.checked) ids.add(input.dataset.modelId);
    });
    return ids;
  }

  function updateChart(ChartLib, models) {
    const ids = visibleModelIds(models);
    const filtered = models.filter((m) => ids.has(m.id));
    const data = filtered.map((m, i) => ({
      x: m.metrics_test.precision,
      y: m.metrics_test.recall,
      label: m.display_name,
      color: MODEL_COLORS[models.indexOf(m) % MODEL_COLORS.length],
    }));

    if (!chart) {
      const ctx = document.getElementById('chartScatter').getContext('2d');
      chart = new ChartLib(ctx, {
        type: 'scatter',
        data: {
          datasets: [
            {
              label: 'Models (test set)',
              data,
              parsing: false,
              pointBackgroundColor: data.map((d) => d.color),
              pointBorderColor: data.map((d) => d.color),
              pointRadius: 10,
              pointHoverRadius: 13,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: chartAnim(),
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label(ctx) {
                  const d = ctx.raw;
                  return `${d.label}: P=${d.x.toFixed(4)} R=${d.y.toFixed(4)}`;
                },
              },
            },
          },
          scales: {
            x: {
              title: {
                display: true,
                text: 'Precision (phishing predicted)',
                color: '#6b7fa3',
                font: { family: 'DM Sans', size: 11 },
              },
              min: 0,
              max: 1,
              ticks: { color: '#6b7fa3' },
              grid: { color: 'rgba(30,44,63,0.6)' },
            },
            y: {
              title: {
                display: true,
                text: 'Recall (phishing caught)',
                color: '#6b7fa3',
                font: { family: 'DM Sans', size: 11 },
              },
              min: 0,
              max: 1,
              ticks: { color: '#6b7fa3' },
              grid: { color: 'rgba(30,44,63,0.6)' },
            },
          },
        },
      });
    } else {
      chart.data.datasets[0].data = data;
      chart.data.datasets[0].pointBackgroundColor = data.map((d) => d.color);
      chart.data.datasets[0].pointBorderColor = data.map((d) => d.color);
      chart.update();
    }
  }

  function buildTable(models) {
    const tbody = document.querySelector('#metricsTable tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    models.forEach((m) => {
      const mt = m.metrics_test;
      const tr = document.createElement('tr');
      tr.dataset.modelId = m.id;
      tr.innerHTML = `
        <td>${m.display_name}</td>
        <td class="num">${mt.precision.toFixed(4)}</td>
        <td class="num">${mt.recall.toFixed(4)}</td>
        <td class="num">${mt.f1.toFixed(4)}</td>
        <td class="num">${mt.pr_auc != null ? mt.pr_auc.toFixed(4) : '—'}</td>
        <td class="num">${mt.roc_auc != null ? mt.roc_auc.toFixed(4) : '—'}</td>
        <td class="num">${mt.brier != null ? mt.brier.toFixed(4) : '—'}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  function syncTableDim(models) {
    const ids = visibleModelIds(models);
    document.querySelectorAll('#metricsTable tbody tr').forEach((tr) => {
      tr.classList.toggle('dimmed', !ids.has(tr.dataset.modelId));
    });
  }

  function buildModelCards(models) {
    const host = document.getElementById('modelCards');
    if (!host) return;
    host.innerHTML = '';
    models.forEach((m) => {
      const article = document.createElement('article');
      article.className = 'model-card';
      article.dataset.modelId = m.id;
      const pick = (m.when_to_pick || []).map((t) => `<li>${escapeHtml(t)}</li>`).join('');
      const avoid = (m.when_to_avoid || []).map((t) => `<li>${escapeHtml(t)}</li>`).join('');
      const cm = m.confusion_test;
      article.innerHTML = `
        <h3>${escapeHtml(m.display_name)}</h3>
        <p class="sub">When to consider</p>
        <ul class="pick">${pick}</ul>
        <p class="sub">Caveats</p>
        <ul class="avoid">${avoid}</ul>
        <p class="sub">Confusion (test, threshold ${m.threshold})</p>
        <p style="font-size:0.82rem;color:var(--muted);margin-top:0.35rem">
          TN ${cm.tn} · FP ${cm.fp} · FN ${cm.fn} · TP ${cm.tp}
        </p>
      `;
      host.appendChild(article);
    });
  }

  function escapeHtml(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function buildLimitations(items) {
    const ul = document.getElementById('limitationsList');
    if (!ul || !items) return;
    ul.innerHTML = '';
    items.forEach((t) => {
      const li = document.createElement('li');
      li.textContent = t;
      ul.appendChild(li);
    });
  }

  async function boot() {
    try {
      const [resRes, metaRes] = await Promise.all([fetch(RESULTS_URL), fetch(META_URL)]);
      if (!resRes.ok || !metaRes.ok) throw new Error('fetch failed');
      const results = await resRes.json();
      const meta = await metaRes.json();

      if (results.schema_version !== '1.0') {
        console.warn('Unknown schema_version', results.schema_version);
      }

      const models = results.models || [];
      setText('heroTitle', meta.title || 'Model comparison');
      setText('heroLead', meta.one_liner || '');
      setText('stripDataset', meta.dataset_name || '—');
      setText('stripSplit', results.split ? `${results.split.method}, test_size=${results.split.test_size}, seed=${results.split.random_state}` : '—');
      setText('stripRows', results.load ? `${results.load.n_rows_used?.toLocaleString() ?? '—'} rows used (${results.load.n_rows_total_file?.toLocaleString() ?? '—'} in file). Prevalence phishing: ${(results.load.prevalence_phishing * 100).toFixed(2)}%.` : '—');
      setText('tradeoffText', results.tradeoff_summary || '');
      setText('footerVersion', `${meta.last_data_build || '—'} · ${results.results_id || '—'} · ${meta.sklearn_version ? `sklearn ${meta.sklearn_version}` : ''}`);

      buildGlossary(meta.metric_definitions);
      buildLimitations(meta.limitations);
      buildTable(models);
      buildModelCards(models);

      const attr = document.getElementById('attrBlock');
      if (attr) {
        const links = (meta.source_urls || [])
          .map((u) => `<a href="${u}" target="_blank" rel="noopener noreferrer">${u}</a>`)
          .join(' · ');
        attr.innerHTML = `
          <p><strong>Methodology:</strong> ${escapeHtml(meta.methodology_note || '')}</p>
          <p style="margin-top:0.6rem"><strong>License / source:</strong> ${links}</p>
          <p style="margin-top:0.6rem">${escapeHtml(meta.license_note || '')}</p>
        `;
      }

      const ChartLib = window.Chart;
      if (!ChartLib) throw new Error('Chart.js not loaded');

      const onToggle = () => {
        updateChart(ChartLib, models);
        syncTableDim(models);
      };
      buildToggles(models, onToggle);
      onToggle();

      const loadLine = document.getElementById('loadingLine');
      if (loadLine) loadLine.hidden = true;
      document.getElementById('loadErr').hidden = true;
      document.getElementById('app').hidden = false;
    } catch (e) {
      console.error(e);
      const loadLine = document.getElementById('loadingLine');
      if (loadLine) loadLine.hidden = true;
      document.getElementById('loadErr').hidden = false;
      setText('loadErrMsg', 'Could not load evaluation JSON. Serve the repo from a local server (e.g. python3 -m http.server) so fetch works.');
    }
  }

  boot();
})();
