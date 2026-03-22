/**
 * Pediatric CXR demo — TensorFlow.js inference on curated samples only.
 * Positive class probability = P(PNEUMONIA).
 */

const MODEL_URL = 'model/model.json';
const METRICS_URL = 'model-metrics.json';
const MANIFEST_URL = 'assets/samples/manifest.json';

let model = null;
let metrics = null;
let samples = [];
let index = 0;

const loadingLine = document.getElementById('loadingLine');
const loadErr = document.getElementById('loadErr');
const loadErrMsg = document.getElementById('loadErrMsg');
const app = document.getElementById('app');

function showError(msg) {
  loadingLine.hidden = true;
  loadErr.hidden = false;
  loadErrMsg.textContent = msg;
}

function preprocessFromImgEl(img) {
  return tf.tidy(() => {
    let t = tf.browser.fromPixels(img);
    t = tf.cast(t, 'float32');
    t = tf.image.resizeBilinear(t, [224, 224]);
    t = tf.sub(tf.div(t, tf.scalar(127.5)), tf.scalar(1.0));
    return t.expandDims(0);
  });
}

function predictPneumoniaProb(imgEl) {
  const input = preprocessFromImgEl(imgEl);
  const out = model.predict(input);
  const raw = out.dataSync();
  // GraphModel may return [1,1] or flat; take first logit/prob either way.
  const prob = raw[0];
  out.dispose();
  input.dispose();
  return prob;
}

function formatPct(x) {
  return `${(x * 100).toFixed(1)}%`;
}

function renderPrediction(pPneumonia) {
  const predLabel = document.getElementById('predLabel');
  const confFill = document.getElementById('confFill');
  const confText = document.getElementById('confText');

  const isPneu = pPneumonia >= 0.5;
  const label = isPneu ? 'PNEUMONIA' : 'NORMAL';
  const conf = isPneu ? pPneumonia : 1 - pPneumonia;

  predLabel.textContent = label;
  predLabel.className = 'prediction ' + (isPneu ? 'pneumonia' : 'normal');
  confFill.style.width = `${Math.min(100, Math.max(0, conf * 100)).toFixed(1)}%`;
  confText.textContent = `${formatPct(conf)}`;
}

function renderSampleMeta() {
  const s = samples[index];
  const el = document.getElementById('sampleMeta');
  el.innerHTML = `Sample <span>${index + 1}</span> / <span>${samples.length}</span> · True label (for eval): <span>${s.trueLabel}</span>`;
  document.getElementById('sampleNote').textContent = s.limitationsNote;
}

function syncThumbs() {
  const buttons = document.querySelectorAll('#thumbGrid .thumb-btn');
  buttons.forEach((b, i) => {
    b.setAttribute('aria-selected', i === index ? 'true' : 'false');
  });
}

async function runCurrent() {
  const s = samples[index];
  const img = document.getElementById('mainImg');
  img.src = new URL(`assets/samples/${s.file}`, window.location.href).href;
  await new Promise((resolve, reject) => {
    img.onload = resolve;
    img.onerror = () => reject(new Error('Failed to load image'));
  });
  const p = predictPneumoniaProb(img);
  renderPrediction(p);
  renderSampleMeta();
  syncThumbs();
}

function step(delta) {
  index = (index + delta + samples.length) % samples.length;
  runCurrent().catch((e) => showError(e.message || String(e)));
}

function populateModelCard() {
  if (!metrics) return;
  document.getElementById('mcInput').textContent = `${metrics.input_size[0]}×${metrics.input_size[1]} RGB`;
  document.getElementById('mcPre').textContent = metrics.preprocessing.mobilenet_v2_preprocess || 'MobileNet v2 preprocess';
  document.getElementById('mcBackbone').textContent = metrics.backbone || '—';
  const sp = metrics.split;
  document.getElementById('mcSplit').textContent = `${sp.strategy}, seed ${sp.random_seed} — train ${sp.counts.train}, val ${sp.counts.val}, test ${sp.counts.test}`;

  const tm = metrics.test_metrics;
  const tbody = document.querySelector('#mcMetricsTable tbody');
  tbody.innerHTML = '';
  const rows = [
    ['Accuracy', tm.accuracy],
    ['Precision (PNEUMONIA)', tm.precision_pneumonia],
    ['Recall (PNEUMONIA)', tm.recall_pneumonia],
    ['F1 (PNEUMONIA)', tm.f1_pneumonia],
    ['ROC AUC', tm.roc_auc],
  ];
  for (const [name, val] of rows) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<th>${name}</th><td class="num">${typeof val === 'number' ? val.toFixed(4) : val}</td>`;
    tbody.appendChild(tr);
  }

  const cm = metrics.confusion_matrix_test.matrix;
  const cmBody = document.querySelector('#mcCmTable tbody');
  cmBody.innerHTML = '';
  const rowsCm = [
    ['true NORMAL', cm[0][0], cm[0][1]],
    ['true PNEUMONIA', cm[1][0], cm[1][1]],
  ];
  for (const [lab, a, b] of rowsCm) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<th>${lab}</th><td class="num">${a}</td><td class="num">${b}</td>`;
    cmBody.appendChild(tr);
  }

  const ul = document.getElementById('mcLimits');
  ul.innerHTML = '';
  for (const line of metrics.limitations || []) {
    const li = document.createElement('li');
    li.textContent = line;
    ul.appendChild(li);
  }
}

function buildThumbs() {
  const grid = document.getElementById('thumbGrid');
  grid.innerHTML = '';
  samples.forEach((s, i) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'thumb-btn';
    btn.setAttribute('role', 'tab');
    btn.setAttribute('aria-selected', i === 0 ? 'true' : 'false');
    btn.setAttribute('aria-label', `Sample ${i + 1}, ${s.trueLabel}`);
    const im = document.createElement('img');
    im.src = new URL(`assets/samples/${s.file}`, window.location.href).href;
    im.alt = '';
    btn.appendChild(im);
    btn.addEventListener('click', () => {
      index = i;
      runCurrent().catch((e) => showError(e.message || String(e)));
    });
    grid.appendChild(btn);
  });
}

async function init() {
  try {
    const [mRes, manRes] = await Promise.all([
      fetch(METRICS_URL),
      fetch(MANIFEST_URL),
    ]);
    if (!mRes.ok) throw new Error(`Missing ${METRICS_URL}`);
    if (!manRes.ok) throw new Error(`Missing ${MANIFEST_URL}`);
    metrics = await mRes.json();
    const manifest = await manRes.json();
    samples = manifest.samples || [];
    if (!samples.length) throw new Error('No samples in manifest');

    await tf.ready();
    try {
      await tf.setBackend('webgl');
    } catch {
      await tf.setBackend('cpu');
    }
    // GraphModel (SavedModel export): avoids Keras 3 nested InputLayer bugs in loadLayersModel.
    model = await tf.loadGraphModel(MODEL_URL);

    populateModelCard();
    buildThumbs();
    index = 0;

    document.getElementById('btnPrev').addEventListener('click', () => step(-1));
    document.getElementById('btnNext').addEventListener('click', () => step(1));

    await runCurrent();

    loadingLine.hidden = true;
    app.hidden = false;
  } catch (e) {
    console.error(e);
    showError(e.message || String(e));
  }
}

init();
