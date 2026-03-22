/**
 * ETL Playground UI — static site only.
 */
(function () {
  'use strict';

  var P = window.ETLPipeline;
  var R = window.ETLReport;
  var PREVIEW = P.constants.PREVIEW_ROWS;

  var state = {
    headers: [],
    beforeRows: [],
    afterRows: [],
    csvText: '',
    stepStats: [],
    parseWarnings: [],
    inferredTypes: {},
    softWarn: false,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function byteLengthUtf8(str) {
    return new Blob([str]).size;
  }

  function setStatus(el, message, kind) {
    el.textContent = message || '';
    el.classList.remove('warn', 'err');
    if (kind === 'warn') el.classList.add('warn');
    if (kind === 'err') el.classList.add('err');
  }

  function renderTable(headers, rows, maxRows) {
    var cap = Math.min(rows.length, maxRows || PREVIEW);
    if (!headers.length) {
      return '<p class="empty-state">No data loaded.</p>';
    }
    var html =
      '<table class="data-table"><thead><tr>' +
      headers.map(function (h) {
        return '<th>' + R.escapeHtml(h) + '</th>';
      }).join('') +
      '</tr></thead><tbody>';
    for (var r = 0; r < cap; r++) {
      html += '<tr>';
      for (var c = 0; c < headers.length; c++) {
        var v = rows[r][headers[c]];
        html += '<td>' + R.escapeHtml(v != null ? v : '') + '</td>';
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
    if (rows.length > cap) {
      html +=
        '<p class="status-line">Showing first ' +
        cap +
        ' of ' +
        rows.length +
        ' rows.</p>';
    }
    return html;
  }

  function renderStepper(parseInfo, pipelineStepStats) {
    var html = '';
    html += '<div class="step-card">';
    html += '<h3>Step 1</h3>';
    html += '<div class="step-title">Parse &amp; normalize table shape</div>';
    html += '<div class="step-meta">';
    html += 'Rows: ' + (parseInfo.rowCount != null ? parseInfo.rowCount : '—');
    html += ' · Columns: ' + (parseInfo.colCount != null ? parseInfo.colCount : '—');
    if (parseInfo.warnings && parseInfo.warnings.length) {
      html +=
        '<br>Warnings: ' + R.escapeHtml(parseInfo.warnings.join('; '));
    } else {
      html += '<br>No parse warnings.';
    }
    html += '</div></div>';

    for (var i = 0; i < pipelineStepStats.length; i++) {
      var s = pipelineStepStats[i];
      html += '<div class="step-card">';
      html += '<h3>Step ' + s.step + '</h3>';
      html += '<div class="step-title">' + R.escapeHtml(s.label) + '</div>';
      html += '<div class="step-meta">' + formatStepDetail(s) + '</div>';
      html += '</div>';
    }
    return html;
  }

  function formatStepDetail(s) {
    var parts = [];
    if (s.cellsNormalized != null) parts.push('Cells normalized to empty: ' + s.cellsNormalized);
    if (s.fullRowDedupeSkipped) {
      parts.push('Full-row dedupe skipped (event log shape; repeat rows preserved).');
    } else if (s.duplicatesRemovedFullRow != null) {
      parts.push('Full-row duplicates removed: ' + s.duplicatesRemovedFullRow);
    }
    if (s.duplicatesRemovedByEmail != null) {
      parts.push('Same-email duplicates removed: ' + s.duplicatesRemovedByEmail);
    }
    if (s.duplicatesRemoved != null) parts.push('Total duplicates removed: ' + s.duplicatesRemoved);
    if (s.invalidDates != null) parts.push('Invalid dates cleared: ' + s.invalidDates);
    if (s.invalidNumbers != null) parts.push('Invalid numbers cleared: ' + s.invalidNumbers);
    if (s.invalidEmails != null) parts.push('Invalid emails cleared: ' + s.invalidEmails);
    if (s.phonesFormatted != null) parts.push('Phones normalized: ' + s.phonesFormatted);
    if (s.invalidPhones != null) parts.push('Invalid phones cleared: ' + s.invalidPhones);
    if (s.booleansNormalized != null) parts.push('Booleans normalized: ' + s.booleansNormalized);
    if (s.invalidBooleans != null) parts.push('Invalid booleans cleared: ' + s.invalidBooleans);
    if (s.priceNegativesAbsoluted != null) {
      parts.push('Negative prices → absolute: ' + s.priceNegativesAbsoluted);
    }
    if (s.weightNegativesAbsoluted != null) {
      parts.push('Negative weights → absolute: ' + s.weightNegativesAbsoluted);
    }
    if (s.weightsFormatted != null) {
      parts.push('Weights formatted (2dp): ' + s.weightsFormatted);
    }
    if (s.stringsTitleCased != null) parts.push('Strings title-cased: ' + s.stringsTitleCased);
    if (s.outliersCappedTotal != null) parts.push('Values winsorized: ' + s.outliersCappedTotal);
    if (s.keysImputed != null) parts.push('Keys imputed (id gap): ' + s.keysImputed);
    if (s.moneyCellsFormatted != null) {
      parts.push('Money cells formatted (2dp): ' + s.moneyCellsFormatted);
    }
    if (s.rowsDropped != null) parts.push('Rows dropped (empty id): ' + s.rowsDropped);
    if (s.cellsFilledNA != null) parts.push('Blanks filled with N/A: ' + s.cellsFilledNA);
    return parts.length ? parts.join('<br>') : '—';
  }

  function processLoadedText(text, sourceLabel) {
    var statusLoad = $('status-load');
    var lim = P.checkLimits(byteLengthUtf8(text), 0);
    if (!lim.ok) {
      setStatus(statusLoad, lim.error, 'err');
      clearOutputs();
      return;
    }
    if (lim.softWarn) {
      setStatus(statusLoad, (sourceLabel ? '[' + sourceLabel + '] ' : '') + lim.softWarnMessage, 'warn');
    } else {
      setStatus(statusLoad, (sourceLabel ? 'Loaded: ' + sourceLabel + '. ' : '') + 'Ready to process.', null);
    }

    var parsed = P.parseCsv(text);
    if (!parsed.ok) {
      setStatus(statusLoad, parsed.error || 'Parse failed.', 'err');
      clearOutputs();
      return;
    }

    var rowCheck = P.checkLimits(byteLengthUtf8(text), parsed.rows.length);
    if (!rowCheck.ok) {
      setStatus(statusLoad, rowCheck.error, 'err');
      clearOutputs();
      return;
    }

    if (parsed.rows.length === 0) {
      setStatus(statusLoad, 'No data rows found after parse.', 'err');
      clearOutputs();
      return;
    }

    state.csvText = text;
    state.headers = parsed.headers;
    state.beforeRows = parsed.rows.map(function (row) {
      var o = {};
      for (var i = 0; i < parsed.headers.length; i++) {
        var h = parsed.headers[i];
        o[h] = row[h] != null ? String(row[h]) : '';
      }
      return o;
    });
    state.parseWarnings = parsed.warnings || [];

    var pipe = P.runPipeline(parsed.headers, state.beforeRows);
    state.afterRows = pipe.finalRows;
    state.stepStats = pipe.stepStats;
    state.inferredTypes = pipe.inferredTypes;

    $('stepper').innerHTML = renderStepper(
      {
        rowCount: parsed.rows.length,
        colCount: parsed.headers.length,
        warnings: state.parseWarnings,
      },
      pipe.stepStats
    );

    var report = R.buildQualityReport(
      state.headers,
      state.afterRows,
      state.inferredTypes,
      state.stepStats
    );
    $('report-panel').innerHTML = R.renderReportTable(report);

    $('preview-before').innerHTML = renderTable(state.headers, state.beforeRows, PREVIEW);
    $('preview-after').innerHTML = renderTable(state.headers, state.afterRows, PREVIEW);

    $('btn-download').disabled = false;
    $('btn-copy').disabled = state.afterRows.length === 0;
  }

  function clearOutputs() {
    state.headers = [];
    state.beforeRows = [];
    state.afterRows = [];
    state.stepStats = [];
    state.inferredTypes = {};
    state.parseWarnings = [];
    $('stepper').innerHTML = '<p class="empty-state">Load data to see pipeline steps.</p>';
    $('report-panel').innerHTML = '<p class="empty-state">No report yet.</p>';
    $('preview-before').innerHTML = '<p class="empty-state">—</p>';
    $('preview-after').innerHTML = '<p class="empty-state">—</p>';
    $('btn-download').disabled = true;
    $('btn-copy').disabled = true;
  }

  function onLoad() {
    var paste = $('paste-area').value;
    if (!paste.trim()) {
      setStatus($('status-load'), 'Paste CSV text or use Upload / Try sample.', 'err');
      return;
    }
    processLoadedText(paste, 'paste');
  }

  function onFile(ev) {
    var f = ev.target.files && ev.target.files[0];
    if (!f) return;
    if (f.type && f.type !== 'text/csv' && f.type !== 'application/vnd.ms-excel' && f.type !== 'text/plain') {
      setStatus($('status-load'), 'Please choose a .csv file.', 'err');
      return;
    }
    var reader = new FileReader();
    reader.onload = function () {
      var text = typeof reader.result === 'string' ? reader.result : '';
      $('paste-area').value = text;
      processLoadedText(text, f.name);
    };
    reader.onerror = function () {
      setStatus($('status-load'), 'Could not read file.', 'err');
    };
    reader.readAsText(f);
  }

  function onSample(path, label) {
    fetch(path)
      .then(function (res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.text();
      })
      .then(function (text) {
        $('paste-area').value = text;
        processLoadedText(text, label);
        document.querySelector('.sample-wrap').removeAttribute('open');
      })
      .catch(function () {
        setStatus($('status-load'), 'Could not load sample (check path on GitHub Pages).', 'err');
      });
  }

  function onDownload() {
    if (!state.afterRows.length && !state.headers.length) return;
    var csv = P.exportCsv(state.headers, state.afterRows);
    var blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'cleaned-etl.csv';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function onCopy() {
    var csv = P.exportCsv(state.headers, state.afterRows);
    function doneOk() {
      setStatus($('status-load'), 'Copied CSV to clipboard.', null);
    }
    function copyFallback() {
      var ta = document.createElement('textarea');
      ta.value = csv;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.left = '-9999px';
      document.body.appendChild(ta);
      ta.select();
      try {
        var ok = document.execCommand('copy');
        document.body.removeChild(ta);
        if (ok) doneOk();
        else setStatus($('status-load'), 'Copy failed.', 'err');
      } catch (e) {
        document.body.removeChild(ta);
        setStatus($('status-load'), 'Copy not supported in this browser.', 'err');
      }
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(csv).then(doneOk, copyFallback);
    } else {
      copyFallback();
    }
  }

  function onReset() {
    $('paste-area').value = '';
    $('file-input').value = '';
    state.csvText = '';
    clearOutputs();
    setStatus($('status-load'), 'Cleared. Data left memory (best-effort).', null);
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('btn-load').addEventListener('click', onLoad);
    $('file-input').addEventListener('change', onFile);
    $('btn-download').addEventListener('click', onDownload);
    $('btn-copy').addEventListener('click', onCopy);
    $('btn-reset').addEventListener('click', onReset);

    document.querySelectorAll('[data-sample]').forEach(function (btn) {
      btn.addEventListener('click', function (ev) {
        var det = ev.target.closest('details');
        if (det) det.removeAttribute('open');
        onSample(btn.getAttribute('data-sample'), btn.textContent.trim());
      });
    });

    clearOutputs();
  });

  var cursor = document.getElementById('cursor');
  var ring = document.getElementById('cursorRing');
  if (cursor && ring) {
    var mx = 0,
      my = 0,
      rx = 0,
      ry = 0;
    document.addEventListener('mousemove', function (e) {
      mx = e.clientX;
      my = e.clientY;
      cursor.style.left = mx - 5 + 'px';
      cursor.style.top = my - 5 + 'px';
    });
    function animRing() {
      rx += (mx - rx - 18) * 0.12;
      ry += (my - ry - 18) * 0.12;
      ring.style.left = rx + 'px';
      ring.style.top = ry + 'px';
      requestAnimationFrame(animRing);
    }
    animRing();
  }
})();
