/**
 * Data quality summary for ETL Playground (client-side).
 */
(function (global) {
  'use strict';

  function missingness(headers, rows) {
    var out = [];
    var total = rows.length || 1;
    for (var c = 0; c < headers.length; c++) {
      var h = headers[c];
      var miss = 0;
      for (var r = 0; r < rows.length; r++) {
        var v = rows[r][h];
        if (v == null || String(v).trim() === '') miss++;
      }
      out.push({
        column: h,
        missing: miss,
        pct: Math.round((miss / total) * 1000) / 10,
      });
    }
    return out;
  }

  function findStepStats(stepStats, stepNum) {
    for (var i = 0; i < stepStats.length; i++) {
      if (stepStats[i].step === stepNum) return stepStats[i];
    }
    return null;
  }

  /**
   * @returns {object} structured report for UI
   */
  function buildQualityReport(headers, finalRows, inferredTypes, stepStats) {
    var miss = missingness(headers, finalRows);
    var s3 = findStepStats(stepStats, 3);
    var s4 = findStepStats(stepStats, 4);
    var s5 = findStepStats(stepStats, 5);
    var s6 = findStepStats(stepStats, 6);

    var duplicatesRemoved = s3 ? s3.duplicatesRemoved : null;
    var duplicatesRemovedFullRow = s3 ? s3.duplicatesRemovedFullRow : null;
    var duplicatesRemovedByEmail = s3 ? s3.duplicatesRemovedByEmail : null;
    var invalidDates = s4 ? s4.invalidDates : 0;
    var invalidNumbers = s4 ? s4.invalidNumbers : 0;
    var invalidEmails = s4 ? s4.invalidEmails : 0;
    var invalidPhones = s4 ? s4.invalidPhones : 0;
    var phonesFormatted = s4 ? s4.phonesFormatted : 0;
    var invalidBooleans = s4 ? s4.invalidBooleans : 0;
    var booleansNormalized = s4 ? s4.booleansNormalized : 0;
    var priceNegativesAbsoluted = s4 ? s4.priceNegativesAbsoluted : 0;
    var weightNegativesAbsoluted = s4 ? s4.weightNegativesAbsoluted : 0;
    var weightsFormatted = s4 ? s4.weightsFormatted : 0;
    var stringsTitleCased = s4 ? s4.stringsTitleCased : 0;
    var typeConflicts = s4 ? s4.typeConflicts : 0;
    var invalidByColumn = s4 && s4.invalidByColumn ? s4.invalidByColumn : {};

    var outliersCapped = s5 ? s5.outliersCappedTotal : 0;
    var winsorByCol = s5 && s5.perColumn ? s5.perColumn : {};

    var schemaRows = [];
    for (var c = 0; c < headers.length; c++) {
      var col = headers[c];
      var t = inferredTypes[col] || 'string';
      var nonEmptyAfter = 0;
      for (var r = 0; r < finalRows.length; r++) {
        var v = finalRows[r][col];
        if (v != null && String(v).trim() !== '') nonEmptyAfter++;
      }
      var colInv = invalidByColumn[col] || { date: 0, number: 0 };
      var conflicts = t === 'date' ? colInv.date : t === 'number' ? colInv.number : 0;
      var typedTotal = nonEmptyAfter + conflicts;
      var conflictRate =
        typedTotal > 0 && (t === 'date' || t === 'number')
          ? Math.round((conflicts / typedTotal) * 1000) / 10
          : 0;
      var note = '—';
      if (t === 'date' || t === 'number') {
        note =
          conflicts === 0
            ? 'ok'
            : conflictRate + '% invalid (' + conflicts + ' cell' + (conflicts !== 1 ? 's' : '') + ')';
      }
      schemaRows.push({
        column: col,
        inferredType: t,
        conflictRate: conflictRate,
        note: note,
      });
    }

    return {
      missingness: miss,
      duplicatesRemoved: duplicatesRemoved,
      duplicatesRemovedFullRow: duplicatesRemovedFullRow,
      duplicatesRemovedByEmail: duplicatesRemovedByEmail,
      invalidDates: invalidDates,
      invalidNumbers: invalidNumbers,
      invalidEmails: invalidEmails,
      invalidPhones: invalidPhones,
      phonesFormatted: phonesFormatted,
      invalidBooleans: invalidBooleans,
      booleansNormalized: booleansNormalized,
      priceNegativesAbsoluted: priceNegativesAbsoluted,
      weightNegativesAbsoluted: weightNegativesAbsoluted,
      weightsFormatted: weightsFormatted,
      stringsTitleCased: stringsTitleCased,
      typeConflicts: typeConflicts,
      outliersCapped: outliersCapped,
      winsorByColumn: winsorByCol,
      schema: schemaRows,
      rowCount: finalRows.length,
      rowsDroppedKey: s6 ? s6.rowsDropped : null,
      cellsFilledNA: s6 ? s6.cellsFilledNA : null,
      keysImputed: s6 ? s6.keysImputed : null,
      moneyCellsFormatted: s6 ? s6.moneyCellsFormatted : null,
    };
  }

  function renderReportTable(report) {
    var html = '';
    html += '<table class="report-mini"><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>';
    html +=
      '<tr><td>Rows (after pipeline)</td><td>' +
      (report.rowCount != null ? report.rowCount : '—') +
      '</td></tr>';
    html +=
      '<tr><td>Full-row duplicates removed (step 3)</td><td>' +
      (report.duplicatesRemovedFullRow != null ? report.duplicatesRemovedFullRow : '—') +
      '</td></tr>';
    html +=
      '<tr><td>Same-email duplicates removed (step 3)</td><td>' +
      (report.duplicatesRemovedByEmail != null ? report.duplicatesRemovedByEmail : '—') +
      '</td></tr>';
    html +=
      '<tr><td>Total duplicates removed (step 3)</td><td>' +
      (report.duplicatesRemoved != null ? report.duplicatesRemoved : '—') +
      '</td></tr>';
    html += '<tr><td>Invalid dates (step 4)</td><td>' + report.invalidDates + '</td></tr>';
    html += '<tr><td>Invalid numbers (step 4)</td><td>' + report.invalidNumbers + '</td></tr>';
    html += '<tr><td>Invalid emails cleared (step 4)</td><td>' + report.invalidEmails + '</td></tr>';
    html += '<tr><td>Phones normalized (step 4)</td><td>' + report.phonesFormatted + '</td></tr>';
    html += '<tr><td>Invalid phones cleared (step 4)</td><td>' + report.invalidPhones + '</td></tr>';
    html += '<tr><td>Booleans normalized (step 4)</td><td>' + report.booleansNormalized + '</td></tr>';
    html += '<tr><td>Invalid booleans cleared (step 4)</td><td>' + report.invalidBooleans + '</td></tr>';
    html += '<tr><td>Negative prices → absolute (step 4)</td><td>' + report.priceNegativesAbsoluted + '</td></tr>';
    html += '<tr><td>Negative weights → absolute (step 4)</td><td>' + report.weightNegativesAbsoluted + '</td></tr>';
    html += '<tr><td>Weights formatted (2dp, step 4)</td><td>' + report.weightsFormatted + '</td></tr>';
    html += '<tr><td>Strings title-cased (step 4)</td><td>' + report.stringsTitleCased + '</td></tr>';
    html += '<tr><td>Values capped (winsorize, step 5)</td><td>' + report.outliersCapped + '</td></tr>';
    html +=
      '<tr><td>Keys imputed (letter+number gap, step 6)</td><td>' +
      (report.keysImputed != null ? report.keysImputed : '—') +
      '</td></tr>';
    html +=
      '<tr><td>Money cells formatted (2 decimals, step 6)</td><td>' +
      (report.moneyCellsFormatted != null ? report.moneyCellsFormatted : '—') +
      '</td></tr>';
    html +=
      '<tr><td>Rows dropped (empty id column, step 6)</td><td>' +
      (report.rowsDroppedKey != null ? report.rowsDroppedKey : '—') +
      '</td></tr>';
    html +=
      '<tr><td>Blanks filled with N/A (step 6)</td><td>' +
      (report.cellsFilledNA != null ? report.cellsFilledNA : '—') +
      '</td></tr>';
    html += '</tbody></table>';

    html += '<h3 class="report-sub">Missingness by column</h3>';
    html += '<div class="table-scroll"><table class="data-table"><thead><tr><th>Column</th><th>Missing</th><th>%</th></tr></thead><tbody>';
    for (var i = 0; i < report.missingness.length; i++) {
      var m = report.missingness[i];
      var badge =
        m.pct === 0
          ? '<span class="badge badge-ok">ok</span>'
          : m.pct > 50
            ? '<span class="badge badge-warn">high</span>'
            : '<span class="badge badge-muted">some</span>';
      html +=
        '<tr><td>' +
        escapeHtml(m.column) +
        '</td><td>' +
        m.missing +
        '</td><td>' +
        m.pct +
        '% ' +
        badge +
        '</td></tr>';
    }
    html += '</tbody></table></div>';

    html += '<h3 class="report-sub">Schema (inferred)</h3>';
    html +=
      '<div class="table-scroll"><table class="data-table"><thead><tr><th>Column</th><th>Type</th><th>Conflict %</th><th>Notes</th></tr></thead><tbody>';
    for (var j = 0; j < report.schema.length; j++) {
      var s = report.schema[j];
      var pct =
        s.inferredType === 'string' ? '—' : (s.conflictRate != null ? s.conflictRate + '%' : '—');
      html +=
        '<tr><td>' +
        escapeHtml(s.column) +
        '</td><td>' +
        escapeHtml(s.inferredType) +
        '</td><td>' +
        escapeHtml(pct) +
        '</td><td>' +
        escapeHtml(s.note) +
        '</td></tr>';
    }
    html += '</tbody></table></div>';

    var cols = Object.keys(report.winsorByColumn || {});
    if (cols.length) {
      html += '<h3 class="report-sub">Winsorize (step 5)</h3>';
      html += '<div class="table-scroll"><table class="data-table"><thead><tr><th>Column</th><th>Result</th></tr></thead><tbody>';
      for (var k = 0; k < cols.length; k++) {
        var col = cols[k];
        var w = report.winsorByColumn[col];
        var desc;
        if (w.skipped) {
          desc = 'Skipped: ' + (w.reason || '');
        } else {
          desc =
            'Capped ' +
            w.outliersCapped +
            ' value(s); bounds [' +
            (w.lowerBound != null ? w.lowerBound.toFixed(4) : '') +
            ', ' +
            (w.upperBound != null ? w.upperBound.toFixed(4) : '') +
            ']';
        }
        html +=
          '<tr><td>' +
          escapeHtml(col) +
          '</td><td>' +
          escapeHtml(desc) +
          '</td></tr>';
      }
      html += '</tbody></table></div>';
    }

    return html;
  }

  function escapeHtml(s) {
    if (s == null) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  global.ETLReport = {
    buildQualityReport: buildQualityReport,
    renderReportTable: renderReportTable,
    escapeHtml: escapeHtml,
  };
})(typeof window !== 'undefined' ? window : this);
