/**
 * ETL Playground — deterministic CSV pipeline (steps 2–5; step 1 is parse).
 * Depends on global Papa (Papa Parse).
 */
(function (global) {
  'use strict';

  var MAX_BYTES_HARD = 50 * 1024 * 1024;
  var SOFT_WARN_BYTES = 20 * 1024 * 1024;
  var MAX_ROWS = 100000;
  var INFER_THRESHOLD = 0.8;
  var IQR_K = 1.5;
  var WINSOR_MIN_VALUES = 4;

  var NULL_TOKENS = new Set([
    '',
    'n/a',
    'na',
    'null',
    'none',
    '-',
    '--',
    'nan',
  ]);

  /** Shared name hints for money / non-negative price columns (salary excluded for negatives). */
  var MONEY_NAME_PARTS = [
    'amount',
    'price',
    'cost',
    'total',
    'balance',
    'subtotal',
    'tax',
    'fee',
    'revenue',
    'salary',
    'payment',
  ];

  function normalizeRaw(raw) {
    var s = typeof raw === 'string' ? raw : String(raw);
    if (s.charCodeAt(0) === 0xfeff) s = s.slice(1);
    return s.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  }

  function padRow(row, headers) {
    var out = {};
    for (var i = 0; i < headers.length; i++) {
      var h = headers[i];
      var v = row[h];
      out[h] = v == null || v === undefined ? '' : String(v);
    }
    return out;
  }

  function isValidDateParts(y, m, d) {
    if (m < 1 || m > 12 || d < 1 || d > 31) return false;
    var dt = new Date(Date.UTC(y, m - 1, d));
    return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
  }

  function pad2(n) {
    return n < 10 ? '0' + n : String(n);
  }

  function toISO(y, m, d) {
    return y + '-' + pad2(m) + '-' + pad2(d);
  }

  /**
   * @returns {string|null} ISO date or null
   */
  function parseDateToISO(s) {
    if (!s || typeof s !== 'string') return null;
    var t = s.trim();
    if (!t) return null;

    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(t);
    if (m) {
      var y = +m[1];
      var mo = +m[2];
      var d = +m[3];
      return isValidDateParts(y, mo, d) ? toISO(y, mo, d) : null;
    }

    m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(t);
    if (m) {
      var a = +m[1];
      var b = +m[2];
      var yy = +m[3];
      if (a > 12 && b <= 12) {
        return isValidDateParts(yy, b, a) ? toISO(yy, b, a) : null;
      }
      if (b > 12 && a <= 12) {
        return isValidDateParts(yy, a, b) ? toISO(yy, a, b) : null;
      }
      if (a <= 12 && b <= 12) {
        // Ambiguous: deterministic US order MM/DD/YYYY (portfolio default).
        return isValidDateParts(yy, a, b) ? toISO(yy, a, b) : null;
      }
    }
    return null;
  }

  /**
   * Whole-string numeric parse only. Avoids parseFloat('2023-01-15') === 2023 and
   * parseFloat('01/15/2023') === 1, which misclassified date columns as numbers.
   */
  function parseNumberCell(s) {
    if (!s || typeof s !== 'string') return null;
    var t = s.trim().replace(/,/g, '');
    if (t === '' || t === '-' || t === '.') return null;
    if (!/^-?\d+(\.\d+)?$/.test(t)) return null;
    var n = parseFloat(t);
    if (!isFinite(n)) return null;
    return n;
  }

  /** Basic sanity check: local@domain.tld (requires a dot in the domain). */
  function isPlausibleEmail(s) {
    if (!s || typeof s !== 'string') return false;
    var t = s.trim();
    if (t === '') return false;
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(t);
  }

  function isPhoneColumnName(h) {
    var n = String(h).trim().toLowerCase().replace(/\s+/g, '_');
    return (
      n === 'phone' ||
      n === 'phone_number' ||
      n === 'mobile' ||
      n === 'cell' ||
      n === 'cell_phone' ||
      n === 'telephone' ||
      n === 'tel'
    );
  }

  /**
   * US/Canada NANP → (XXX) XXX-XXXX; 7–15 digit intl. → +digits; too short/long or no digits → invalid.
   */
  function normalizePhoneCell(raw) {
    if (!raw || typeof raw !== 'string') return { value: '', invalid: false };
    var t = raw.trim();
    if (t === '') return { value: '', invalid: false };
    var d = t.replace(/\D/g, '');
    if (d.length === 0) return { value: '', invalid: true };
    var work = d;
    if (work.length === 11 && work.charAt(0) === '1') work = work.slice(1);
    if (work.length === 10) {
      return {
        value:
          '(' +
          work.slice(0, 3) +
          ') ' +
          work.slice(3, 6) +
          '-' +
          work.slice(6),
        invalid: false,
      };
    }
    if (d.length >= 7 && d.length <= 15) {
      return { value: '+' + d, invalid: false };
    }
    return { value: '', invalid: true };
  }

  /** Weight / mass columns: 2 decimal places; negatives → absolute value. */
  function isWeightColumnName(h) {
    var n = String(h).trim().toLowerCase().replace(/\s+/g, '_');
    if (n === 'weight' || n === 'mass' || n === 'net_weight' || n === 'gross_weight') return true;
    if (/^weight_(kg|lb|oz|g|grams)$/.test(n)) return true;
    return false;
  }

  /** Money-like columns where negative values use absolute value (typo); salary excluded. */
  function isNonNegativePriceColumnName(h) {
    var n = String(h).trim().toLowerCase().replace(/\s+/g, '_');
    if (n.indexOf('salary') !== -1) return false;
    for (var i = 0; i < MONEY_NAME_PARTS.length; i++) {
      var p = MONEY_NAME_PARTS[i];
      if (p === 'salary') continue;
      if (n === p || n.indexOf(p) !== -1) return true;
    }
    return false;
  }

  /** Only `is_*` flags — avoids treating a generic `active` / `enabled` text column as boolean. */
  function isBooleanColumnName(h) {
    var n = String(h).trim().toLowerCase().replace(/\s+/g, '_');
    return /^is_(active|inactive|enabled|disabled|archived|deleted|published)$/.test(n);
  }

  /** @returns {'TRUE'|'FALSE'|null} */
  function normalizeBooleanCell(v) {
    var s = v != null ? String(v).trim() : '';
    if (s === '') return null;
    var low = s.toLowerCase();
    if (['true', 't', 'yes', 'y', 'on', '1'].indexOf(low) !== -1) return 'TRUE';
    if (['false', 'f', 'no', 'n', 'off', '0'].indexOf(low) !== -1) return 'FALSE';
    var n = parseNumberCell(s);
    if (n !== null) {
      if (n === 1) return 'TRUE';
      if (n === 0) return 'FALSE';
    }
    return null;
  }

  function isSkuSerialColumnName(h) {
    var n = String(h).trim().toLowerCase().replace(/\s+/g, '_');
    if (
      n === 'sku' ||
      n === 'serial' ||
      n === 'serial_number' ||
      n === 'product_sku' ||
      n === 'part_number' ||
      n === 'part_no' ||
      n === 'model_number' ||
      n === 'upc' ||
      n === 'ean' ||
      n === 'imei' ||
      n === 'barcode' ||
      n === 'asset_tag'
    ) {
      return true;
    }
    if (/_sku$|^_sku_|^sku_|_serial$|_upc$|_ean$|_imei$/.test(n)) return true;
    return false;
  }

  function capitalizeWordPart(part) {
    if (part.length === 0) return part;
    var bits = part.split("'");
    if (bits.length === 1) {
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    }
    if (bits.length === 2 && bits[1].length === 1) {
      return (
        bits[0].charAt(0).toUpperCase() +
        bits[0].slice(1).toLowerCase() +
        "'" +
        bits[1].toLowerCase()
      );
    }
    return bits
      .map(function (b) {
        if (b.length === 0) return b;
        return b.charAt(0).toUpperCase() + b.slice(1).toLowerCase();
      })
      .join("'");
  }

  /** Title case words (space / hyphen); SKU/serial columns should skip via caller. */
  function titleCaseWords(s) {
    if (!s || typeof s !== 'string') return '';
    var t = s.trim();
    if (t === '') return '';
    return t
      .split(/\s+/)
      .map(function (word) {
        return word.split('-').map(capitalizeWordPart).join('-');
      })
      .join(' ');
  }

  function rowFingerprint(row, headers) {
    var keys = headers.slice().sort();
    var obj = {};
    for (var i = 0; i < keys.length; i++) {
      var k = keys[i];
      obj[k] = row[k] != null ? String(row[k]) : '';
    }
    return JSON.stringify(obj);
  }

  function cloneRows(rows, headers) {
    return rows.map(function (r) {
      return padRow(r, headers);
    });
  }

  function quantile(sortedAsc, p) {
    var n = sortedAsc.length;
    if (n === 0) return NaN;
    var pos = (n - 1) * p;
    var lo = Math.floor(pos);
    var hi = Math.ceil(pos);
    if (lo === hi) return sortedAsc[lo];
    return sortedAsc[lo] + (sortedAsc[hi] - sortedAsc[lo]) * (pos - lo);
  }

  function inferColumnTypes(headers, rows) {
    var types = {};
    for (var c = 0; c < headers.length; c++) {
      var col = headers[c];
      var nonEmpty = [];
      for (var r = 0; r < rows.length; r++) {
        var v = rows[r][col];
        if (v != null && String(v).trim() !== '') nonEmpty.push(String(v));
      }
      if (nonEmpty.length === 0) {
        types[col] = 'string';
        continue;
      }
      var dateOk = 0;
      var numOk = 0;
      for (var i = 0; i < nonEmpty.length; i++) {
        if (parseDateToISO(nonEmpty[i])) dateOk++;
        if (parseNumberCell(nonEmpty[i]) !== null) numOk++;
      }
      var dr = dateOk / nonEmpty.length;
      var nr = numOk / nonEmpty.length;
      var nameSuggestsDate = /date|datetime|(^|_)(at|time|ts)$/i.test(
        String(col).replace(/\s/g, '_')
      );
      if (nonEmpty.length >= 2 && dr >= INFER_THRESHOLD) types[col] = 'date';
      else if (
        nonEmpty.length >= 2 &&
        nameSuggestsDate &&
        dr >= 0.5 &&
        dr >= nr
      ) {
        types[col] = 'date';
      } else if (nr >= INFER_THRESHOLD) types[col] = 'number';
      else types[col] = 'string';
    }
    return types;
  }

  function step2_trimNullTokens(headers, rows) {
    var out = [];
    var normalizedEmptyCells = 0;
    for (var r = 0; r < rows.length; r++) {
      var row = rows[r];
      var nr = {};
      for (var c = 0; c < headers.length; c++) {
        var h = headers[c];
        var v = row[h] != null ? String(row[h]) : '';
        v = v.trim();
        var low = v.toLowerCase();
        if (NULL_TOKENS.has(low)) {
          v = '';
          normalizedEmptyCells++;
        }
        nr[h] = v;
      }
      out.push(nr);
    }
    return {
      rows: out,
      stats: {
        step: 2,
        label: 'Trim whitespace & null tokens',
        cellsNormalized: normalizedEmptyCells,
      },
    };
  }

  function findEmailColumn(headers) {
    for (var i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim().toLowerCase() === 'email') return headers[i];
    }
    return null;
  }

  /** Event logs: duplicate event_id rows are valid; skip same-email dedupe. */
  function isEventLogShape(headers) {
    var H = headers.map(function (x) {
      return String(x).trim().toLowerCase();
    });
    var hasEventId = H.indexOf('event_id') !== -1;
    var hasEventDate =
      H.indexOf('event_date') !== -1 || H.indexOf('occurred_at') !== -1;
    return hasEventId && hasEventDate;
  }

  /**
   * Keep first row per normalized email; empty email rows always kept.
   */
  function dedupeByEmail(headers, rows, emailCol) {
    var seen = new Set();
    var out = [];
    var dropped = 0;
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var ev = row[emailCol];
      ev = ev != null ? String(ev).trim() : '';
      if (ev === '') {
        out.push(row);
        continue;
      }
      var norm = ev.toLowerCase();
      if (seen.has(norm)) {
        dropped++;
        continue;
      }
      seen.add(norm);
      out.push(row);
    }
    return { rows: out, duplicatesRemoved: dropped };
  }

  function step3_deduplicate(headers, rows) {
    var duplicateRowsFull = 0;
    var fullRowDedupeSkipped = false;
    var eventLog = isEventLogShape(headers);
    var out;

    if (eventLog) {
      fullRowDedupeSkipped = true;
      out = cloneRows(rows, headers);
    } else {
      var seen = new Set();
      out = [];
      for (var i = 0; i < rows.length; i++) {
        var fp = rowFingerprint(rows[i], headers);
        if (seen.has(fp)) {
          duplicateRowsFull++;
          continue;
        }
        seen.add(fp);
        out.push(rows[i]);
      }
    }

    var emailDup = 0;
    var finalRows = out;
    if (!eventLog) {
      var emailCol = findEmailColumn(headers);
      if (emailCol) {
        var ed = dedupeByEmail(headers, out, emailCol);
        finalRows = ed.rows;
        emailDup = ed.duplicatesRemoved;
      }
    }

    return {
      rows: finalRows,
      stats: {
        step: 3,
        label: 'Deduplicate rows',
        duplicatesRemovedFullRow: duplicateRowsFull,
        duplicatesRemovedByEmail: emailDup,
        duplicatesRemoved: duplicateRowsFull + emailDup,
        rowsAfter: finalRows.length,
        fullRowDedupeSkipped: fullRowDedupeSkipped,
      },
    };
  }

  function step4_coerce(headers, rows, inferredTypes) {
    var invalidDates = 0;
    var invalidNumbers = 0;
    var invalidEmails = 0;
    var invalidPhones = 0;
    var phonesFormatted = 0;
    var invalidBooleans = 0;
    var booleansNormalized = 0;
    var priceNegativesAbsoluted = 0;
    var weightNegativesAbsoluted = 0;
    var weightsFormatted = 0;
    var stringsTitleCased = 0;
    var invalidByColumn = {};
    for (var ic = 0; ic < headers.length; ic++) {
      invalidByColumn[headers[ic]] = { date: 0, number: 0 };
    }
    var out = [];

    for (var r = 0; r < rows.length; r++) {
      var row = rows[r];
      var nr = {};
      for (var c = 0; c < headers.length; c++) {
        var h = headers[c];
        var v = row[h] != null ? String(row[h]) : '';
        var t = inferredTypes[h] || 'string';
        if (v === '') {
          nr[h] = '';
          continue;
        }
        if (String(h).trim().toLowerCase() === 'email') {
          if (isPlausibleEmail(v)) {
            nr[h] = v.trim().toLowerCase();
          } else {
            nr[h] = '';
            invalidEmails++;
          }
          continue;
        }
        if (isPhoneColumnName(h)) {
          var ph = normalizePhoneCell(v);
          if (ph.invalid) {
            nr[h] = '';
            invalidPhones++;
          } else {
            nr[h] = ph.value;
            if (ph.value !== v.trim()) phonesFormatted++;
          }
          continue;
        }
        if (isBooleanColumnName(h)) {
          var bv = normalizeBooleanCell(v);
          if (bv === null) {
            nr[h] = '';
            invalidBooleans++;
          } else {
            nr[h] = bv;
            if (bv !== v.trim()) booleansNormalized++;
          }
          continue;
        }
        if (t === 'date') {
          var iso = parseDateToISO(v);
          if (iso) {
            nr[h] = iso;
          } else {
            nr[h] = '';
            invalidDates++;
            invalidByColumn[h].date++;
          }
        } else if (t === 'number') {
          var n = parseNumberCell(v);
          if (n !== null) {
            if (isWeightColumnName(h)) {
              if (n < 0) {
                n = Math.abs(n);
                weightNegativesAbsoluted++;
              }
              var wf = n.toFixed(2);
              nr[h] = wf;
              var vin = v.trim().replace(/,/g, '');
              if (wf !== vin) weightsFormatted++;
            } else if (isNonNegativePriceColumnName(h) && n < 0) {
              n = Math.abs(n);
              priceNegativesAbsoluted++;
              nr[h] = String(n);
            } else {
              nr[h] = String(n);
            }
          } else {
            nr[h] = '';
            invalidNumbers++;
            invalidByColumn[h].number++;
          }
        } else {
          if (isWeightColumnName(h) && parseNumberCell(v) !== null) {
            var wn = parseNumberCell(v);
            if (wn < 0) {
              wn = Math.abs(wn);
              weightNegativesAbsoluted++;
            }
            var wfs = wn.toFixed(2);
            nr[h] = wfs;
            if (wfs !== v.trim().replace(/,/g, '')) weightsFormatted++;
          } else if (isWeightColumnName(h)) {
            nr[h] = v;
          } else if (isNonNegativePriceColumnName(h) && parseNumberCell(v) !== null) {
            var pn = parseNumberCell(v);
            if (pn < 0) {
              pn = Math.abs(pn);
              priceNegativesAbsoluted++;
            }
            nr[h] = String(pn);
          } else if (isNonNegativePriceColumnName(h)) {
            nr[h] = v;
          } else if (!isSkuSerialColumnName(h)) {
            var titled = titleCaseWords(v);
            nr[h] = titled;
            if (titled !== v.trim()) stringsTitleCased++;
          } else {
            nr[h] = v;
          }
        }
      }
      out.push(nr);
    }

    var typeConflicts =
      invalidDates +
      invalidNumbers +
      invalidEmails +
      invalidPhones +
      invalidBooleans;

    return {
      rows: out,
      stats: {
        step: 4,
        label: 'Coerce dates & numbers',
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
        invalidByColumn: invalidByColumn,
        inferredTypes: inferredTypes,
      },
    };
  }

  function step5_winsorize(headers, rows, inferredTypes) {
    var perCol = {};
    var outliersCappedTotal = 0;
    var out = cloneRows(rows, headers);

    for (var c = 0; c < headers.length; c++) {
      var col = headers[c];
      if (inferredTypes[col] !== 'number') {
        perCol[col] = { skipped: true, reason: 'not numeric column' };
        continue;
      }
      if (isWeightColumnName(col)) {
        perCol[col] = {
          skipped: true,
          reason: 'weight column (2dp + abs in step 4)',
        };
        continue;
      }

      var values = [];
      var indices = [];
      for (var r = 0; r < out.length; r++) {
        var cell = out[r][col];
        if (cell === '') continue;
        var n = parseNumberCell(cell);
        if (n !== null) {
          values.push(n);
          indices.push(r);
        }
      }

      if (values.length < WINSOR_MIN_VALUES) {
        perCol[col] = {
          skipped: true,
          reason: 'insufficient data (< ' + WINSOR_MIN_VALUES + ' numeric values)',
        };
        continue;
      }

      var sorted = values.slice().sort(function (a, b) {
        return a - b;
      });
      var q1 = quantile(sorted, 0.25);
      var q3 = quantile(sorted, 0.75);
      var iqr = q3 - q1;
      if (iqr === 0) {
        perCol[col] = { skipped: true, reason: 'IQR is zero' };
        continue;
      }

      var lo = q1 - IQR_K * iqr;
      var hi = q3 + IQR_K * iqr;
      var capped = 0;

      for (var i = 0; i < out.length; i++) {
        var cellv = out[i][col];
        if (cellv === '') continue;
        var nv = parseNumberCell(cellv);
        if (nv === null) continue;
        if (nv < lo || nv > hi) {
          var clamped = nv < lo ? lo : hi;
          out[i][col] = String(clamped);
          capped++;
        }
      }

      outliersCappedTotal += capped;
      perCol[col] = {
        skipped: false,
        outliersCapped: capped,
        lowerBound: lo,
        upperBound: hi,
        q1: q1,
        q3: q3,
        iqr: iqr,
      };
    }

    return {
      rows: out,
      stats: {
        step: 5,
        label: 'Winsorize outliers (IQR k=' + IQR_K + ')',
        outliersCappedTotal: outliersCappedTotal,
        perColumn: perCol,
      },
    };
  }

  /** Column names like id, customer_id, order_id, event_id (not *_date). */
  function findKeyColumn(headers) {
    for (var i = 0; i < headers.length; i++) {
      var name = String(headers[i]).trim();
      if (/^(?:id|.*_id)$/i.test(name)) return headers[i];
    }
    return null;
  }

  /** @returns {{ prefix: string, num: number } | null} */
  function parseLetterNumberKey(s) {
    var t = s != null ? String(s).trim() : '';
    var m = /^([A-Za-z]+)(\d+)$/.exec(t);
    if (!m) return null;
    return { prefix: m[1], num: parseInt(m[2], 10) };
  }

  /**
   * Impute empty key cells when prev/next keys share a letter prefix and differ by 2 (single gap).
   */
  function imputeSequentialKeys(headers, rows) {
    var keyCol = findKeyColumn(headers);
    if (!keyCol) return { rows: cloneRows(rows, headers), keysImputed: 0 };
    var out = cloneRows(rows, headers);
    var imputed = 0;
    for (var i = 0; i < out.length; i++) {
      var raw = out[i][keyCol];
      raw = raw != null ? String(raw).trim() : '';
      if (raw !== '') continue;
      var prevKey = null;
      for (var j = i - 1; j >= 0; j--) {
        var vj = out[j][keyCol];
        vj = vj != null ? String(vj).trim() : '';
        if (vj !== '') {
          prevKey = vj;
          break;
        }
      }
      var nextKey = null;
      for (var k = i + 1; k < out.length; k++) {
        var wk = out[k][keyCol];
        wk = wk != null ? String(wk).trim() : '';
        if (wk !== '') {
          nextKey = wk;
          break;
        }
      }
      if (!prevKey || !nextKey) continue;
      var prevP = parseLetterNumberKey(prevKey);
      var nextP = parseLetterNumberKey(nextKey);
      if (!prevP || !nextP) continue;
      if (prevP.prefix !== nextP.prefix) continue;
      if (nextP.num !== prevP.num + 2) continue;
      out[i][keyCol] = prevP.prefix + String(prevP.num + 1);
      imputed++;
    }
    return { rows: out, keysImputed: imputed };
  }

  function isMoneyLikeColumn(name, inferredTypes) {
    if (inferredTypes[name] !== 'number') return false;
    var n = String(name).toLowerCase();
    for (var i = 0; i < MONEY_NAME_PARTS.length; i++) {
      var p = MONEY_NAME_PARTS[i];
      if (n === p || n.indexOf(p) !== -1) return true;
    }
    return false;
  }

  /**
   * Format money-like numeric columns to two decimals; empty or invalid → 0.00.
   */
  function formatMoneyColumns(headers, rows, inferredTypes) {
    var moneyCols = [];
    for (var c = 0; c < headers.length; c++) {
      var h = headers[c];
      if (isMoneyLikeColumn(h, inferredTypes)) moneyCols.push(h);
    }
    if (moneyCols.length === 0) return { rows: rows, moneyCellsFormatted: 0 };
    var out = cloneRows(rows, headers);
    var formatted = 0;
    for (var r = 0; r < out.length; r++) {
      for (var m = 0; m < moneyCols.length; m++) {
        var col = moneyCols[m];
        var v = out[r][col];
        var s = v != null ? String(v).trim() : '';
        var n = parseNumberCell(s);
        if (s === '' || n === null) {
          out[r][col] = '0.00';
          formatted++;
        } else {
          var fixed = n.toFixed(2);
          if (String(v).trim() !== fixed) formatted++;
          out[r][col] = fixed;
        }
      }
    }
    return { rows: out, moneyCellsFormatted: formatted };
  }

  /**
   * Impute sequential keys, format money, drop rows still missing id, fill remaining blanks with N/A.
   */
  function step6_finalize(headers, rows, inferredTypes) {
    var im = imputeSequentialKeys(headers, rows);
    var out = im.rows;
    var keysImputed = im.keysImputed;

    var fm = formatMoneyColumns(headers, out, inferredTypes);
    out = fm.rows;
    var moneyCellsFormatted = fm.moneyCellsFormatted;

    var keyCol = findKeyColumn(headers);
    var afterDrop = [];
    var dropped = 0;
    for (var r = 0; r < out.length; r++) {
      if (keyCol) {
        var raw = out[r][keyCol];
        raw = raw != null ? String(raw).trim() : '';
        if (raw === '') {
          dropped++;
          continue;
        }
      }
      afterDrop.push(padRow(out[r], headers));
    }

    var filled = 0;
    for (var i = 0; i < afterDrop.length; i++) {
      for (var c = 0; c < headers.length; c++) {
        var h = headers[c];
        var v = afterDrop[i][h];
        if (v === '' || v == null) {
          afterDrop[i][h] = 'N/A';
          filled++;
        }
      }
    }

    return {
      rows: afterDrop,
      stats: {
        step: 6,
        label: 'Impute id gaps, money (2dp), drop empty id, fill N/A',
        keysImputed: keysImputed,
        moneyCellsFormatted: moneyCellsFormatted,
        rowsDropped: dropped,
        cellsFilledNA: filled,
      },
    };
  }

  /**
   * Parse CSV string (step 1). Uses Papa Parse.
   */
  function parseCsv(text) {
    var warnings = [];
    if (typeof Papa === 'undefined') {
      return { ok: false, error: 'Papa Parse not loaded', headers: [], rows: [] };
    }
    var normalized = normalizeRaw(text);
    if (normalized.trim() === '') {
      return { ok: false, error: 'Empty input', headers: [], rows: [], warnings: warnings };
    }

    var parsed = Papa.parse(normalized, {
      header: true,
      skipEmptyLines: 'greedy',
      dynamicTyping: false,
    });

    if (parsed.errors && parsed.errors.length) {
      var fatal = parsed.errors.some(function (e) {
        return e.type === 'Quotes' || e.type === 'Delimiter';
      });
      if (fatal) {
        return {
          ok: false,
          error: parsed.errors.map(function (e) {
            return e.message || String(e);
          }).join('; '),
          headers: [],
          rows: [],
          warnings: warnings,
        };
      }
      warnings = parsed.errors.map(function (e) {
        return e.message || String(e);
      });
    }

    var fields = parsed.meta.fields || [];
    var headers = fields.filter(function (f) {
      return f != null && String(f).length > 0;
    });
    if (headers.length === 0 && parsed.data && parsed.data[0]) {
      headers = Object.keys(parsed.data[0]);
    }

    var rows = [];
    for (var i = 0; i < parsed.data.length; i++) {
      var rawRow = parsed.data[i];
      if (!rawRow || typeof rawRow !== 'object') continue;
      var padded = padRow(rawRow, headers);
      var allEmpty = headers.every(function (h) {
        return padded[h] === '';
      });
      if (allEmpty) continue;
      rows.push(padded);
    }

    return {
      ok: true,
      headers: headers,
      rows: rows,
      warnings: warnings,
    };
  }

  function checkLimits(byteLength, rowCount) {
    if (byteLength > MAX_BYTES_HARD) {
      return {
        ok: false,
        error: 'File exceeds hard limit of ' + Math.round(MAX_BYTES_HARD / (1024 * 1024)) + ' MB.',
      };
    }
    if (rowCount > MAX_ROWS) {
      return {
        ok: false,
        error: 'Row count exceeds limit of ' + MAX_ROWS.toLocaleString() + '.',
      };
    }
    return {
      ok: true,
      softWarn: byteLength > SOFT_WARN_BYTES,
      softWarnMessage:
        byteLength > SOFT_WARN_BYTES
          ? 'Large file: processing may be slow in the browser.'
          : null,
    };
  }

  function runPipeline(headers, rows) {
    var stepStats = [];
    var h = headers.slice();

    var s2 = step2_trimNullTokens(h, rows);
    stepStats.push(s2.stats);
    var r2 = s2.rows;

    var s3 = step3_deduplicate(h, r2);
    stepStats.push(s3.stats);
    var r3 = s3.rows;

    var inferred = inferColumnTypes(h, r3);
    var s4 = step4_coerce(h, r3, inferred);
    stepStats.push(s4.stats);
    var r4 = s4.rows;

    var s5 = step5_winsorize(h, r4, inferred);
    stepStats.push(s5.stats);
    var r5 = s5.rows;

    var s6 = step6_finalize(h, r5, inferred);
    stepStats.push(s6.stats);
    var r6 = s6.rows;

    return {
      finalRows: r6,
      stepStats: stepStats,
      inferredTypes: inferred,
    };
  }

  function exportCsv(headers, rows) {
    var esc = function (cell) {
      var s = cell == null ? '' : String(cell);
      if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
      return s;
    };
    var lines = [headers.map(esc).join(',')];
    for (var r = 0; r < rows.length; r++) {
      var row = rows[r];
      var line = headers.map(function (h) {
        return esc(row[h]);
      });
      lines.push(line.join(','));
    }
    return lines.join('\n');
  }

  global.ETLPipeline = {
    normalizeRaw: normalizeRaw,
    parseCsv: parseCsv,
    checkLimits: checkLimits,
    runPipeline: runPipeline,
    exportCsv: exportCsv,
    inferColumnTypes: inferColumnTypes,
    constants: {
      MAX_BYTES_HARD: MAX_BYTES_HARD,
      SOFT_WARN_BYTES: SOFT_WARN_BYTES,
      MAX_ROWS: MAX_ROWS,
      PREVIEW_ROWS: 100,
    },
  };
})(typeof window !== 'undefined' ? window : this);
