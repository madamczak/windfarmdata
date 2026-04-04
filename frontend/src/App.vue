<template>
  <!-- =====================================================================
    App.vue — Wind Farm Data Explorer
    - Step 1: select farm + file type + date
    - Step 2: select columns (or "All columns")
    - Step 3: click Fetch Data → table appears
  ====================================================================== -->
  <div class="app">
    <header class="app-header">
      <h1>🌬️ Wind Farm Data Explorer</h1>
    </header>

    <main class="app-body">
      <!-- ── Controls panel ─────────────────────────────────────────── -->
      <section class="controls card">

        <!-- Row 1: Farm + File type -->
        <div class="control-row">
          <div class="control-group">
            <label for="farm-select">Wind Farm</label>
            <select id="farm-select" v-model="selectedFarm" @change="onFarmChange">
              <option value="" disabled>— select farm —</option>
              <option
                v-for="farm in farms"
                :key="farm.directory"
                :value="farm.directory"
              >
                {{ farm.name }} ({{ farm.turbine_count }} turbines)
              </option>
            </select>
          </div>

          <div class="control-group">
            <label for="file-type-select">File Type</label>
            <select id="file-type-select" v-model="selectedFileType" @change="onFileTypeChange">
              <option value="" disabled>— select type —</option>
              <option
                v-for="ft in availableFileTypes"
                :key="ft"
                :value="ft"
              >
                {{ ft }}
              </option>
            </select>
          </div>

          <div class="control-group">
            <label for="date-input">Date</label>
            <input
              id="date-input"
              type="date"
              v-model="selectedDate"
              :min="minDate"
              :max="maxDate"
              :disabled="!selectedFarm"
            />
            <span v-if="minDate && maxDate" class="date-hint">
              {{ minDate }} → {{ maxDate }}
            </span>
          </div>
        </div>

        <!-- Row 2: Column picker -->
        <div v-if="availableColumns.length" class="control-group column-picker">
          <label>Columns</label>

          <div class="column-toggle-row">
            <label class="checkbox-label all-cols">
              <input type="checkbox" v-model="allColumns" @change="onAllColumnsToggle" />
              <span>All columns</span>
            </label>
          </div>

          <div v-if="!allColumns" class="column-grid">
            <label
              v-for="col in availableColumns"
              :key="col"
              class="checkbox-label"
            >
              <input type="checkbox" :value="col" v-model="selectedColumns" />
              <span>{{ col }}</span>
            </label>
          </div>
        </div>

        <!-- Row 3: Fetch button -->
        <div class="control-row actions">
          <button
            class="btn-primary"
            :disabled="!canFetch || loading"
            @click="fetchData"
          >
            <span v-if="loading">⏳ Loading…</span>
            <span v-else>Fetch Data</span>
          </button>
          <span v-if="error" class="error-msg">{{ error }}</span>
        </div>
      </section>

      <!-- ── Results table ──────────────────────────────────────────── -->
      <section v-if="result" class="results card">
        <div class="results-header">
          <h2>{{ result.farm }} / {{ result.file_type }} / {{ result.date }}</h2>
          <span class="row-count">
            {{ filteredRows.length.toLocaleString() }}
            <template v-if="filteredRows.length !== result.row_count">
              / {{ result.row_count.toLocaleString() }}
            </template>
            rows
          </span>
          <input
            class="global-filter"
            type="search"
            v-model="globalFilter"
            placeholder="🔍 Search all columns…"
          />
          <button class="btn-clear" @click="clearFilters" title="Clear all filters &amp; sort">
            ✕ Clear filters
          </button>
        </div>

        <!-- ── Data quality report ──────────────────────────────────── -->
        <div class="report">
          <div class="report-title">
            📊 Data Quality Report
            <span class="report-subtitle">{{ result.row_count }} rows · {{ result.columns.length }} columns</span>
          </div>
          <div class="report-grid">
            <div
              v-for="stat in columnStats"
              :key="stat.col"
              class="stat-card"
              :class="stat.fillRate < 50 ? 'stat-bad' : stat.fillRate < 90 ? 'stat-warn' : 'stat-ok'"
            >
              <div class="stat-col-name" :title="stat.col">{{ stat.col }}</div>
              <div class="stat-bar-wrap">
                <div class="stat-bar" :style="{ width: stat.fillRate + '%' }"></div>
              </div>
              <div class="stat-numbers">
                <span class="stat-fill">{{ stat.fillRate }}% filled</span>
                <span v-if="stat.nullCount"  class="stat-null">{{ stat.nullCount }} null</span>
                <span v-if="stat.zeroCount"  class="stat-zero">{{ stat.zeroCount }} zero</span>
              </div>
            </div>
          </div>
        </div>

        <div class="table-scroll">
          <table>
            <thead>
              <!-- Row 1: column headers with sort arrows -->
              <tr>
                <th
                  v-for="(col, ci) in result.columns"
                  :key="col"
                  class="sortable"
                  @click="setSort(ci)"
                >
                  <span class="th-label">{{ col }}</span>
                  <span class="sort-icon">
                    <template v-if="sortCol === ci">
                      {{ sortDir === 1 ? '▲' : '▼' }}
                    </template>
                    <template v-else>⇅</template>
                  </span>
                </th>
              </tr>
              <!-- Row 2: per-column filter inputs -->
              <tr class="filter-row">
                <th v-for="(col, ci) in result.columns" :key="'f' + ci">
                  <input
                    class="col-filter"
                    type="text"
                    :placeholder="'filter…'"
                    v-model="colFilters[ci]"
                  />
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, i) in filteredRows" :key="i">
                <td v-for="(cell, j) in row" :key="j">{{ cell ?? '—' }}</td>
              </tr>
              <tr v-if="filteredRows.length === 0">
                <td :colspan="result.columns.length" class="no-rows">No rows match the current filters.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </main>
  </div>
</template>

<script setup>
/**
 * App.vue — main (and only) view for the Wind Farm Data Explorer.
 * Uses Vue 3 Composition API with <script setup>.
 */
import { ref, computed, onMounted } from 'vue'
import { fetchWindFarms, fetchColumns, fetchDayData, fetchTimeRanges } from './api.js'

// ── State ──────────────────────────────────────────────────────────────────

/** All wind farms returned by /wind-farms */
const farms = ref([])

/** Columns metadata returned by /wind-farms/columns  { farm, columns_by_type } */
const columnsMap = ref([])

/** Time ranges per farm: { farm, earliest, latest } */
const timeRanges = ref([])

const selectedFarm     = ref('')
const selectedFileType = ref('')
const selectedDate     = ref('')
const selectedColumns  = ref([])
const allColumns       = ref(true)

const loading = ref(false)
const error   = ref('')
const result  = ref(null)

// ── Sort & filter state ────────────────────────────────────────────────────
const globalFilter = ref('')   // global search string
const colFilters   = ref([])   // per-column filter strings (indexed by column index)
const sortCol      = ref(null) // column index being sorted, or null
const sortDir      = ref(1)    // 1 = ascending, -1 = descending

// ── Derived ────────────────────────────────────────────────────────────────

/** File types available for the selected farm */
const availableFileTypes = computed(() => {
  if (!selectedFarm.value) return []
  const entry = columnsMap.value.find(e => e.farm === selectedFarm.value)
  return entry ? Object.keys(entry.columns_by_type) : []
})

/** Column names for the selected farm + file type */
const availableColumns = computed(() => {
  if (!selectedFarm.value || !selectedFileType.value) return []
  const entry = columnsMap.value.find(e => e.farm === selectedFarm.value)
  return entry?.columns_by_type[selectedFileType.value] ?? []
})

/** ISO date string (YYYY-MM-DD) for the earliest record in the selected farm */
const minDate = computed(() => {
  if (!selectedFarm.value) return ''
  const tr = timeRanges.value.find(t => t.farm === selectedFarm.value)
  if (!tr?.earliest) return ''
  // earliest may be "2016-01-03 00:00:00" or "2016-01-03T00:00:00+0000"
  return tr.earliest.slice(0, 10)
})

/** ISO date string (YYYY-MM-DD) for the latest record in the selected farm */
const maxDate = computed(() => {
  if (!selectedFarm.value) return ''
  const tr = timeRanges.value.find(t => t.farm === selectedFarm.value)
  if (!tr?.latest) return ''
  return tr.latest.slice(0, 10)
})

/** True only when all required fields are filled */
const canFetch = computed(() =>
  selectedFarm.value &&
  selectedFileType.value &&
  selectedDate.value
)

/**
 * Rows after applying global filter, per-column filters, and sort.
 * All filtering is case-insensitive string matching.
 */
const filteredRows = computed(() => {
  if (!result.value) return []

  const global = globalFilter.value.trim().toLowerCase()
  const perCol = colFilters.value.map(f => (f ?? '').trim().toLowerCase())

  let rows = result.value.rows

  // Global filter — row must contain the string in at least one cell
  if (global) {
    rows = rows.filter(row =>
      row.some(cell => String(cell ?? '').toLowerCase().includes(global))
    )
  }

  // Per-column filters
  perCol.forEach((f, ci) => {
    if (!f) return
    rows = rows.filter(row =>
      String(row[ci] ?? '').toLowerCase().includes(f)
    )
  })

  // Sort
  if (sortCol.value !== null) {
    const ci  = sortCol.value
    const dir = sortDir.value
    rows = [...rows].sort((a, b) => {
      const av = a[ci] ?? ''
      const bv = b[ci] ?? ''
      // Numeric sort when both values are numbers
      const an = parseFloat(av), bn = parseFloat(bv)
      if (!isNaN(an) && !isNaN(bn)) return (an - bn) * dir
      return String(av).localeCompare(String(bv)) * dir
    })
  }

  return rows
})

/**
 * Per-column data quality stats computed from the raw (unfiltered) result rows.
 * For each column: null count, zero count, and fill rate (non-null %).
 */
const columnStats = computed(() => {
  if (!result.value) return []
  const { columns, rows } = result.value
  const total = rows.length
  if (total === 0) return []

  return columns.map((col, ci) => {
    let nullCount = 0
    let zeroCount = 0
    for (const row of rows) {
      const v = row[ci]
      if (v === null || v === undefined || v === '') {
        nullCount++
      } else if (v === 0 || v === '0') {
        zeroCount++
      }
    }
    const filled   = total - nullCount
    const fillRate = Math.round((filled / total) * 100)
    return { col, nullCount, zeroCount, fillRate }
  })
})

// ── Handlers ───────────────────────────────────────────────────────────────

function onFarmChange() {
  selectedFileType.value = ''
  selectedColumns.value  = []
  result.value           = null
  error.value            = ''
  // Auto-set date to the first available date for this farm
  const tr = timeRanges.value.find(t => t.farm === selectedFarm.value)
  selectedDate.value = tr?.earliest ? tr.earliest.slice(0, 10) : ''
}

function onFileTypeChange() {
  selectedColumns.value = []
  allColumns.value      = true
  result.value          = null
  error.value           = ''
}

function onAllColumnsToggle() {
  if (allColumns.value) {
    selectedColumns.value = []
  }
}

/** Toggle sort on a column: none → asc → desc → none */
function setSort(ci) {
  if (sortCol.value !== ci) {
    sortCol.value = ci
    sortDir.value = 1
  } else if (sortDir.value === 1) {
    sortDir.value = -1
  } else {
    sortCol.value = null
    sortDir.value = 1
  }
}

/** Clear all filters and sort */
function clearFilters() {
  globalFilter.value = ''
  colFilters.value   = result.value ? Array(result.value.columns.length).fill('') : []
  sortCol.value      = null
  sortDir.value      = 1
}

async function fetchData() {
  if (!canFetch.value) return
  loading.value = true
  error.value   = ''
  result.value  = null

  try {
    const cols = allColumns.value ? [] : selectedColumns.value
    result.value = await fetchDayData(
      selectedFarm.value,
      selectedDate.value,
      selectedFileType.value,
      cols
    )
    // Reset sort and filters for the new dataset
    globalFilter.value = ''
    colFilters.value   = Array(result.value.columns.length).fill('')
    sortCol.value      = null
    sortDir.value      = 1
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// ── Initialisation ─────────────────────────────────────────────────────────

onMounted(async () => {
  try {
    const [farmsData, colsData, rangesData] = await Promise.all([
      fetchWindFarms(),
      fetchColumns(),
      fetchTimeRanges(),
    ])
    farms.value      = farmsData
    columnsMap.value = colsData
    timeRanges.value = rangesData
  } catch (e) {
    error.value = `Could not load farm metadata: ${e.message}`
  }
})
</script>

<style>
/* ── Global reset ─────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: system-ui, -apple-system, sans-serif;
  font-size: 14px;
  background: #f0f2f5;
  color: #1a1a2e;
}

/* ── Layout ───────────────────────────────────────────────────────────── */
.app { display: flex; flex-direction: column; min-height: 100vh; }

.app-header {
  background: #1a1a2e;
  color: #fff;
  padding: 16px 24px;
}
.app-header h1 { font-size: 20px; font-weight: 600; }

.app-body {
  flex: 1;
  padding: 24px;
  display: flex;
  flex-direction: column;
  gap: 20px;
  max-width: 1600px;
  width: 100%;
  margin: 0 auto;
}

/* ── Card ─────────────────────────────────────────────────────────────── */
.card {
  background: #fff;
  border-radius: 10px;
  padding: 20px 24px;
  box-shadow: 0 2px 8px rgba(0,0,0,.08);
}

/* ── Controls ─────────────────────────────────────────────────────────── */
.controls { display: flex; flex-direction: column; gap: 18px; }

.control-row {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  align-items: flex-end;
}

.control-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 180px;
}
.control-group label {
  font-weight: 600;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: .05em;
  color: #555;
}

select, input[type="date"] {
  padding: 8px 10px;
  border: 1px solid #d0d5dd;
  border-radius: 6px;
  font-size: 14px;
  background: #fff;
  color: #1a1a2e;
  cursor: pointer;
}
select:focus, input[type="date"]:focus {
  outline: 2px solid #4361ee;
  border-color: transparent;
}
input[type="date"]:disabled {
  background: #f0f2f5;
  color: #aaa;
  cursor: not-allowed;
}
.date-hint {
  font-size: 11px;
  color: #888;
  margin-top: 2px;
}

/* ── Column picker ────────────────────────────────────────────────────── */
.column-picker { max-width: 100%; }

.column-toggle-row { margin-bottom: 10px; }

.column-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 6px 12px;
  max-height: 240px;
  overflow-y: auto;
  border: 1px solid #e4e7ec;
  border-radius: 6px;
  padding: 10px;
  background: #fafafa;
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 7px;
  font-size: 13px;
  cursor: pointer;
  user-select: none;
  padding: 3px 0;
}
.checkbox-label.all-cols { font-weight: 600; }
.checkbox-label input { accent-color: #4361ee; width: 15px; height: 15px; cursor: pointer; }

/* ── Actions row ──────────────────────────────────────────────────────── */
.actions { align-items: center; }

.btn-primary {
  padding: 10px 28px;
  background: #4361ee;
  color: #fff;
  border: none;
  border-radius: 7px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: background .15s;
}
.btn-primary:hover:not(:disabled) { background: #3451d1; }
.btn-primary:disabled { opacity: .5; cursor: not-allowed; }

.error-msg {
  color: #e53e3e;
  font-size: 13px;
  font-weight: 500;
}

/* ── Data quality report ──────────────────────────────────────────────── */
.report {
  border: 1px solid #e4e7ec;
  border-radius: 8px;
  padding: 14px 16px;
  background: #fafbfc;
}

.report-title {
  font-weight: 700;
  font-size: 13px;
  margin-bottom: 12px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.report-subtitle {
  font-weight: 400;
  font-size: 12px;
  color: #888;
}

.report-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 8px;
}

.stat-card {
  background: #fff;
  border: 1px solid #e4e7ec;
  border-radius: 7px;
  padding: 8px 10px;
  display: flex;
  flex-direction: column;
  gap: 5px;
}
.stat-card.stat-ok   { border-left: 3px solid #38a169; }
.stat-card.stat-warn { border-left: 3px solid #d69e2e; }
.stat-card.stat-bad  { border-left: 3px solid #e53e3e; }

.stat-col-name {
  font-size: 11px;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  color: #333;
}

.stat-bar-wrap {
  height: 5px;
  background: #e4e7ec;
  border-radius: 3px;
  overflow: hidden;
}
.stat-bar {
  height: 100%;
  border-radius: 3px;
  background: #4361ee;
  transition: width .3s;
}
.stat-ok   .stat-bar { background: #38a169; }
.stat-warn .stat-bar { background: #d69e2e; }
.stat-bad  .stat-bar { background: #e53e3e; }

.stat-numbers {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  font-size: 10px;
}
.stat-fill { color: #555; font-weight: 600; }
.stat-null { color: #e53e3e; }
.stat-zero { color: #d69e2e; }

/* ── Results ──────────────────────────────────────────────────────────── */
.results { display: flex; flex-direction: column; gap: 14px; }

.results-header {
  display: flex;
  align-items: baseline;
  gap: 14px;
  flex-wrap: wrap;
}
.results-header h2 { font-size: 15px; font-weight: 600; }
.row-count {
  font-size: 12px;
  color: #888;
  background: #f0f2f5;
  padding: 3px 9px;
  border-radius: 20px;
}

.table-scroll {
  overflow: auto;
  max-height: 520px;
  border: 1px solid #e4e7ec;
  border-radius: 7px;
}

table {
  border-collapse: collapse;
  width: 100%;
  font-size: 13px;
}

thead {
  position: sticky;
  top: 0;
  z-index: 1;
  background: #1a1a2e;
  color: #fff;
}

th {
  padding: 10px 12px;
  text-align: left;
  font-weight: 600;
  white-space: nowrap;
  border-right: 1px solid #2d2d4e;
}
th:last-child { border-right: none; }

tbody tr:nth-child(even) { background: #f7f8fc; }
tbody tr:hover { background: #eef1fd; }

td {
  padding: 7px 12px;
  border-bottom: 1px solid #e4e7ec;
  border-right: 1px solid #e4e7ec;
  white-space: nowrap;
}
td:last-child { border-right: none; }
</style>

