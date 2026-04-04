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
            />
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
          <h2>
            {{ result.farm }} / {{ result.file_type }} / {{ result.date }}
          </h2>
          <span class="row-count">{{ result.row_count.toLocaleString() }} rows</span>
        </div>

        <div class="table-scroll">
          <table>
            <thead>
              <tr>
                <th v-for="col in result.columns" :key="col">{{ col }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, i) in result.rows" :key="i">
                <td v-for="(cell, j) in row" :key="j">
                  {{ cell ?? '—' }}
                </td>
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
import { fetchWindFarms, fetchColumns, fetchDayData } from './api.js'

// ── State ──────────────────────────────────────────────────────────────────

/** All wind farms returned by /wind-farms */
const farms = ref([])

/** Columns metadata returned by /wind-farms/columns  { farm, columns_by_type } */
const columnsMap = ref([])

const selectedFarm     = ref('')
const selectedFileType = ref('')
const selectedDate     = ref('')
const selectedColumns  = ref([])   // empty = send nothing (all columns)
const allColumns       = ref(true) // checkbox state

const loading = ref(false)
const error   = ref('')
const result  = ref(null)

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

/** True only when all required fields are filled */
const canFetch = computed(() =>
  selectedFarm.value &&
  selectedFileType.value &&
  selectedDate.value
)

// ── Handlers ───────────────────────────────────────────────────────────────

function onFarmChange() {
  selectedFileType.value = ''
  selectedColumns.value  = []
  result.value           = null
  error.value            = ''
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
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// ── Initialisation ─────────────────────────────────────────────────────────

onMounted(async () => {
  try {
    const [farmsData, colsData] = await Promise.all([
      fetchWindFarms(),
      fetchColumns(),
    ])
    farms.value      = farmsData
    columnsMap.value = colsData
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

