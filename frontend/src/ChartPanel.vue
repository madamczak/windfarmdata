<template>
  <!--
    ChartPanel.vue — Charts tab for the Wind Farm Data Explorer.
    Renders one line chart per selected numeric column, all sharing
    the same time axis (first column detected as timestamp).
    Uses Chart.js via vue-chartjs.
  -->
  <div class="chart-panel">

    <!-- ── Controls row ─────────────────────────────────────────────── -->
    <div class="chart-controls">
      <!-- Column selector: only numeric columns are listed -->
      <div class="chart-control-group">
        <label class="chart-label">Columns to plot</label>
        <div class="chart-col-list">
          <label
            v-for="col in numericColumns"
            :key="col"
            class="chart-checkbox"
          >
            <input
              type="checkbox"
              :value="col"
              v-model="selectedCols"
            />
            {{ col }}
          </label>
          <span v-if="numericColumns.length === 0" class="chart-empty">
            No numeric columns in this dataset.
          </span>
        </div>
      </div>

      <!-- Chart type -->
      <div class="chart-control-group">
        <label class="chart-label">Chart type</label>
        <div class="chart-type-btns">
          <button
            v-for="t in chartTypes"
            :key="t.value"
            class="chart-type-btn"
            :class="{ active: chartType === t.value }"
            @click="chartType = t.value"
          >{{ t.label }}</button>
        </div>
      </div>

      <!-- Max points (performance guard) -->
      <div class="chart-control-group">
        <label class="chart-label">Max points</label>
        <select v-model="maxPoints" class="chart-select">
          <option :value="500">500</option>
          <option :value="1000">1 000</option>
          <option :value="2000">2 000</option>
          <option :value="5000">5 000</option>
          <option :value="Infinity">All</option>
        </select>
      </div>
    </div>

    <!-- ── No columns selected hint ─────────────────────────────────── -->
    <div v-if="selectedCols.length === 0" class="chart-hint">
      ☝️ Select at least one column above to display a chart.
    </div>

    <!-- ── One chart per selected column ────────────────────────────── -->
    <div v-else class="charts-grid">
      <div
        v-for="col in selectedCols"
        :key="col"
        class="chart-card"
      >
        <div class="chart-card-title">{{ col }}</div>
        <div class="chart-wrap">
          <component
            :is="chartComponent"
            :data="chartDataMap[col]"
            :options="chartOptionsMap[col]"
          />
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
/**
 * ChartPanel — receives the raw API result object and renders Chart.js charts.
 * Props:
 *   result  — { columns: string[], rows: any[][], row_count: number, ... }
 */
import { ref, computed, watch } from 'vue'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'
import { Line, Bar } from 'vue-chartjs'

// Register all required Chart.js components once
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
)

// ── Props ──────────────────────────────────────────────────────────────────
const props = defineProps({
  result: {
    type: Object,
    required: true,
  },
})

// ── Local state ────────────────────────────────────────────────────────────
const selectedCols = ref([])
const chartType    = ref('line')
const maxPoints    = ref(1000)

const chartTypes = [
  { value: 'line', label: '📈 Line' },
  { value: 'bar',  label: '📊 Bar'  },
]

// ── Derived ────────────────────────────────────────────────────────────────

/** Index of the timestamp column (first col whose name contains "time" or "date") */
const timeColIdx = computed(() => {
  const cols = props.result.columns
  const idx = cols.findIndex(c =>
    /time|date|timestamp/i.test(c)
  )
  return idx >= 0 ? idx : 0
})

/**
 * Columns that contain at least one finite numeric value.
 * Excludes the timestamp column itself.
 */
const numericColumns = computed(() => {
  const { columns, rows } = props.result
  const tIdx = timeColIdx.value
  return columns.filter((col, ci) => {
    if (ci === tIdx) return false
    // Sample up to 50 rows to decide
    const sample = rows.slice(0, 50)
    return sample.some(row => {
      const v = row[ci]
      return v !== null && v !== undefined && v !== '' && isFinite(Number(v))
    })
  })
})

/** Downsampled rows to keep rendering fast */
const sampledRows = computed(() => {
  const rows = props.result.rows
  if (rows.length <= maxPoints.value) return rows
  const step = Math.ceil(rows.length / maxPoints.value)
  return rows.filter((_, i) => i % step === 0)
})

/** X-axis labels from the timestamp column */
const labels = computed(() => {
  const tIdx = timeColIdx.value
  return sampledRows.value.map(row => {
    const raw = row[tIdx]
    if (!raw) return ''
    // Trim to HH:MM:SS for readability (date is in the page title already)
    const s = String(raw)
    // Handles "2016-07-19 00:10:00" and ISO formats
    const match = s.match(/(\d{2}:\d{2}(:\d{2})?)/)
    return match ? match[1] : s.slice(0, 16)
  })
})

/** The Chart.js component to render (Line or Bar) */
const chartComponent = computed(() => chartType.value === 'bar' ? Bar : Line)

/**
 * Memoised map of col → Chart.js data object.
 * Recomputes only when selectedCols, sampledRows, chartType or labels change.
 */
const chartDataMap = computed(() => {
  const map = {}
  for (const col of selectedCols.value) {
    map[col] = buildChartData(col)
  }
  return map
})

/**
 * Memoised map of col → Chart.js options object.
 * Recomputes only when selectedCols or chartType change.
 */
const chartOptionsMap = computed(() => {
  const map = {}
  for (const col of selectedCols.value) {
    map[col] = buildChartOptions(col)
  }
  return map
})

// ── Chart builders ─────────────────────────────────────────────────────────

// Palette of distinct colours for multiple series
const PALETTE = [
  '#4361ee', '#e63946', '#2ec4b6', '#f4a261', '#8338ec',
  '#06d6a0', '#ef476f', '#ffd166', '#118ab2', '#073b4c',
]

function buildChartData(col) {
  const ci = props.result.columns.indexOf(col)
  const colourIdx = selectedCols.value.indexOf(col)
  const colour = PALETTE[colourIdx % PALETTE.length]

  const data = sampledRows.value.map(row => {
    const v = row[ci]
    if (v === null || v === undefined || v === '') return null
    const n = Number(v)
    return isFinite(n) ? n : null
  })

  return {
    labels: labels.value,
    datasets: [{
      label: col,
      data,
      borderColor: colour,
      backgroundColor: chartType.value === 'bar'
        ? colour + '99'                // semi-transparent fill for bars
        : colour + '22',               // very light fill under line
      borderWidth: chartType.value === 'bar' ? 1 : 1.5,
      pointRadius: chartType.value === 'line' && data.length < 300 ? 2 : 0,
      fill: chartType.value === 'line',
      tension: 0.3,
      spanGaps: true,                  // connect across null values
    }],
  }
}

function buildChartOptions(col) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,                  // instant render — no animation lag
    plugins: {
      legend: { display: false },
      title:  { display: false },
      tooltip: {
        mode: 'index',
        intersect: false,
      },
    },
    scales: {
      x: {
        ticks: {
          maxTicksLimit: 12,
          maxRotation: 0,
          font: { size: 11 },
        },
        grid: { color: '#e4e7ec' },
      },
      y: {
        ticks: { font: { size: 11 } },
        grid: { color: '#e4e7ec' },
        title: {
          display: true,
          text: col,
          font: { size: 11 },
          color: '#888',
        },
      },
    },
  }
}

// ── Auto-select first few numeric columns when result changes ──────────────
watch(
  () => props.result,
  () => {
    // Pre-select up to 3 numeric columns so something is visible immediately
    selectedCols.value = numericColumns.value.slice(0, 3)
  },
  { immediate: true },
)
</script>

<style scoped>
/* ── Layout ───────────────────────────────────────────────────────────── */
.chart-panel {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

/* ── Controls ─────────────────────────────────────────────────────────── */
.chart-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 20px;
  align-items: flex-start;
  padding: 14px 16px;
  background: #fafbfc;
  border: 1px solid #e4e7ec;
  border-radius: 8px;
}

.chart-control-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.chart-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: .05em;
  color: #555;
}

/* Column checkbox list */
.chart-col-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
  max-height: 160px;
  overflow-y: auto;
  min-width: 260px;
}

.chart-checkbox {
  display: flex;
  align-items: center;
  gap: 5px;
  font-size: 13px;
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}
.chart-checkbox input { accent-color: #4361ee; cursor: pointer; }

.chart-empty {
  font-size: 13px;
  color: #888;
  font-style: italic;
}

/* Chart type buttons */
.chart-type-btns {
  display: flex;
  gap: 6px;
}

.chart-type-btn {
  padding: 6px 14px;
  border: 1px solid #d0d5dd;
  border-radius: 6px;
  background: #fff;
  font-size: 13px;
  cursor: pointer;
  color: #555;
  transition: background .12s, color .12s, border-color .12s;
}
.chart-type-btn:hover { background: #f0f2f5; }
.chart-type-btn.active {
  background: #4361ee;
  color: #fff;
  border-color: #4361ee;
}

/* Max points select */
.chart-select {
  padding: 6px 10px;
  border: 1px solid #d0d5dd;
  border-radius: 6px;
  font-size: 13px;
  background: #fff;
  cursor: pointer;
}

/* ── Hint ─────────────────────────────────────────────────────────────── */
.chart-hint {
  text-align: center;
  color: #888;
  font-style: italic;
  padding: 32px 0;
  font-size: 14px;
}

/* ── Charts grid ──────────────────────────────────────────────────────── */
.charts-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(520px, 1fr));
  gap: 16px;
}

.chart-card {
  background: #fff;
  border: 1px solid #e4e7ec;
  border-radius: 8px;
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.chart-card-title {
  font-size: 13px;
  font-weight: 600;
  color: #333;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.chart-wrap {
  /* Fixed height so charts don't collapse */
  height: 220px;
  position: relative;
}
</style>

