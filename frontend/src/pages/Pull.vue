<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { getJSON, patchJSON, postJSON } from '../api'

const streams = ref([])
const sources = ref([])
const streamId = ref(null)
const state = ref(null)
const tasks = ref([])
const detail = ref(null)
const err = ref('')
const busy = ref(false)
const bindSourceId = ref(null)
const bindPosition = ref(0)
const newSource = ref({ name: '', manifest_path: '', note: '' })

const lastTaskFailed = computed(() => tasks.value.length && tasks.value[0].status === 'failed')

watch(bindSourceId, (val) => {
  // 换源默认位点归零（与后端一致），避免带旧源位点越界
  if (state.value && val !== state.value.stream.pull_source_id) bindPosition.value = 0
})

async function loadStreams() {
  streams.value = await getJSON('/api/streams')
}
async function loadSources() {
  sources.value = await getJSON('/api/pull/sources')
}
async function loadState() {
  if (!streamId.value) return
  state.value = await getJSON(`/api/streams/${streamId.value}/pull`)
  bindSourceId.value = state.value.stream.pull_source_id
  bindPosition.value = state.value.position
}
async function loadTasks() {
  if (!streamId.value) return
  tasks.value = await getJSON(`/api/pull/tasks?stream_id=${streamId.value}`)
}
async function showTask(id) {
  detail.value = await getJSON(`/api/pull/tasks/${id}`)
}
async function refreshAll() {
  await loadStreams()
  await loadState()
  await loadTasks()
  if (tasks.value.length) await showTask(tasks.value[0].id)
  else detail.value = null
}
async function saveBind() {
  err.value = ''
  try {
    await patchJSON(`/api/streams/${streamId.value}/pull`, {
      source_id: bindSourceId.value,
      position: Number(bindPosition.value),
    })
    await refreshAll()
  } catch (e) {
    err.value = String(e.message || e)
  }
}
async function doPull(mode) {
  busy.value = true
  err.value = ''
  try {
    detail.value = await postJSON(`/api/streams/${streamId.value}/pull`, { mode })
    await loadStreams()
    await loadState()
    await loadTasks()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
async function registerSource() {
  err.value = ''
  try {
    await postJSON('/api/pull/sources', {
      name: newSource.value.name,
      manifest_path: newSource.value.manifest_path,
      note: newSource.value.note || null,
    })
    newSource.value = { name: '', manifest_path: '', note: '' }
    await loadSources()
  } catch (e) {
    err.value = String(e.message || e)
  }
}
async function onStreamChange() {
  err.value = ''
  detail.value = null
  await loadState()
  await loadTasks()
  if (tasks.value.length) await showTask(tasks.value[0].id)
}
onMounted(async () => {
  try {
    await loadStreams()
    await loadSources()
    const paged = streams.value.find((s) => s.pull_source_id)
    streamId.value = (paged || streams.value[0] || {}).id || null
    await onStreamChange()
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>

<template>
  <div class="page">
    <h1>分页拉取</h1>
    <p class="muted">
      按位点从本机分页源逐页拉取：整页先入账本，再走 Apply 裁决；页全部成功才推进位点，失败页挡住位点、可续拉。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel">
      <h3>流 · 源 · 位点</h3>
      <div class="row-actions">
        <label>流
          <select v-model.number="streamId" @change="onStreamChange">
            <option v-for="s in streams" :key="s.id" :value="s.id">
              {{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}）
            </option>
          </select>
        </label>
        <label>分页源
          <select v-model.number="bindSourceId">
            <option :value="null">— 未绑定 —</option>
            <option v-for="src in sources" :key="src.id" :value="src.id">
              {{ src.id }} · {{ src.name }}（{{ src.page_count }} 页）
            </option>
          </select>
        </label>
        <label>位点
          <input type="number" min="0" v-model.number="bindPosition" style="width: 6rem" />
        </label>
        <button class="ghost" @click="saveBind">保存源与位点</button>
      </div>
      <p v-if="state" class="muted">
        当前位点 {{ state.position }}
        <span v-if="state.next_page">→ 下一页 <code>{{ state.next_page.page_token }}</code>（{{ state.next_page.file }}）</span>
        <span v-else-if="state.exhausted" class="tag ok">已拉完</span>
        <span v-if="state.error" class="tag danger">{{ state.error }}</span>
      </p>
    </div>

    <div class="panel" v-if="state && state.pages.length">
      <h3>源页清单（{{ state.source?.manifest_path }}）</h3>
      <table>
        <thead>
          <tr><th>#</th><th>page_token</th><th>文件</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="p in state.pages" :key="p.index">
            <td>{{ p.index }}</td>
            <td><code>{{ p.page_token }}</code></td>
            <td><code>{{ p.file }}</code></td>
            <td>
              <span class="tag" :class="p.status === 'pulled' ? 'ok' : p.status === 'next' ? 'warn' : ''">
                {{ p.status === 'pulled' ? '已拉取' : p.status === 'next' ? '下一页' : '待拉' }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel row-actions">
      <button :disabled="busy || !state || !state.source || state.exhausted" @click="doPull('one')">
        {{ busy ? '拉取中…' : '拉取一页' }}
      </button>
      <button :disabled="busy || !state || !state.source || state.exhausted" @click="doPull('all')">
        {{ lastTaskFailed ? '续拉（从失败页连续拉完）' : '连续拉完' }}
      </button>
      <span v-if="state && !state.source" class="tag warn">未绑定分页源：请先选择源并保存</span>
      <span v-if="lastTaskFailed" class="tag danger">上次任务失败：位点停在失败页，可续拉</span>
    </div>

    <div class="panel" v-if="detail">
      <h3>
        任务报告 #{{ detail.id }}
        <span class="tag" :class="detail.status === 'succeeded' ? 'ok' : 'danger'">{{ detail.status }}</span>
      </h3>
      <div class="tiles">
        <div class="tile"><div class="hero-num">{{ detail.report.pages_pulled }}</div><div class="muted">拉取页数</div></div>
        <div class="tile"><div class="hero-num">{{ detail.report.events_loaded }}</div><div class="muted">事件条数</div></div>
        <div class="tile"><div class="hero-num">{{ detail.report.ledger_inserted }}</div><div class="muted">入账条数</div></div>
        <div class="tile"><div class="hero-num">{{ detail.applied_count }}</div><div class="muted">applied</div></div>
        <div class="tile"><div class="hero-num">{{ detail.quarantined_count }}</div><div class="muted">quarantined</div></div>
        <div class="tile"><div class="hero-num">{{ detail.skipped_count }}</div><div class="muted">skipped</div></div>
      </div>
      <p>
        位点 {{ detail.report.position_before }} → {{ detail.report.position_after }}
        · 游标 {{ detail.cursor_before }} → {{ detail.cursor_after }}
        <span v-if="detail.report.failed_page" class="tag danger">
          失败页 {{ detail.report.failed_page.page_token }}：{{ detail.report.failed_page.error }}
        </span>
        <span v-if="detail.report.exhausted" class="tag ok">源已拉完</span>
      </p>
      <h4>页清单</h4>
      <table>
        <thead>
          <tr><th>#</th><th>page_token</th><th>状态</th><th>事件</th><th>入账</th><th>applied</th><th>quarantined</th><th>skipped</th><th>错误</th></tr>
        </thead>
        <tbody>
          <tr v-for="p in detail.pages" :key="p.id">
            <td>{{ p.page_index }}</td>
            <td><code>{{ p.page_token }}</code></td>
            <td><span class="tag" :class="p.status === 'pulled' ? 'ok' : 'danger'">{{ p.status }}</span></td>
            <td>{{ p.events_count }}</td>
            <td>{{ p.ledger_inserted }}</td>
            <td>{{ p.applied_count }}</td>
            <td>{{ p.quarantined_count }}</td>
            <td>{{ p.skipped_count }}</td>
            <td class="muted">{{ p.error || '' }}</td>
          </tr>
        </tbody>
      </table>
      <h4>对账（与页清单 / 账本 / 隔离区 / Sink / 位点）</h4>
      <table>
        <thead>
          <tr><th>校验</th><th>结果</th></tr>
        </thead>
        <tbody>
          <tr v-for="(ok, name) in detail.checks" :key="name">
            <td>{{ name }}</td>
            <td><span class="tag" :class="ok ? 'ok' : 'danger'">{{ ok ? '✓' : '✗' }}</span></td>
          </tr>
        </tbody>
      </table>
      <p class="muted mono">
        账本 {{ detail.reconcile.ledger_rows }} 行（applied {{ detail.reconcile.ledger_applied }} /
        quarantined {{ detail.reconcile.ledger_quarantined }} / skipped {{ detail.reconcile.ledger_skipped }} /
        未裁决 {{ detail.reconcile.ledger_pending_pulled }}） · 隔离区本任务 {{ detail.reconcile.quarantine_rows }} 行 ·
        applied_seqs {{ detail.reconcile.applied_seq_rows }} 行 · Sink 共 {{ detail.reconcile.sink_rows }} 行 ·
        流位点 {{ detail.reconcile.stream_pull_position }} · 流游标 {{ detail.reconcile.stream_apply_cursor }}
      </p>
    </div>

    <div class="panel">
      <h3>拉取任务历史</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>状态</th><th>页数</th><th>事件</th><th>入账</th><th>applied</th><th>quar.</th><th>skipped</th><th>位点</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="t in tasks" :key="t.id" style="cursor: pointer" @click="showTask(t.id)">
            <td>{{ t.id }}</td>
            <td><span class="tag" :class="t.status === 'succeeded' ? 'ok' : 'danger'">{{ t.status }}</span></td>
            <td>{{ t.report.pages_pulled }}</td>
            <td>{{ t.report.events_loaded }}</td>
            <td>{{ t.report.ledger_inserted }}</td>
            <td>{{ t.applied_count }}</td>
            <td>{{ t.quarantined_count }}</td>
            <td>{{ t.skipped_count }}</td>
            <td>{{ t.report.position_before }}→{{ t.report.position_after }}</td>
            <td>{{ t.created_at }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>登记分页源</h3>
      <div class="row-actions">
        <label>名称 <input v-model="newSource.name" placeholder="如 Orders 分页源" /></label>
        <label>清单路径 <input v-model="newSource.manifest_path" placeholder="orders_pages/manifest.json" /></label>
        <label>备注 <input v-model="newSource.note" placeholder="可选" /></label>
        <button class="ghost" :disabled="!newSource.name || !newSource.manifest_path" @click="registerSource">登记</button>
      </div>
      <table v-if="sources.length">
        <thead>
          <tr><th>ID</th><th>名称</th><th>清单</th><th>页数</th><th>备注</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="src in sources" :key="src.id">
            <td>{{ src.id }}</td>
            <td>{{ src.name }}</td>
            <td><code>{{ src.manifest_path }}</code></td>
            <td>{{ src.page_count }}</td>
            <td class="muted">{{ src.note }}</td>
            <td><span v-if="src.error" class="tag danger">{{ src.error }}</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
