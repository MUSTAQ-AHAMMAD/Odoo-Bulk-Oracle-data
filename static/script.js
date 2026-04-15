const steps = Array.from(document.querySelectorAll(".step-panel"));
const stepLabels = Array.from(document.querySelectorAll("#stepLabels .step"));
const progressBar = document.getElementById("progressBar");
const transactionDateInput = document.getElementById("transactionDate");
const accountingDateInput = document.getElementById("accountingDate");
const fileInputs = {
  line_items: document.getElementById("lineItems"),
  payments: document.getElementById("payments"),
  metadata: document.getElementById("metadata"),
  registers: document.getElementById("registers"),
};
const statusEls = {
  line_items: document.getElementById("lineItemsStatus"),
  payments: document.getElementById("paymentsStatus"),
  metadata: document.getElementById("metadataStatus"),
  registers: document.getElementById("registersStatus"),
};
const startProcessingBtn = document.getElementById("startProcessing");
const processingBar = document.getElementById("processingBar");
const processingStatus = document.getElementById("processingStatus");
const toResultsBtn = document.getElementById("toResults");
const downloadButtons = {
  ar: document.getElementById("downloadAR"),
  standard: document.getElementById("downloadStandard"),
  miss: document.getElementById("downloadMiss"),
  all: document.getElementById("downloadAll"),
  report: document.getElementById("downloadReport"),
};
const summaryCards = document.getElementById("summaryCards");
const standardTable = document.getElementById("standardTable").querySelector("tbody");
const missTable = document.getElementById("missTable").querySelector("tbody");
const roundingTable = document.getElementById("roundingTable").querySelector("tbody");
const verificationBadge = document.getElementById("verificationBadge");
const chargesTableBody = document.querySelector("#chargesTable tbody");
const loadDefaultsBtn = document.getElementById("loadDefaults");
const saveConfigBtn = document.getElementById("saveConfig");
const taxRateInput = document.getElementById("taxRate");
const startSequenceInput = document.getElementById("startSequence");
const legacy1Input = document.getElementById("legacy1");
const legacy2Input = document.getElementById("legacy2");

let currentStep = 0;
let validationState = {
  line_items: false,
  payments: false,
  metadata: false,
  registers: false,
};
let chargesConfig = [];
let activeJobId = null;
let pollTimer = null;

function setTodayDefaults() {
  const today = new Date().toISOString().split("T")[0];
  transactionDateInput.value = today;
  accountingDateInput.value = today;
}

function showStep(index) {
  currentStep = index;
  steps.forEach((panel, idx) => {
    panel.classList.toggle("active", idx === index);
  });
  stepLabels.forEach((label, idx) => {
    label.classList.toggle("active", idx === index);
  });
  progressBar.style.width = `${((index + 1) / steps.length) * 100}%`;
}

function nextStep() {
  if (currentStep < steps.length - 1) showStep(currentStep + 1);
}

function prevStep() {
  if (currentStep > 0) showStep(currentStep - 1);
}

function setStatus(el, ok, missing = []) {
  el.className = "status " + (ok ? "ok" : "error");
  el.textContent = ok ? "✔ Valid" : `✖ Missing: ${missing.join(", ")}`;
}

async function validateFile(type, file) {
  const el = statusEls[type];
  setStatus(el, false, ["validating..."]);
  const form = new FormData();
  form.append("fileType", type);
  form.append("file", file);
  const res = await fetch("/api/validate-file", { method: "POST", body: form });
  const data = await res.json();
  validationState[type] = data.valid;
  setStatus(el, data.valid, data.missing || []);
  document.getElementById("toStep3").disabled = !Object.values(validationState).every(Boolean);
}

function wireFileInputs() {
  Object.entries(fileInputs).forEach(([type, input]) => {
    input.addEventListener("change", (e) => {
      const file = e.target.files[0];
      if (file) validateFile(type, file);
    });
  });
}

async function loadDefaults() {
  const res = await fetch("/api/config/defaults");
  const data = await res.json();
  taxRateInput.value = data.tax_rate ?? 5.0;
  chargesConfig = data.methods || [];
  renderChargesTable();
}

function renderChargesTable() {
  chargesTableBody.innerHTML = "";
  chargesConfig.forEach((row, idx) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.method}</td>
      <td><input type="number" step="0.1" data-idx="${idx}" data-field="bank_charge_pct" value="${row.bank_charge_pct}"></td>
      <td><input type="number" step="0.01" data-idx="${idx}" data-field="cap" value="${row.cap}"></td>
      <td><input type="checkbox" data-idx="${idx}" data-field="apply_cap" ${row.apply_cap ? "checked" : ""}></td>
      <td><input type="checkbox" data-idx="${idx}" data-field="generate_miss" ${row.generate_miss ? "checked" : ""}></td>
    `;
    chargesTableBody.appendChild(tr);
  });

  chargesTableBody.querySelectorAll("input").forEach((input) => {
    input.addEventListener("input", (e) => {
      const idx = Number(e.target.dataset.idx);
      const field = e.target.dataset.field;
      const val = e.target.type === "checkbox" ? e.target.checked : Number(e.target.value);
      chargesConfig[idx][field] = val;
    });
  });
}

async function saveConfig() {
  await fetch("/api/config/save", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tax_rate: Number(taxRateInput.value), methods: chargesConfig }),
  });
}

async function startProcessing() {
  if (!Object.values(validationState).every(Boolean)) {
    alert("Please validate all files first.");
    return;
  }
  startProcessingBtn.disabled = true;
  processingStatus.textContent = "Starting...";
  const form = new FormData();
  form.append("transaction_date", transactionDateInput.value);
  form.append("accounting_date", accountingDateInput.value);
  form.append("start_sequence", startSequenceInput.value);
  form.append("legacy_segment1", legacy1Input.value);
  form.append("legacy_segment2", legacy2Input.value);
  form.append("tax_rate", taxRateInput.value);
  form.append("charges", JSON.stringify(chargesConfig));
  Object.entries(fileInputs).forEach(([key, input]) => {
    form.append(key, input.files[0]);
  });

  const res = await fetch("/api/process", { method: "POST", body: form });
  const data = await res.json();
  if (data.error) {
    processingStatus.textContent = data.error;
    startProcessingBtn.disabled = false;
    return;
  }
  activeJobId = data.job_id;
  processingStatus.textContent = `Job ${activeJobId} started`;
  pollStatus();
}

async function pollStatus() {
  if (!activeJobId) return;
  const res = await fetch(`/api/status/${activeJobId}`);
  if (!res.ok) {
    processingStatus.textContent = "Status unavailable";
    return;
  }
  const data = await res.json();
  processingBar.style.width = `${data.progress || 0}%`;
  processingStatus.textContent = data.message || "";
  if (data.status === "completed") {
    toResultsBtn.disabled = false;
    enableDownloads();
    renderResults(data.result);
    clearTimeout(pollTimer);
  } else if (data.status === "failed") {
    processingStatus.textContent = `Failed: ${data.message}`;
    clearTimeout(pollTimer);
  } else {
    pollTimer = setTimeout(pollStatus, 1500);
  }
}

function enableDownloads() {
  Object.values(downloadButtons).forEach((btn) => (btn.disabled = false));
}

function renderSummaryCard(label, value, tone = "") {
  const div = document.createElement("div");
  div.className = "upload";
  div.innerHTML = `<p class="eyebrow">${label}</p><h3>${value}</h3>`;
  summaryCards.appendChild(div);
}

function renderResults(result) {
  summaryCards.innerHTML = "";
  const s = result.summary;
  renderSummaryCard("Total Sales Amount", `${s.total_sales_amount.toLocaleString()} SAR`);
  renderSummaryCard("Total AR Invoice Amount", `${s.total_ar_amount.toLocaleString()} SAR`);
  renderSummaryCard("Standard Receipts", `${s.total_standard_receipts.toLocaleString()} SAR`);
  renderSummaryCard("Miss Receipts (abs)", `${s.total_miss_receipts.toLocaleString()} SAR`);
  renderSummaryCard("Net Settlement", `${s.net_settlement.toLocaleString()} SAR`);

  verificationBadge.textContent = s.verification_passed ? "Balanced" : "Mismatch";
  verificationBadge.className = "pill " + (s.verification_passed ? "success" : "danger");

  standardTable.innerHTML = "";
  Object.entries(result.standard_breakdown || {}).forEach(([method, row]) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${method}</td><td>${row.amount.toFixed(2)}</td><td>${row.files}</td><td>${row.transactions}</td>`;
    standardTable.appendChild(tr);
  });

  missTable.innerHTML = "";
  Object.entries(result.miss_breakdown || {}).forEach(([method, row]) => {
    const config = chargesConfig.find((c) => c.method.toUpperCase() === method) || {};
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${method}</td><td>${(config.bank_charge_pct ?? 0).toFixed(1)}%</td><td>${row.amount.toFixed(3)}</td><td>${row.cap_count || 0}</td><td>${row.example || ""}</td>`;
    missTable.appendChild(tr);
  });

  roundingTable.innerHTML = "";
  (result.rounding_breakdown || []).forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${row.store}</td><td>${row.date}</td><td>${row.rounding_amount.toFixed(3)}</td><td>${row.miss_amount.toFixed(3)}</td><td>Rounding amount ${row.rounding_amount.toFixed(3)} → ${row.miss_amount.toFixed(3)}</td>`;
    roundingTable.appendChild(tr);
  });
}

function wireNav() {
  document.querySelectorAll(".next").forEach((btn) => btn.addEventListener("click", nextStep));
  document.querySelectorAll(".prev").forEach((btn) => btn.addEventListener("click", prevStep));
}

function wireDownloads() {
  downloadButtons.ar.addEventListener("click", () => window.location = `/api/download/${activeJobId}/ar`);
  downloadButtons.standard.addEventListener("click", () => window.location = `/api/download/${activeJobId}/standard`);
  downloadButtons.miss.addEventListener("click", () => window.location = `/api/download/${activeJobId}/miss`);
  downloadButtons.all.addEventListener("click", () => window.location = `/api/download/${activeJobId}/all`);
  downloadButtons.report.addEventListener("click", () => window.location = `/api/download/${activeJobId}/verification`);
}

document.getElementById("toStep3").disabled = true;
startProcessingBtn.addEventListener("click", startProcessing);
loadDefaultsBtn.addEventListener("click", loadDefaults);
saveConfigBtn.addEventListener("click", saveConfig);
wireNav();
wireFileInputs();
wireDownloads();
setTodayDefaults();
loadDefaults();
showStep(0);
