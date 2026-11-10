/* ── Shared utility functions ──────────────────────────────────── */

function formatValue(value, suffix) {
    suffix = suffix || '';
    if (value === null || value === undefined) return 'N/A';
    if (typeof value === 'number') return value.toFixed(2) + suffix;
    return value + suffix;
}

function formatCurrency(value) {
    if (value === null || value === undefined) return 'N/A';
    return '$' + value.toFixed(2);
}

function formatMarketCap(value) {
    if (value === null || value === undefined) return 'N/A';
    if (value >= 1e12) return '$' + (value / 1e12).toFixed(2) + 'T';
    if (value >= 1e9) return '$' + (value / 1e9).toFixed(2) + 'B';
    if (value >= 1e6) return '$' + (value / 1e6).toFixed(2) + 'M';
    return '$' + value.toLocaleString();
}

function formatVolume(value) {
    if (value === null || value === undefined) return 'N/A';
    return '$' + value.toLocaleString();
}

function colorClass(value) {
    if (value === null || value === undefined) return '';
    return value > 0 ? 'positive' : 'negative';
}

/* ── Loading helpers ──────────────────────────────────────────── */

function showLoading(elementId) {
    var el = document.getElementById(elementId);
    if (el) el.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
}

function hideLoading(elementId) {
    var el = document.getElementById(elementId);
    if (el) {
        var spinner = el.querySelector('.loading');
        if (spinner) spinner.remove();
    }
}

/* ── Modal helpers ────────────────────────────────────────────── */

function openModal(modalId) {
    var m = document.getElementById(modalId);
    if (m) m.style.display = 'block';
}

function closeModal(modalId) {
    var m = document.getElementById(modalId);
    if (m) m.style.display = 'none';
}

// Close any modal when clicking outside content
window.addEventListener('click', function(event) {
    if (event.target.classList.contains('modal')) {
        event.target.style.display = 'none';
    }
});

/* ── RSI / MACD formatting for metrics display ────────────────── */

function rsiColorClass(value) {
    if (value === null || value === undefined) return '';
    if (value >= 70) return 'negative';  // overbought
    if (value <= 30) return 'positive';  // oversold
    return '';
}

function buildMetricCard(label, value, cssClass) {
    cssClass = cssClass || '';
    return '<div class="metric-card">' +
        '<div class="metric-label">' + label + '</div>' +
        '<div class="metric-value ' + cssClass + '">' + value + '</div>' +
        '</div>';
}
