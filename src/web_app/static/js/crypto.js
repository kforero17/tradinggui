(() => {
    const el = id => document.getElementById(id);
    let quotes = [], selected = null, loading = false, chartRequest = 0;
    let lastChart = 0;
    const number = value => typeof value === 'number' && Number.isFinite(value);
    const money = value => number(value) ? value.toLocaleString('en-US', {
        style: 'currency', currency: 'USD', maximumFractionDigits: value !== 0 && Math.abs(value) < 1 ? 10 : 2
    }) : '—';
    const date = value => value ? new Date(value.endsWith('Z') || /[+-]\d\d:\d\d$/.test(value) ? value : value + 'Z') : null;
    function render() {
        const search = el('cryptoSearch').value.trim().toUpperCase();
        const min = Math.max(0, Number(el('minVolume').value) || 0);
        const sort = el('cryptoSort').value;
        const rows = quotes.filter(q => q.instrument.includes(search) && (q.volume_24h || 0) >= min);
        rows.sort((a, b) => sort === 'instrument' ? a.instrument.localeCompare(b.instrument) :
            sort === 'losers' ? (a.price_change_24h || 0) - (b.price_change_24h || 0) : (b[sort] || 0) - (a[sort] || 0));
        el('cryptoCount').textContent = `${rows.length} of ${quotes.length} markets · Select a symbol to chart it`;
        const body = el('cryptoRows');
        body.replaceChildren();
        for (const q of rows) {
            const tr = document.createElement('tr');
            if (q.instrument === selected) tr.className = 'crypto-selected';
            const td = document.createElement('td'), button = document.createElement('button');
            button.textContent = q.instrument; button.className = 'crypto-select';
            button.onclick = () => { selected = q.instrument; render(); loadChart(); };
            td.append(button); tr.append(td);
            const received = date(q.updated_at);
            const stale = !received || Date.now() - received.getTime() > 180000;
            [money(q.last_price), number(q.price_change_24h) ? q.price_change_24h.toFixed(2) + '%' : '—',
                money(q.volume_24h), money(q.high_24h), money(q.low_24h),
                (received ? received.toLocaleString() : 'Unknown') + (stale ? ' · Stale' : '')].forEach((text, i) => {
                const cell = document.createElement('td'); cell.textContent = text;
                if (i === 1 && number(q.price_change_24h)) cell.className = q.price_change_24h > 0 ? 'positive' : q.price_change_24h < 0 ? 'negative' : '';
                tr.append(cell);
            });
            body.append(tr);
        }
        if (!rows.length) {
            const tr = body.insertRow(), cell = tr.insertCell(); cell.colSpan = 7;
            cell.textContent = quotes.length ? 'No markets match your filters.' : 'Waiting for market data. Automatic retries are enabled.';
        }
        if (!selected && rows.length) { selected = rows[0].instrument; loadChart(); }
    }
    async function loadChart() {
        if (!selected) return;
        const request = ++chartRequest, symbol = selected, timeframe = el('chartTimeframe').value;
        el('chartTitle').textContent = symbol + ' · ' + (timeframe === '1D' ? 'Daily' : 'Hourly');
        el('chartStatus').textContent = 'Loading chart…';
        if (window.Plotly) Plotly.purge('cryptoChart');
        try {
            const response = await fetch(`/get_crypto_plot/${encodeURIComponent(symbol)}?timeframe=${timeframe}`, {signal: AbortSignal.timeout(25000)});
            const data = await response.json();
            if (request !== chartRequest) return;
            if (!response.ok || !data.success || !data.plot_data) throw new Error(data.error || 'Chart unavailable');
            if (!window.Plotly) throw new Error('Chart library could not load. Reload the page to retry.');
            const plot = JSON.parse(data.plot_data);
            await Plotly.newPlot('cryptoChart', plot.data, plot.layout, {responsive: true, displaylogo: false});
            el('chartStatus').textContent = (data.stale ? 'Cached chart — provider unavailable. ' : '') +
                'Latest candle: ' + new Date(data.last_candle).toLocaleString() + ' · Times in UTC · MA/EMA use 50 candles';
            lastChart = Date.now();
        } catch (error) {
            if (request === chartRequest) el('chartStatus').textContent = 'Could not load chart: ' + error.message;
        }
    }
    async function refresh() {
        if (loading || document.hidden) return;
        loading = true; el('refreshQuotes').disabled = true;
        try {
            const response = await fetch('/api/crypto/quotes', {cache: 'no-store', signal: AbortSignal.timeout(20000)});
            if (!response.ok) throw new Error('Quote request failed');
            const data = await response.json(); quotes = data.instruments;
            el('feedStatus').textContent = data.error || (data.last_success ? 'Feed updated ' + new Date(data.last_success).toLocaleString() : 'Fetching live quotes; saved data shown below.');
            render();
        } catch (error) {
            el('feedStatus').textContent = 'Unable to refresh. Showing last received quotes; retrying automatically.';
            render();
        } finally { loading = false; el('refreshQuotes').disabled = false; }
    }
    ['cryptoSearch', 'minVolume', 'cryptoSort'].forEach(id => el(id).addEventListener('input', render));
    el('chartTimeframe').onchange = loadChart;
    el('refreshQuotes').onclick = () => { refresh(); loadChart(); };
    document.addEventListener('visibilitychange', () => { if (!document.hidden) { refresh(); if (Date.now() - lastChart >= 300000) loadChart(); } });
    setInterval(refresh, 15000);
    setInterval(() => { if (!document.hidden) loadChart(); }, 300000);
    refresh();
})();
