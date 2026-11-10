(() => {
    const el = id => document.getElementById(id);
    const SETTINGS_KEY = 'trend-settings';
    const RULE_LABELS = {
        EXIT_STOP: 'EXIT · stop hit',
        EXIT_CHANNEL: 'EXIT · channel',
        ADD_UNIT: 'Add unit',
        HOLD: 'Hold',
    };
    const number = v => typeof v === 'number' && Number.isFinite(v);
    const money = v => number(v) ? v.toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 2}) : '—';
    const fixed = (v, digits = 2) => number(v) ? v.toFixed(digits) : '—';
    const pct = v => number(v) ? (v * 100).toFixed(1) + '%' : '—';

    function loadSettings() {
        try {
            const saved = JSON.parse(localStorage.getItem(SETTINGS_KEY) || '{}');
            for (const [id, value] of Object.entries(saved)) {
                const input = el(id);
                if (!input) continue;
                if (input.type === 'checkbox') input.checked = value; else input.value = value;
            }
        } catch (error) { /* storage unavailable: keep the defaults */ }
    }

    function saveSettings() {
        const values = {};
        for (const id of ['equity', 'system', 'riskPct', 'unitCapPct', 'stopN', 'exitDays', 'trendFilter']) {
            values[id] = el(id).type === 'checkbox' ? el(id).checked : el(id).value;
        }
        try { localStorage.setItem(SETTINGS_KEY, JSON.stringify(values)); } catch (error) { /* ignore */ }
    }

    function query() {
        return new URLSearchParams({
            equity: el('equity').value,
            system: el('system').value,
            risk_pct: el('riskPct').value,
            unit_cap_pct: el('unitCapPct').value,
            stop_n: el('stopN').value,
            exit_days: el('exitDays').value,
            trend_filter: el('trendFilter').checked,
        }).toString();
    }

    function cell(row, text, className) {
        const td = row.insertCell();
        td.textContent = text;
        if (className) td.className = className;
        return td;
    }

    function tickerButton(row, ticker, account) {
        const button = document.createElement('button');
        button.className = 'trend-ticker';
        button.textContent = ticker;
        button.onclick = () => loadChart(ticker, account);
        row.insertCell().append(button);
    }

    async function getJson(url, options) {
        const response = await fetch(url, options);
        const data = await response.json();
        if (!response.ok || data.success === false) throw new Error(data.error || response.statusText);
        return data;
    }

    async function loadPositions() {
        el('positionsStatus').textContent = 'Loading positions…';
        const body = el('positionRows');
        body.replaceChildren();
        try {
            const data = await getJson('/api/trend/positions?' + query());
            for (const p of data.positions) {
                const row = body.insertRow();
                tickerButton(row, p.ticker, p.account);
                cell(row, p.account + (p.direction === 'SHORT' ? ' · SHORT' : ''),
                    p.direction === 'SHORT' ? 'negative' : '');
                const rule = row.insertCell();
                const badge = document.createElement('span');
                badge.className = 'rule-badge rule-' + p.action;
                badge.textContent = RULE_LABELS[p.action];
                rule.append(badge);
                if (p.rule_fired_on) {
                    const late = document.createElement('span');
                    late.className = 'rule-late';
                    late.textContent = `${p.rule_fired_reason} fired ${p.rule_fired_on} at ${money(p.rule_fired_price)}`;
                    rule.append(late);
                }
                cell(row, money(p.last_close));
                cell(row, `${money(p.entry_price)} · ${p.entry_date}`);
                cell(row, fixed(p.n_now));
                cell(row, money(p.stop_price));
                cell(row, money(p.exit_level));
                cell(row, fixed(p.distance_to_stop_n, 1), p.distance_to_stop_n < 1 ? 'negative' : '');
                cell(row, fixed(p.open_r_multiple), colorClass(p.open_r_multiple));
                cell(row, money(p.unrealized_pnl), colorClass(p.unrealized_pnl));
                cell(row, `${money(p.open_risk_dollars)} (${pct(p.open_risk_pct_equity)})`);
                cell(row, fixed(p.units_held, 1));
            }
            if (!data.positions.length) {
                const row = body.insertRow();
                cell(row, 'No positions in the Portfolio tab.').colSpan = 13;
            }
            const exits = data.positions.filter(p => p.action.startsWith('EXIT')).length;
            const skipped = data.skipped.map(s => `${s.ticker} in ${s.account} (${s.reason})`).join(', ');
            el('positionsStatus').textContent =
                `${data.config.system} · ${data.config.entry_period}-day entry / ${data.config.exit_period}-day exit / ` +
                `${data.config.stop_n}N stop · ${exits} position${exits === 1 ? '' : 's'} to exit · ` +
                `total open risk to stops ${money(data.total_open_risk)}` + (skipped ? ` · Skipped: ${skipped}` : '');
            const breaches = el('unitBreaches');
            breaches.replaceChildren(...data.unit_limit_breaches.map(text => {
                const li = document.createElement('li');
                li.textContent = 'Unit limit: ' + text;
                return li;
            }));
            if (data.positions.length && !el('trendChart').hasChildNodes()) {
                loadChart(data.positions[0].ticker, data.positions[0].account);
            }
        } catch (error) {
            el('positionsStatus').textContent = 'Could not load positions: ' + error.message;
        }
    }

    async function loadChart(ticker, account) {
        el('chartTitle').textContent = account ? `${ticker} · ${account}` : ticker;
        el('chartStatus').textContent = 'Loading chart…';
        try {
            const params = query() + (account ? '&account=' + encodeURIComponent(account) : '');
            const data = await getJson(`/api/trend/chart/${encodeURIComponent(ticker)}?` + params);
            const plot = JSON.parse(data.plot_data);
            await Plotly.newPlot('trendChart', plot.data, plot.layout, {responsive: true, displaylogo: false});
            el('chartStatus').textContent = 'Last 12 months of daily bars.';
        } catch (error) {
            el('chartStatus').textContent = 'Could not load chart: ' + error.message;
        }
    }

    async function loadScan() {
        el('scanStatus').textContent = 'Scanning watchlist (the first load of the day fetches price history)…';
        const body = el('scanRows');
        body.replaceChildren();
        try {
            const data = await getJson('/api/trend/scan?' + query());
            for (const s of data.signals) {
                const row = body.insertRow();
                tickerButton(row, s.ticker);
                const signal = s.triggered ? 'BREAKOUT' : s.distance_n <= 1 ? 'Within 1N' : 'Waiting';
                cell(row, signal + (s.skipped_after_winner ? ' (55-day failsafe)' : ''),
                    s.triggered ? 'positive' : '');
                cell(row, money(s.last_close));
                cell(row, money(s.entry_level));
                cell(row, fixed(s.distance_n, 1));
                cell(row, fixed(s.n));
                cell(row, s.unit_shares);
                cell(row, money(s.unit_shares * s.entry_level));
                cell(row, money(s.stop_if_filled));
                cell(row, s.trend_ok ? 'Up' : 'Down', s.trend_ok ? 'positive' : 'negative');
                const remove = document.createElement('button');
                remove.className = 'btn-sm btn-danger';
                remove.textContent = 'Remove';
                remove.onclick = async () => {
                    await getJson(`/api/trend/watchlist/${encodeURIComponent(s.ticker)}`, {method: 'DELETE'});
                    loadScan();
                };
                row.insertCell().append(remove);
            }
            const triggered = data.signals.filter(s => s.triggered).length;
            el('scanStatus').textContent = `${data.signals.length} tickers · ${triggered} breaking out today` +
                (data.missing.length ? ` · No data: ${data.missing.join(', ')}` : '');
        } catch (error) {
            el('scanStatus').textContent = 'Could not scan watchlist: ' + error.message;
        }
    }

    async function addToWatchlist() {
        const tickers = el('watchlistAdd').value.trim();
        if (!tickers) return;
        await getJson('/api/trend/watchlist', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({tickers}),
        });
        el('watchlistAdd').value = '';
        loadScan();
    }

    async function runBacktest() {
        el('runBacktest').disabled = true;
        el('backtestStatus').textContent = 'Running backtest…';
        try {
            const data = await getJson('/api/trend/backtest?' + query());
            const s = data.stats;
            el('backtestStats').innerHTML = [
                buildMetricCard('CAGR', pct(s.cagr), colorClass(s.cagr)),
                buildMetricCard('Max drawdown', pct(s.max_drawdown), 'negative'),
                buildMetricCard('MAR (CAGR / max DD)', fixed(s.mar)),
                buildMetricCard('R² of log equity', fixed(s.r_squared)),
                buildMetricCard('Trades', s.trades),
                buildMetricCard('Win rate', pct(s.win_rate)),
                buildMetricCard('Profit factor', fixed(s.profit_factor)),
                buildMetricCard('Average R', fixed(s.avg_r), colorClass(s.avg_r)),
            ].join('');
            await Plotly.newPlot('backtestChart', [
                {x: data.equity.dates, y: data.equity.values, name: 'Turtle rules', line: {color: '#00D4AA'}},
                {x: data.benchmark.dates, y: data.benchmark.values, name: 'SPY (price only)', line: {color: '#aaa', dash: 'dot'}},
            ], {
                template: 'plotly_dark', paper_bgcolor: '#1e1e1e', plot_bgcolor: '#1e1e1e', height: 420,
                yaxis: {type: 'log', title: 'Equity ($, log scale)'}, legend: {orientation: 'h'},
                margin: {l: 60, r: 20, t: 20, b: 40},
            }, {responsive: true, displaylogo: false});
            const body = el('backtestTrades');
            body.replaceChildren();
            for (const t of data.trades) {
                const row = body.insertRow();
                cell(row, t.ticker); cell(row, t.entry_date); cell(row, t.exit_date); cell(row, t.units);
                cell(row, money(t.avg_entry)); cell(row, money(t.pnl), colorClass(t.pnl));
                cell(row, fixed(t.r_multiple), colorClass(t.r_multiple)); cell(row, t.reason);
            }
            const yearly = data.yearly.map(y => `${y.year}: ${pct(y.return)}`).join(' · ');
            el('backtestStatus').textContent = `${data.tickers} tickers · most recent 100 trades shown · ${yearly}`;
        } catch (error) {
            el('backtestStatus').textContent = 'Backtest failed: ' + error.message;
        } finally {
            el('runBacktest').disabled = false;
        }
    }

    function refresh() {
        saveSettings();
        loadPositions();
        loadScan();
    }

    loadSettings();
    el('refreshTrend').onclick = refresh;
    el('watchlistAddButton').onclick = addToWatchlist;
    el('runBacktest').onclick = runBacktest;
    refresh();
})();
