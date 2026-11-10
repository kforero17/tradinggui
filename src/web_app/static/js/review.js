(() => {
    const el = id => document.getElementById(id);
    const number = v => typeof v === 'number' && Number.isFinite(v);
    const money = v => number(v) ? v.toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 0}) : '—';
    const price = v => number(v) ? v.toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 2}) : '—';
    const pct = v => number(v) ? (v * 100).toFixed(0) + '%' : '—';
    const fixed = (v, digits = 2) => number(v) ? v.toFixed(digits) : '—';
    const layout = extra => Object.assign({
        template: 'plotly_dark', paper_bgcolor: '#1e1e1e', plot_bgcolor: '#1e1e1e', height: 380,
        margin: {l: 60, r: 20, t: 10, b: 60}, legend: {orientation: 'h'},
    }, extra);

    function cell(row, text, className) {
        const td = row.insertCell();
        td.textContent = text;
        if (className) td.className = className;
        return td;
    }

    function listItems(id, items) {
        el(id).replaceChildren(...items.map(text => {
            const li = document.createElement('li');
            li.textContent = text;
            return li;
        }));
    }

    function groupTable(id, groups) {
        const body = el(id);
        body.replaceChildren();
        for (const g of groups) {
            const row = body.insertRow();
            cell(row, g.group); cell(row, g.trades); cell(row, pct(g.win_rate)); cell(row, money(g.pnl), colorClass(g.pnl));
        }
    }

    function renderSummary(s) {
        el('summaryCards').innerHTML = [
            buildMetricCard('Closed trades', s.trades),
            buildMetricCard('Net P&L', money(s.total_pnl), colorClass(s.total_pnl)),
            buildMetricCard('Win rate', pct(s.win_rate)),
            buildMetricCard('Average win', money(s.avg_win), 'positive'),
            buildMetricCard('Average loss', money(s.avg_loss), 'negative'),
            buildMetricCard('Payoff ratio', fixed(s.payoff_ratio)),
            buildMetricCard('Profit factor', fixed(s.profit_factor)),
            buildMetricCard('Expectancy / trade', money(s.expectancy), colorClass(s.expectancy)),
        ].join('');
        el('disciplineCards').innerHTML = [
            buildMetricCard('Losers past the 2N stop', `${s.losers_past_2n_stop} of ${s.losses}`, s.losers_past_2n_stop ? 'negative' : ''),
            buildMetricCard('Lost beyond the 2N stop', money(s.loss_beyond_2n_stop), 'negative'),
            buildMetricCard('Median worst drawdown in losers', pct(s.median_mae_losers), 'negative'),
            buildMetricCard('Median hold: winners / losers', `${fixed(s.median_hold_days_winners, 0)}d / ${fixed(s.median_hold_days_losers, 0)}d`),
            buildMetricCard('Winners: share of best gain kept', pct(s.median_mfe_capture_winners)),
            buildMetricCard('Top 3 winners: share of gross wins', pct(s.top3_share_of_gross_win)),
            buildMetricCard('Rule replay P&L vs actual', `${money(s.turtle_replay_rule_pnl)} vs ${money(s.turtle_replay_actual_pnl)}`),
            buildMetricCard('Position size spread (CV)', fixed(s.position_value_cv)),
        ].join('');
        groupTable('byEntry', s.by_entry_type);
        groupTable('byAccount', s.by_account);
        groupTable('byYear', s.by_year);
        listItems('reentries', s.reentries.map(r =>
            `${r.symbol}: sold ${r.sold} at ${price(r.sold_at)}, re-bought ${r.rebought} at ${price(r.rebought_at)} (${r.days_apart} days later)`));
        listItems('unmatched', s.unmatched_sells.map(u => `${u.symbol} · ${u.account} · ${u.date} · ${fixed(u.quantity, 3)} sh`));
    }

    function renderTrades(trades) {
        const body = el('tradeRows');
        body.replaceChildren();
        for (const t of [...trades].sort((a, b) => a.pnl - b.pnl)) {
            const row = body.insertRow();
            cell(row, t.symbol); cell(row, t.account); cell(row, t.entry_date); cell(row, t.exit_date); cell(row, t.holding_days);
            cell(row, price(t.entry_price)); cell(row, price(t.exit_price));
            cell(row, money(t.pnl), colorClass(t.pnl)); cell(row, pct(t.return_pct), colorClass(t.return_pct));
            cell(row, fixed(t.r_multiple), number(t.r_multiple) && t.r_multiple < -1 ? 'negative' : '');
            cell(row, pct(t.mae_pct), 'negative'); cell(row, pct(t.mfe_pct), 'positive');
            cell(row, t.entry_type);
            cell(row, t.turtle_exit_date ? `${t.turtle_exit_date} (${t.turtle_exit_reason})` : (t.turtle_still_open ? 'still open' : '—'));
            cell(row, money(t.turtle_pnl), colorClass(t.turtle_pnl));
            cell(row, money(t.turtle_minus_actual), colorClass(t.turtle_minus_actual));
        }
    }

    function renderCharts(trades) {
        const sorted = [...trades].filter(t => number(t.turtle_pnl)).sort((a, b) => a.pnl - b.pnl);
        const labels = sorted.map(t => `${t.symbol} ${t.exit_date.slice(0, 7)}`);
        Plotly.newPlot('pnlChart', [
            {type: 'bar', x: labels, y: sorted.map(t => t.pnl), name: 'Actual', marker: {color: '#007bff'}},
            {type: 'bar', x: labels, y: sorted.map(t => t.turtle_pnl), name: 'Turtle rules', marker: {color: '#00D4AA'}},
        ], layout({barmode: 'group', yaxis: {title: 'P&L ($)'}}), {responsive: true, displaylogo: false});
        const rs = trades.map(t => t.r_multiple).filter(number);
        Plotly.newPlot('rChart', [{
            type: 'histogram', x: rs.map(r => Math.max(-6, Math.min(r, 10))), xbins: {size: 0.5},
            marker: {color: rs.map(r => (r < -1 ? '#dc3545' : '#00D4AA'))},
        }], layout({
            xaxis: {title: 'R-multiple (clipped to −6…10). Under −1 means the loss went past a 2N stop'},
            yaxis: {title: 'Trades'},
            shapes: [{type: 'line', x0: -1, x1: -1, yref: 'paper', y0: 0, y1: 1, line: {color: '#dc3545', dash: 'dash'}}],
        }), {responsive: true, displaylogo: false});
    }

    async function loadAccounts() {
        try {
            const response = await fetch('/api/review/accounts');
            const data = await response.json();
            el('knownAccounts').replaceChildren(...data.accounts.map(account => {
                const option = document.createElement('option');
                option.value = account;
                return option;
            }));
        } catch (error) { /* suggestions are optional */ }
    }

    async function load() {
        loadAccounts();
        el('reviewStatus').textContent = 'Reviewing trades (the first run of the day fetches price history)…';
        try {
            const response = await fetch('/api/review/summary?system=' + el('system').value);
            const data = await response.json();
            if (!response.ok || !data.success) throw new Error(data.error || response.statusText);
            if (data.empty) {
                el('reviewStatus').textContent = 'No fills imported yet. Choose your CSV exports above and click Import.';
                return;
            }
            const s = data.summary;
            el('reviewStatus').textContent = `${data.fills} fills · ${s.trades} closed round trips · ${s.first_trade} to ${s.last_trade}`;
            renderSummary(s);
            renderTrades(data.trades);
            renderCharts(data.trades);
            listItems('openPositions', data.open_positions.map(o =>
                `${o.symbol} · ${o.account} · ${fixed(o.quantity, 3)} sh at ${price(o.entry_price)} since ${o.entry_date}`));
        } catch (error) {
            el('reviewStatus').textContent = 'Could not build the review: ' + error.message;
        }
    }

    async function importFiles() {
        const files = el('csvFiles').files;
        if (!files.length) return;
        const form = new FormData();
        for (const file of files) form.append('files', file);
        form.append('account', el('importAccount').value.trim());
        el('reviewStatus').textContent = 'Importing…';
        const response = await fetch('/api/review/import', {method: 'POST', body: form});
        const data = await response.json();
        const notes = data.files.map(f => f.error ? `${f.file}: ${f.error}` : `${f.file}: ${f.new} new of ${f.fills}`);
        el('reviewStatus').textContent = notes.join(' · ');
        el('csvFiles').value = '';
        load();
    }

    async function clearFills() {
        if (!confirm('Remove all imported fills? You can re-import the CSVs afterwards.')) return;
        await fetch('/api/review/fills', {method: 'DELETE'});
        load();
    }

    el('importButton').onclick = importFiles;
    el('clearButton').onclick = clearFills;
    el('system').onchange = load;
    load();
})();
