(() => {
    const el = id => document.getElementById(id);
    const CONFIG = window.TODAY_CONFIG || {staleDays: 8, defaultAccount: 'default'};
    const FILTERS_KEY = 'today-filters';
    const GROUPS = [
        {key: 'exit', title: 'Exits', actions: ['EXIT_STOP', 'EXIT_CHANNEL'], cls: 'today-group-exit', open: true,
            empty: 'No exits: every stop and exit channel holds.'},
        {key: 'add', title: 'Adds', actions: ['ADD_UNIT'], cls: 'today-group-add', open: true,
            empty: 'No position has reached its next add level.'},
        {key: 'enter', title: 'New entries', actions: ['ENTER'], cls: 'today-group-enter', open: true,
            empty: 'No breakouts in the latest entry scan.'},
        {key: 'watch', title: 'Watch', actions: ['WATCH'], cls: 'today-group-watch', open: true,
            empty: 'Nothing within reach of a breakout.'},
        {key: 'hold', title: 'Holdings OK', actions: ['HOLD'], cls: '', open: false,
            empty: 'No holdings to check.'},
    ];
    const ACTION_LABELS = {
        EXIT_STOP: 'EXIT · stop', EXIT_CHANNEL: 'EXIT · channel', ADD_UNIT: 'Add unit',
        ENTER: 'Enter', WATCH: 'Watch', HOLD: 'Hold',
    };
    const DECISION_LABELS = {taken: 'Taken', skipped: 'Skipped', pending: 'Pending'};
    const HEADERS = ['Ticker', 'Account', 'Side', 'Action', 'Signal', 'Order ticket', 'Score · RS', 'Why', 'Journal'];
    const ENTRY_ACTIONS = new Set(['ENTER', 'WATCH']);

    const number = v => typeof v === 'number' && Number.isFinite(v);
    const money = v => number(v) ? v.toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 0}) : '—';
    const price = v => number(v) ? v.toFixed(2) : '—';
    const pct = v => number(v) ? (v * 100).toFixed(1) + '%' : '—';

    const state = {dashboard: null, accounts: [], totalEquity: 0};
    const filters = {direction: 'ALL', asset: 'ALL', account: 'ALL'};
    const groupOpen = Object.fromEntries(GROUPS.map(g => [g.key, g.open]));
    // Once the heat budget fills most entries are blocked; they start collapsed.
    let blockedEntriesOpen = false;
    const isBlocked = r => Boolean(r.blocked_by && r.blocked_by.length);

    /* ── Helpers ──────────────────────────────────────────────── */

    async function getJson(url, options) {
        const response = await fetch(url, options);
        let data;
        try {
            data = await response.json();
        } catch (error) {
            throw new Error(response.statusText || 'Unexpected response');
        }
        if (!response.ok || data.success === false) throw new Error(data.error || response.statusText);
        return data;
    }

    function postJson(url, body) {
        return getJson(url, {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body || {}),
        });
    }

    // Stored times are naive UTC; without an offset JS would read them as local time.
    function parseUtc(iso) {
        if (!iso) return null;
        return new Date(/([zZ]|[+-]\d\d:\d\d)$/.test(iso) ? iso : iso + 'Z');
    }

    function ageText(date) {
        const hours = (Date.now() - date.getTime()) / 3.6e6;
        if (hours < 1) return 'just now';
        if (hours < 48) return `${Math.round(hours)}h ago`;
        return `${Math.round(hours / 24)}d ago`;
    }

    function node(tag, className, text) {
        const n = document.createElement(tag);
        if (className) n.className = className;
        if (text !== undefined && text !== null) n.textContent = text;
        return n;
    }

    function button(text, className, onClick, title) {
        const b = node('button', className, text);
        b.type = 'button';
        if (title) b.title = title;
        b.onclick = onClick;
        return b;
    }

    function cell(row, text, className) {
        const td = row.insertCell();
        if (text !== undefined) td.textContent = text;
        if (className) td.className = className;
        return td;
    }

    // buildMetricCard builds the markup from static labels; values go in with textContent.
    function metricCard(label, value, cssClass) {
        const wrap = document.createElement('div');
        wrap.innerHTML = buildMetricCard(label, '', cssClass);
        const card = wrap.firstElementChild;
        card.querySelector('.metric-label').textContent = label;
        card.querySelector('.metric-value').textContent = value;
        return card;
    }

    async function copyText(text) {
        try {
            if (navigator.clipboard && window.isSecureContext) {
                await navigator.clipboard.writeText(text);
                return true;
            }
        } catch (error) { /* fall back to execCommand */ }
        const area = document.createElement('textarea');
        area.value = text;
        area.setAttribute('readonly', '');
        area.style.position = 'fixed';
        area.style.opacity = '0';
        document.body.append(area);
        area.select();
        let ok = false;
        try { ok = document.execCommand('copy'); } catch (error) { ok = false; }
        area.remove();
        return ok;
    }

    /* ── Filters ──────────────────────────────────────────────── */

    function loadFilters() {
        try {
            Object.assign(filters, JSON.parse(localStorage.getItem(FILTERS_KEY) || '{}'));
        } catch (error) { /* storage unavailable: keep the defaults */ }
    }

    function saveFilters() {
        try { localStorage.setItem(FILTERS_KEY, JSON.stringify(filters)); } catch (error) { /* ignore */ }
    }

    function bindToggle(groupId, key) {
        const group = el(groupId);
        const sync = () => {
            for (const b of group.querySelectorAll('button')) b.classList.toggle('active', b.dataset.value === filters[key]);
        };
        if (![...group.querySelectorAll('button')].some(b => b.dataset.value === filters[key])) filters[key] = 'ALL';
        for (const b of group.querySelectorAll('button')) {
            b.onclick = () => {
                filters[key] = b.dataset.value;
                sync();
                saveFilters();
                render();
            };
        }
        sync();
    }

    function accountOptions() {
        const names = new Set(state.accounts.map(a => a.name));
        const rows = state.dashboard ? [...state.dashboard.recommendations, ...state.dashboard.expired] : [];
        for (const r of rows) if (r.account) names.add(r.account);
        const select = el('filterAccount');
        select.replaceChildren(new Option('All accounts', 'ALL'), ...[...names].sort().map(n => new Option(n, n)));
        if (!names.has(filters.account)) filters.account = 'ALL';
        select.value = filters.account;
    }

    function matches(r) {
        if (filters.direction !== 'ALL' && r.direction !== filters.direction) return false;
        // Rows without an asset type are treated as stocks; ETFs are always flagged.
        const asset = r.asset_type === 'etf' ? 'etf' : 'stock';
        if (filters.asset !== 'ALL' && asset !== filters.asset) return false;
        if (filters.account !== 'ALL' && r.account !== filters.account) return false;
        return true;
    }

    /* ── Order tickets ────────────────────────────────────────── */

    function ticketText(r) {
        const x = r.extra || {};
        const type = (x.order_type || '').toUpperCase();
        const shares = number(r.shares) ? r.shares : '?';
        if (r.action === 'EXIT_STOP' || r.action === 'EXIT_CHANNEL') {
            return `${type} ${shares} ${r.ticker} at market (rule level ${price(r.trigger_price)}` +
                (x.rule_fired_on ? `, fired ${x.rule_fired_on})` : ')');
        }
        if (r.action === 'HOLD') {
            return `Stop ${price(r.stop_price)} · exit ${price(x.exit_level)} · next add ${price(x.next_add_level)}`;
        }
        let text = `${type} ${shares} ${r.ticker} @ ${price(r.trigger_price)}`;
        if (number(x.limit_hint)) text += ` (limit ${price(x.limit_hint)})`;
        text += `, stop ${price(r.stop_price)}, risk ${money(r.risk_dollars)}`;
        if (number(x.r_to_extreme)) {
            text += `, ${x.r_to_extreme.toFixed(1)}R to 52w ${r.direction === 'SHORT' ? 'low' : 'high'}`;
        }
        if (x.check_borrow) text += ', check borrow';
        return text;
    }

    /* ── Journal ──────────────────────────────────────────────── */

    async function decide(r, status, note) {
        try {
            const data = await postJson('/api/recommendations/decision', {
                signal_key: r.signal_key, first_seen_at: r.first_seen_at, status, note,
            });
            r.decision = data.decision;
            render();
        } catch (error) {
            el('todayStatus').textContent = `Could not save the decision for ${r.ticker}: ${error.message}`;
        }
    }

    function journalCell(row, r) {
        const td = row.insertCell();
        const current = r.decision ? r.decision.status : null;
        if (current) td.append(node('span', 'badge badge-' + current, DECISION_LABELS[current] || current));
        if (r.decision && r.decision.note) td.append(node('span', 'note-text', r.decision.note));
        const actions = node('div', 'row-actions');
        // Clicking the active status again puts the signal back to pending.
        const toggle = status => () => decide(r, current === status ? 'pending' : status, null);
        actions.append(
            button('Taken', 'btn-xs ' + (current === 'taken' ? 'btn-success' : 'btn-muted'), toggle('taken'),
                current === 'taken' ? 'Mark pending again' : 'I placed this order'),
            button('Skip', 'btn-xs ' + (current === 'skipped' ? 'btn-danger' : 'btn-muted'), toggle('skipped'),
                current === 'skipped' ? 'Mark pending again' : 'I am not taking this signal'),
            button('Note', 'btn-xs btn-muted', () => {
                const note = window.prompt(`Note for ${r.ticker}`, (r.decision && r.decision.note) || '');
                if (note !== null) decide(r, current || 'pending', note);
            }),
        );
        td.append(actions);
    }

    /* ── Tables ───────────────────────────────────────────────── */

    function buildRow(body, r, expired) {
        const row = body.insertRow();
        if (isBlocked(r)) row.className = 'today-blocked';
        const x = r.extra || {};

        const tickerCell = row.insertCell();
        const position = !ENTRY_ACTIONS.has(r.action);
        const ticker = button(r.ticker, 'trend-ticker', () => openChart(r.ticker, position ? r.account : null),
            'Show the chart with its channels and stop');
        tickerCell.append(ticker);
        if (number(x.last_close)) tickerCell.append(node('span', 'reasons', `${price(x.last_close)} · ${x.last_date || ''}`));

        cell(row, r.account || '—');
        const side = row.insertCell();
        side.append(node('span', 'badge ' + (r.direction === 'SHORT' ? 'badge-short' : 'badge-long'), r.direction));

        const action = row.insertCell();
        action.append(node('span', 'rule-badge rule-' + r.action, ACTION_LABELS[r.action] || r.action));

        const signal = row.insertCell();
        if (expired) signal.append(node('span', 'badge', 'EXPIRED'));
        else if (r.status === 'NEW') signal.append(node('span', 'badge badge-new', 'NEW'));
        else signal.append(node('span', 'badge', 'STILL VALID'));
        if (r.is_late) signal.append(node('span', 'badge badge-late', 'LATE'));
        const seen = parseUtc(r.first_seen_at);
        if (seen) signal.append(node('span', 'reasons', 'since ' + seen.toLocaleDateString()));

        const ticketCell = row.insertCell();
        const text = ticketText(r);
        ticketCell.append(node('span', 'ticket', text));
        if (!expired && x.order_type) {
            const copy = button('Copy', 'btn-xs btn-muted', async () => {
                const ok = await copyText(text);
                copy.textContent = ok ? 'Copied' : 'Copy failed';
                setTimeout(() => { copy.textContent = 'Copy'; }, 1500);
            }, 'Copy the order ticket');
            const actions = node('div', 'row-actions');
            actions.append(copy);
            ticketCell.append(actions);
        }

        const scoreParts = [];
        if (number(r.trend_score)) scoreParts.push(r.trend_score.toFixed(2));
        if (number(r.rs_rank)) scoreParts.push('RS ' + Math.round(r.rs_rank * 100));
        cell(row, scoreParts.join(' · ') || '—');

        const why = row.insertCell();
        why.className = 'today-why';
        if (r.reasons && r.reasons.length) why.append(node('span', 'reasons', r.reasons.join(' · ')));
        for (const b of r.blocked_by || []) why.append(node('span', 'blocked', 'Blocked: ' + b));

        journalCell(row, r);
    }

    function buildTable(rows, expired) {
        const wrap = node('div', 'data-table');
        const table = document.createElement('table');
        const head = table.createTHead().insertRow();
        for (const h of HEADERS) head.append(node('th', null, h));
        const body = table.createTBody();
        for (const r of rows) buildRow(body, r, expired);
        wrap.append(table);
        return wrap;
    }

    function renderGroups(rows) {
        const container = el('actionGroups');
        container.replaceChildren();
        if (!state.dashboard || !state.dashboard.run) {
            const empty = node('div', 'card today-group');
            empty.append(node('p', 'text-muted', 'No recommendation run is stored yet. Run the weekly job, or press ' +
                '"Recheck holdings", which re-evaluates holdings and this week\'s watch list on fresh bars.'));
            container.append(empty);
            return;
        }
        for (const g of GROUPS) {
            const groupRows = rows.filter(r => g.actions.includes(r.action));
            const collapseBlocked = g.key === 'enter';
            const shown = collapseBlocked ? groupRows.filter(r => !isBlocked(r)) : groupRows;
            const blocked = collapseBlocked ? groupRows.filter(isBlocked) : [];
            const details = node('details', `card today-group ${g.cls}`);
            details.open = groupOpen[g.key];
            details.addEventListener('toggle', () => { groupOpen[g.key] = details.open; });
            const summary = node('summary', null, g.title + ' ');
            const counts = blocked.length ? `(${shown.length} · ${blocked.length} blocked)` : `(${shown.length})`;
            summary.append(node('span', 'text-muted', counts));
            details.append(summary);
            if (shown.length) details.append(buildTable(shown, false));
            else details.append(node('p', 'text-muted', blocked.length ? 'Every breakout is blocked.' : g.empty));
            if (blocked.length) details.append(blockedEntries(blocked));
            container.append(details);
        }
    }

    function blockedEntries(rows) {
        const details = node('details', 'today-blocked-entries');
        details.open = blockedEntriesOpen;
        details.addEventListener('toggle', () => { blockedEntriesOpen = details.open; });
        details.append(node('summary', null, `Blocked entries (${rows.length})`));
        details.append(buildTable(rows, false));
        return details;
    }

    function renderExpired() {
        const rows = state.dashboard ? state.dashboard.expired.filter(matches) : [];
        el('expiredCount').textContent = `(${rows.length})`;
        const container = el('expiredTable');
        container.replaceChildren(rows.length ? buildTable(rows, true) : node('p', 'text-muted', 'No expired signals.'));
    }

    /* ── Summary cards ────────────────────────────────────────── */

    function equityCard() {
        const card = metricCard('Total equity', money(state.totalEquity));
        const sub = node('div', 'metric-sub', `${state.accounts.length} account${state.accounts.length === 1 ? '' : 's'} · `);
        sub.append(button('Edit accounts', 'link-button', openAccounts));
        card.append(sub);
        return card;
    }

    function heatCard() {
        const h = state.dashboard && state.dashboard.heat;
        if (!h) {
            const card = metricCard('Open risk vs heat budget', '—');
            card.append(node('div', 'metric-sub', 'No run yet'));
            return card;
        }
        const over = h.remaining_dollars < 0;
        const card = metricCard('Open risk vs heat budget', `${money(h.open_risk_dollars)} (${pct(h.open_risk_pct)})`,
            over ? 'negative' : '');
        const budget = h.budget_dollars > 0 ? h.budget_dollars : null;
        const bar = node('div', 'heat-bar' + (over ? ' over' : ''));
        const open = node('span', 'heat-open');
        const planned = node('span', 'heat-planned');
        const openShare = budget ? Math.min(h.open_risk_dollars / budget, 1) : 0;
        open.style.width = (openShare * 100).toFixed(1) + '%';
        planned.style.width = (budget ? Math.min(h.planned_risk_dollars / budget, 1 - openShare) * 100 : 0).toFixed(1) + '%';
        bar.title = 'Yellow: open risk to stops · Blue: planned adds and entries';
        bar.append(open, planned);
        card.append(bar);
        card.append(node('div', 'metric-sub' + (over ? ' negative' : ''),
            `Budget ${pct(h.budget_pct)} = ${money(h.budget_dollars)} · planned ${money(h.planned_risk_dollars)} · ` +
            `remaining ${money(h.remaining_dollars)}`));
        return card;
    }

    function countCards(rows) {
        const count = actions => rows.filter(r => actions.includes(r.action)).length;
        const exits = count(['EXIT_STOP', 'EXIT_CHANNEL']);
        const cards = [
            metricCard('Exits', String(exits), exits ? 'negative' : ''),
            metricCard('Adds', String(count(['ADD_UNIT'])), count(['ADD_UNIT']) ? 'positive' : ''),
            metricCard('New entries', String(rows.filter(r => r.action === 'ENTER' && !isBlocked(r)).length)),
            metricCard('Watch', String(count(['WATCH']))),
        ];
        const hidden = state.dashboard && state.dashboard.summary && state.dashboard.summary.watch_hidden;
        if (number(hidden) && hidden > 0) cards[3].append(node('div', 'metric-sub', `${hidden} more not shown`));
        const blocked = rows.filter(r => r.action === 'ENTER' && isBlocked(r)).length;
        if (blocked) cards[2].append(node('div', 'metric-sub', `${blocked} more blocked`));
        return cards;
    }

    function runCard() {
        const d = state.dashboard;
        const run = d && d.run;
        const weekly = (d && d.weekly) || {stale: true};
        const when = run ? parseUtc(run.run_at) : null;
        const card = metricCard('Last run', run ? `${run.job} · ${ageText(when)}` : 'None', weekly.stale ? 'negative' : '');
        if (when) card.append(node('div', 'metric-sub', when.toLocaleString()));
        const entries = d && d.entries_run;
        if (entries && (!run || entries.id !== run.id)) {
            const at = parseUtc(entries.run_at);
            card.append(node('div', 'metric-sub', `Entries from the ${entries.job} run ${at.toLocaleDateString()}`));
        }
        if (weekly.stale) {
            const stale = node('div', 'metric-sub negative', 'STALE · run ');
            stale.append(node('code', null, 'python -m src.jobs weekly'));
            card.append(stale);
        }
        return card;
    }

    function regimeCard() {
        const regime = state.dashboard && state.dashboard.run && state.dashboard.run.regime;
        if (!regime || !regime.label) {
            const card = metricCard('Market regime', '—');
            card.append(node('div', 'metric-sub', 'No run yet'));
            return card;
        }
        const cls = {risk_on: 'positive', risk_off: 'negative'}[regime.label] || '';
        const card = metricCard('Market regime', regime.label.replace('_', ' ').toUpperCase(), cls);
        const position = regime.benchmark_above_200d === null || regime.benchmark_above_200d === undefined
            ? 'vs' : regime.benchmark_above_200d ? 'above' : 'below';
        card.append(node('div', 'metric-sub',
            `${regime.benchmark || 'SPY'} ${price(regime.benchmark_close)} ${position} 200d ${price(regime.benchmark_sma200)}`));
        card.append(node('div', 'metric-sub',
            number(regime.breadth_above_200d) ? `Breadth: ${pct(regime.breadth_above_200d)} above 200d` : 'Breadth: n/a'));
        return card;
    }

    function renderSummary(rows) {
        el('summaryCards').replaceChildren(equityCard(), heatCard(), ...countCards(rows), runCard(), regimeCard());
        const weekly = state.dashboard && state.dashboard.weekly;
        const warning = el('staleWarning');
        warning.hidden = !(weekly && weekly.stale);
        if (!warning.hidden) {
            const age = number(weekly.weekly_age_days) ? Math.floor(weekly.weekly_age_days) : null;
            warning.replaceChildren(
                node('strong', null, 'STALE: '),
                document.createTextNode(age === null
                    ? 'no weekly run has been stored yet. '
                    : `the last weekly run is ${age} days old (more than ${weekly.stale_days || CONFIG.staleDays}). `),
                document.createTextNode('Run '),
                node('code', null, 'python -m src.jobs weekly'),
                document.createTextNode(' to refresh entries and metrics.'),
            );
        }
    }

    function renderStaleBars() {
        const stale = (state.dashboard && state.dashboard.stale_holdings) || [];
        const banner = el('staleBarsBanner');
        banner.hidden = !stale.length;
        if (!stale.length) return;
        const list = node('ul');
        for (const s of stale) {
            const item = node('li');
            item.append(node('strong', null, s.ticker), document.createTextNode(` · last bar ${s.last_bar}`));
            list.append(item);
        }
        banner.replaceChildren(
            node('strong', null, 'STALE PRICES: '),
            document.createTextNode(`price data stale for ${stale.length} holding${stale.length === 1 ? '' : 's'} – ` +
                'stop checks may be late. The last download failed, so these were checked on old bars. ' +
                'Press "Recheck holdings" to try again.'),
            list,
        );
    }

    function renderSkippedPositions() {
        const skipped = (state.dashboard && state.dashboard.skipped_positions) || [];
        const card = el('skippedPositions');
        card.hidden = !skipped.length;
        if (!skipped.length) return;
        const list = node('ul');
        for (const s of skipped) {
            const item = node('li', null, 'Not evaluated: ');
            item.append(node('strong', null, s.ticker), document.createTextNode(` (${s.account}) – ${s.reason}`));
            list.append(item);
        }
        const hint = node('div', null, 'No stop, exit or add is checked for these holdings. Set the cost basis (and shares) on the ');
        const link = node('a', null, 'Portfolio page');
        link.href = '/portfolio';
        hint.append(link, document.createTextNode('.'));
        card.replaceChildren(
            node('strong', null, `${skipped.length} holding${skipped.length === 1 ? '' : 's'} not evaluated`),
            list,
            hint,
        );
    }

    function render() {
        if (!state.dashboard) return;
        const rows = state.dashboard.recommendations.filter(matches);
        renderStaleBars();
        renderSkippedPositions();
        renderSummary(rows);
        renderGroups(rows);
        renderExpired();
        const run = state.dashboard.run;
        const total = state.dashboard.recommendations.length;
        el('todayStatus').textContent = run
            ? `Run #${run.id} (${run.job}) · ${total} signal${total === 1 ? '' : 's'}` +
              (rows.length !== total ? ` · ${rows.length} after filters` : '')
            : 'No recommendation run yet.';
    }

    /* ── Chart modal ──────────────────────────────────────────── */

    function chartQuery(account) {
        const config = (state.dashboard && state.dashboard.run && state.dashboard.run.config) || {};
        const params = new URLSearchParams();
        if (config.system) params.set('system', config.system);
        if (number(config.risk_per_unit)) params.set('risk_pct', config.risk_per_unit * 100);
        if (number(config.max_unit_fraction)) params.set('unit_cap_pct', config.max_unit_fraction * 100);
        if (number(config.stop_n)) params.set('stop_n', config.stop_n);
        if (number(config.exit_period)) params.set('exit_days', config.exit_period);
        params.set('trend_filter', Boolean(config.use_trend_filter));
        if (state.totalEquity > 0) params.set('equity', state.totalEquity);
        if (account) params.set('account', account);
        return params.toString();
    }

    async function openChart(ticker, account) {
        el('chartModalTitle').textContent = account ? `${ticker} · ${account}` : ticker;
        el('chartModalStatus').textContent = 'Loading chart…';
        el('todayChart').replaceChildren();
        openModal('chartModal');
        try {
            const data = await getJson(`/api/trend/chart/${encodeURIComponent(ticker)}?` + chartQuery(account));
            const plot = JSON.parse(data.plot_data);
            await Plotly.newPlot('todayChart', plot.data, plot.layout, {responsive: true, displaylogo: false});
            el('chartModalStatus').textContent = 'Last 12 months of daily bars with the entry and exit channels.';
        } catch (error) {
            el('chartModalStatus').textContent = 'Could not load chart: ' + error.message;
        }
    }

    /* ── Accounts ─────────────────────────────────────────────── */

    function numberInput(value, label) {
        const input = document.createElement('input');
        input.type = 'number';
        input.step = '0.01';
        input.value = number(value) ? value : 0;
        input.setAttribute('aria-label', label);
        return input;
    }

    function checkbox(checked, label) {
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = Boolean(checked);
        input.setAttribute('aria-label', label);
        return input;
    }

    function accountsStatus(text, isError) {
        const status = el('accountsStatus');
        status.textContent = text;
        status.className = isError ? 'negative' : 'text-muted';
    }

    function applyAccounts(data) {
        state.accounts = data.accounts;
        state.totalEquity = data.total_equity;
        renderAccounts();
        accountOptions();
        render();
    }

    async function saveAccount(name, equity, cash, allowShort) {
        try {
            const data = await postJson('/api/accounts', {
                name, equity: equity.value, cash: cash.value, allow_short: allowShort.checked,
            });
            applyAccounts(data);
            accountsStatus(`Saved ${name}.`);
        } catch (error) {
            accountsStatus(`Could not save ${name}: ${error.message}`, true);
        }
    }

    async function deleteAccount(name) {
        if (!window.confirm(`Delete the account "${name}"?`)) return;
        try {
            applyAccounts(await getJson(`/api/accounts/${encodeURIComponent(name)}`, {method: 'DELETE'}));
            accountsStatus(`Deleted ${name}.`);
        } catch (error) {
            accountsStatus(`Could not delete ${name}: ${error.message}`, true);
        }
    }

    async function renameAccount(name) {
        const newName = window.prompt(`Rename the account "${name}" to`, name);
        if (newName === null || !newName.trim() || newName.trim() === name) return;
        try {
            applyAccounts(await postJson(`/api/accounts/${encodeURIComponent(name)}/rename`, {new_name: newName.trim()}));
            accountsStatus(`Renamed ${name} to ${newName.trim()}. Stored runs keep the old name until the next run.`);
        } catch (error) {
            accountsStatus(`Could not rename ${name}: ${error.message}`, true);
        }
    }

    function renderAccounts() {
        const body = el('accountRows');
        body.replaceChildren();
        for (const a of state.accounts) {
            const row = body.insertRow();
            const nameCell = cell(row, a.name);
            if (a.name === CONFIG.defaultAccount) nameCell.append(node('span', 'reasons', 'default for new positions'));
            const equity = numberInput(a.equity, `${a.name} equity`);
            const cash = numberInput(a.cash, `${a.name} cash`);
            const allowShort = checkbox(a.allow_short, `${a.name} allows shorts`);
            row.insertCell().append(equity);
            row.insertCell().append(cash);
            row.insertCell().append(allowShort);
            const updated = parseUtc(a.updated_at);
            cell(row, updated ? updated.toLocaleDateString() : '—');
            const actions = node('div', 'row-actions');
            actions.append(
                button('Save', 'btn-xs btn-success', () => saveAccount(a.name, equity, cash, allowShort)),
                button('Rename', 'btn-xs btn-muted', () => renameAccount(a.name)),
                button('Delete', 'btn-xs btn-danger', () => deleteAccount(a.name)),
            );
            row.insertCell().append(actions);
        }
        const row = body.insertRow();
        const name = document.createElement('input');
        name.type = 'text';
        name.placeholder = 'New account (e.g. Roth)';
        name.setAttribute('aria-label', 'New account name');
        const equity = numberInput(0, 'New account equity');
        const cash = numberInput(0, 'New account cash');
        const allowShort = checkbox(false, 'New account allows shorts');
        row.insertCell().append(name);
        row.insertCell().append(equity);
        row.insertCell().append(cash);
        row.insertCell().append(allowShort);
        cell(row, '');
        row.insertCell().append(button('Add account', 'btn-xs', () => {
            if (!name.value.trim()) {
                accountsStatus('Enter a name for the new account.', true);
                return;
            }
            saveAccount(name.value.trim(), equity, cash, allowShort);
        }));
    }

    function openAccounts() {
        accountsStatus('');
        renderAccounts();
        openModal('accountsModal');
    }

    /* ── Loading ──────────────────────────────────────────────── */

    async function loadAccounts() {
        try {
            const data = await getJson('/api/accounts');
            state.accounts = data.accounts;
            state.totalEquity = data.total_equity;
        } catch (error) {
            accountsStatus('Could not load accounts: ' + error.message, true);
        }
    }

    async function loadDashboard() {
        try {
            state.dashboard = await getJson('/api/recommendations/latest');
        } catch (error) {
            el('todayStatus').textContent = 'Could not load recommendations: ' + error.message;
            el('actionGroups').replaceChildren();
        }
    }

    async function loadAll() {
        showLoading('actionGroups');
        await Promise.all([loadAccounts(), loadDashboard()]);
        accountOptions();
        render();
    }

    async function recheck() {
        const b = el('recheckHoldings');
        b.disabled = true;
        el('todayStatus').textContent = 'Rechecking: re-evaluates holdings and this week\'s watch list on fresh bars…';
        try {
            const data = await postJson('/api/recommendations/recheck');
            await loadDashboard();
            render();
            el('todayStatus').textContent += ` · rechecked as run #${data.run_id}`;
        } catch (error) {
            el('todayStatus').textContent = 'Recheck failed: ' + error.message;
        } finally {
            b.disabled = false;
        }
    }

    loadFilters();
    bindToggle('filterDirection', 'direction');
    bindToggle('filterAsset', 'asset');
    el('filterAccount').onchange = () => {
        filters.account = el('filterAccount').value;
        saveFilters();
        render();
    };
    for (const close of document.querySelectorAll('[data-close]')) {
        close.onclick = () => closeModal(close.dataset.close);
    }
    el('openAccounts').onclick = openAccounts;
    el('recheckHoldings').onclick = recheck;
    loadAll();
})();
