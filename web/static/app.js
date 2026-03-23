const API_BASE = '';
let currentPage = 1;
const pageSize = 10;

// ── Tab navigation ─────────────────────────────────────────────────────────────
document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
        const tabName = btn.dataset.tab;
        document.querySelectorAll('.tab-btn').forEach((item) => item.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach((item) => item.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(tabName).classList.add('active');
    });
});

// ── Event listeners ────────────────────────────────────────────────────────────
document.getElementById('loadArticles').addEventListener('click', () => {
    currentPage = 1;
    loadArticles();
});
document.getElementById('prevPage').addEventListener('click', () => {
    if (currentPage > 1) { currentPage -= 1; loadArticles(); }
});
document.getElementById('nextPage').addEventListener('click', () => {
    currentPage += 1; loadArticles();
});
document.getElementById('searchBtn').addEventListener('click', searchArticles);
document.getElementById('searchInput').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') searchArticles();
});
document.getElementById('loadStats').addEventListener('click', loadStats);

// ── Filter helpers ─────────────────────────────────────────────────────────────
function buildFilterParams(prefix) {
    const params = new URLSearchParams();
    const bucket = document.getElementById(`${prefix}Bucket`)?.value;
    const signal = document.getElementById(`${prefix}Signal`)?.value;
    const dateFrom = document.getElementById(`${prefix}DateFrom`)?.value;
    const dateTo = document.getElementById(`${prefix}DateTo`)?.value;
    if (bucket) params.set('bucket', bucket);
    if (signal) params.set('signal_type', signal);
    if (dateFrom) params.set('date_from', dateFrom);
    if (dateTo) params.set('date_to', dateTo);
    return params;
}

function populateSelect(id, values, allLabel = 'All') {
    const el = document.getElementById(id);
    if (!el) return;
    const current = el.value;
    el.innerHTML = `<option value="">${allLabel}</option>`;
    values.forEach((v) => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = humanize(v);
        el.appendChild(opt);
    });
    el.value = current;
}

// ── Load dynamic filters from API ─────────────────────────────────────────────
async function loadFilters() {
    try {
        const res = await fetch(`${API_BASE}/api/filters`);
        if (!res.ok) return;
        const { buckets, signal_types } = await res.json();

        ['browse', 'search'].forEach((prefix) => {
            populateSelect(`${prefix}Bucket`, buckets, 'All topics');
            populateSelect(`${prefix}Signal`, signal_types, 'All signals');
        });

        const badgesEl = document.getElementById('heroBadges');
        if (badgesEl) {
            badgesEl.innerHTML = buckets.map((b) => `<span class="badge">${humanize(b)}</span>`).join('');
        }
    } catch (_) {}
}

// ── Articles ───────────────────────────────────────────────────────────────────
async function loadArticles() {
    const listEl = document.getElementById('articlesList');
    const params = buildFilterParams('browse');
    params.set('limit', pageSize);
    params.set('offset', (currentPage - 1) * pageSize);
    listEl.innerHTML = '<div class="loading">Loading articles...</div>';

    try {
        const res = await fetch(`${API_BASE}/api/articles?${params}`);
        if (!res.ok) throw new Error('Failed to load articles');
        const articles = await res.json();

        if (articles.length === 0) {
            listEl.innerHTML = '<div class="empty-state"><h3>No articles found</h3><p>Try adjusting your filters.</p></div>';
            return;
        }
        listEl.innerHTML = articles.map(renderArticle).join('');
        document.getElementById('pageInfo').textContent = `Page ${currentPage}`;
        document.getElementById('prevPage').disabled = currentPage === 1;
        document.getElementById('nextPage').disabled = articles.length < pageSize;
    } catch (err) {
        listEl.innerHTML = `<div class="error">Error: ${err.message}</div>`;
    }
}

async function searchArticles() {
    const query = document.getElementById('searchInput').value.trim();
    const resultsEl = document.getElementById('searchResults');
    const useSemantic = document.getElementById('semanticToggle')?.checked ?? true;

    if (!query) {
        resultsEl.innerHTML = '<div class="error">Enter a search term.</div>';
        return;
    }

    const params = buildFilterParams('search');
    params.set('q', query);
    params.set('limit', 20);

    const endpoint = useSemantic
        ? `${API_BASE}/api/articles/semantic-search`
        : `${API_BASE}/api/articles/search`;
    const mode = useSemantic ? 'semantic' : 'keyword';

    resultsEl.innerHTML = `<div class="loading">Searching (${mode})...</div>`;

    try {
        const res = await fetch(`${endpoint}?${params}`);
        if (!res.ok) throw new Error('Search failed');
        const articles = await res.json();

        if (articles.length === 0) {
            resultsEl.innerHTML = '<div class="empty-state"><h3>No results found</h3><p>Try a different query.</p></div>';
            return;
        }
        resultsEl.innerHTML = `
            <div class="result-summary">
                ${articles.length} result(s) for "${query}" <span class="mode-badge">${mode}</span>
            </div>
            ${articles.map((a) => renderArticle(a, useSemantic)).join('')}
        `;
    } catch (err) {
        resultsEl.innerHTML = `<div class="error">Error: ${err.message}</div>`;
    }
}

// ── Stats ──────────────────────────────────────────────────────────────────────
async function loadStats() {
    const statsEl = document.getElementById('statsDisplay');
    statsEl.innerHTML = '<div class="loading">Loading stats...</div>';

    try {
        const res = await fetch(`${API_BASE}/api/stats`);
        if (!res.ok) throw new Error('Failed to load stats');
        const stats = await res.json();

        const bucketCards = Object.entries(stats.by_bucket || {}).map(([bucket, count]) => `
            <div class="stat-card stat-card-league">
                <div class="stat-value">${count}</div>
                <div class="stat-label">${humanize(bucket)}</div>
            </div>
        `).join('');

        const signalCards = Object.entries(stats.by_signal_type || {}).map(([signal, count]) => `
            <div class="mini-stat">
                <span class="mini-stat-key">${humanize(signal)}</span>
                <span class="mini-stat-value">${count}</span>
            </div>
        `).join('');

        const publishers = (stats.top_publishers || []).map((item) => `
            <li><span>${item.publisher_name}</span><strong>${item.count}</strong></li>
        `).join('');

        const timeline = (stats.recent_timeline || []).map((item) => `
            <div class="timeline-row">
                <span>${new Date(item.date).toLocaleDateString()}</span>
                <strong>${item.count}</strong>
            </div>
        `).join('');

        statsEl.innerHTML = `
            <section class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">${stats.total_articles || 0}</div>
                    <div class="stat-label">Total articles</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${stats.unique_publishers || 0}</div>
                    <div class="stat-label">Publishers</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${stats.date_range?.latest ? new Date(stats.date_range.latest).toLocaleDateString() : 'N/A'}</div>
                    <div class="stat-label">Latest article</div>
                </div>
            </section>

            <section class="stats-section">
                <h3>By topic</h3>
                <div class="stats-grid">${bucketCards}</div>
            </section>

            <section class="stats-section split-grid">
                <div class="stats-panel">
                    <h3>Signal mix</h3>
                    <div class="mini-stat-list">${signalCards}</div>
                </div>
                <div class="stats-panel">
                    <h3>Top publishers</h3>
                    <ul class="publisher-list">${publishers}</ul>
                </div>
                <div class="stats-panel">
                    <h3>Recent timeline</h3>
                    <div class="timeline-list">${timeline}</div>
                </div>
            </section>
        `;
    } catch (err) {
        statsEl.innerHTML = `<div class="error">Error: ${err.message}</div>`;
    }
}

// ── Render ─────────────────────────────────────────────────────────────────────
function renderArticle(article, showSimilarity = false) {
    const date = article.published_date ? new Date(article.published_date).toLocaleDateString() : 'Unknown date';
    const bucket = humanize(article.bucket || '');
    const signal = humanize(article.signal_type || '');
    const source = article.source_type ? article.source_type.toUpperCase() : 'SOURCE';
    const similarityBadge = showSimilarity && article.similarity != null
        ? `<span class="similarity-badge">${(article.similarity * 100).toFixed(0)}% match</span>`
        : '';

    return `
        <article class="article-card">
            <div class="article-header">
                <div>
                    <div class="article-kicker">${bucket}${signal ? ' · ' + signal : ''} ${similarityBadge}</div>
                    <div class="article-title">
                        ${article.url
                            ? `<a href="${article.url}" target="_blank" rel="noreferrer">${article.title || 'Untitled'}</a>`
                            : (article.title || 'Untitled')}
                    </div>
                </div>
                <span class="source-pill">${source}</span>
            </div>
            <div class="article-meta">
                <span>${article.publisher_name || 'Unknown publisher'}</span>
                <span>${date}</span>
            </div>
            ${article.content ? `<div class="article-content">${article.content}</div>` : ''}
        </article>
    `;
}

// ── Helpers ────────────────────────────────────────────────────────────────────
function humanize(str) {
    if (!str) return '';
    return str.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

// ── Init ───────────────────────────────────────────────────────────────────────
window.addEventListener('load', async () => {
    await loadFilters();
    await loadArticles();
});
