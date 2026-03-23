const API_BASE = '';
let currentPage = 1;
const pageSize = 10;

document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
        const tabName = btn.dataset.tab;
        document.querySelectorAll('.tab-btn').forEach((item) => item.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach((item) => item.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(tabName).classList.add('active');
    });
});

document.getElementById('loadArticles').addEventListener('click', () => {
    currentPage = 1;
    loadArticles();
});
document.getElementById('prevPage').addEventListener('click', () => {
    if (currentPage > 1) {
        currentPage -= 1;
        loadArticles();
    }
});
document.getElementById('nextPage').addEventListener('click', () => {
    currentPage += 1;
    loadArticles();
});

document.getElementById('searchBtn').addEventListener('click', searchArticles);
document.getElementById('searchInput').addEventListener('keypress', (event) => {
    if (event.key === 'Enter') {
        searchArticles();
    }
});

document.getElementById('queryBtn').addEventListener('click', queryGraphRAG);
document.getElementById('graphragQuery').addEventListener('keypress', (event) => {
    if (event.ctrlKey && event.key === 'Enter') {
        queryGraphRAG();
    }
});

document.getElementById('loadStats').addEventListener('click', loadStats);

function buildFilterParams(prefix) {
    const params = new URLSearchParams();
    const league = document.getElementById(`${prefix}League`)?.value;
    const signalType = document.getElementById(`${prefix}Signal`)?.value;
    const dateFrom = document.getElementById(`${prefix}DateFrom`)?.value;
    const dateTo = document.getElementById(`${prefix}DateTo`)?.value;

    if (league) params.set('league', league);
    if (signalType) params.set('signal_type', signalType);
    if (dateFrom) params.set('date_from', dateFrom);
    if (dateTo) params.set('date_to', dateTo);
    return params;
}

async function loadArticles() {
    const offset = (currentPage - 1) * pageSize;
    const listEl = document.getElementById('articlesList');
    const params = buildFilterParams('browse');
    params.set('limit', pageSize);
    params.set('offset', offset);

    listEl.innerHTML = '<div class="loading">Loading football monitor...</div>';

    try {
        const response = await fetch(`${API_BASE}/api/articles?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to load monitor');

        const articles = await response.json();
        if (articles.length === 0) {
            listEl.innerHTML = '<div class="empty-state"><h3>No football coverage found</h3><p>Try loosening the league or signal filters.</p></div>';
            return;
        }

        listEl.innerHTML = articles.map(renderArticle).join('');
        document.getElementById('pageInfo').textContent = `Page ${currentPage}`;
        document.getElementById('prevPage').disabled = currentPage === 1;
        document.getElementById('nextPage').disabled = articles.length < pageSize;
    } catch (error) {
        listEl.innerHTML = `<div class="error">Error loading monitor: ${error.message}</div>`;
    }
}

async function searchArticles() {
    const query = document.getElementById('searchInput').value.trim();
    const resultsEl = document.getElementById('searchResults');
    const useSemantic = document.getElementById('semanticToggle')?.checked ?? false;

    if (!query) {
        resultsEl.innerHTML = '<div class="error">Enter a football search term.</div>';
        return;
    }

    const params = buildFilterParams('search');
    params.set('q', query);
    params.set('limit', 20);

    const mode = useSemantic ? 'semantic' : 'keyword';
    const endpoint = useSemantic
        ? `${API_BASE}/api/articles/semantic-search`
        : `${API_BASE}/api/articles/search`;

    resultsEl.innerHTML = `<div class="loading">Searching football coverage (${mode})...</div>`;

    try {
        const response = await fetch(`${endpoint}?${params.toString()}`);
        if (!response.ok) throw new Error('Search failed');

        const articles = await response.json();
        if (articles.length === 0) {
            resultsEl.innerHTML = '<div class="empty-state"><h3>No football results found</h3><p>Try another club, league, or story type.</p></div>';
            return;
        }

        const modeLabel = useSemantic ? 'semantic' : 'keyword';
        resultsEl.innerHTML = `
            <div class="result-summary">Found ${articles.length} football article(s) for "${query}" <span class="mode-badge">${modeLabel}</span></div>
            ${articles.map((a) => renderArticle(a, useSemantic)).join('')}
        `;
    } catch (error) {
        resultsEl.innerHTML = `<div class="error">Error searching coverage: ${error.message}</div>`;
    }
}

async function queryGraphRAG() {
    const query = document.getElementById('graphragQuery').value.trim();
    const league = document.getElementById('queryLeague').value;
    const signalType = document.getElementById('querySignal').value;
    const maxResults = parseInt(document.getElementById('maxResults').value, 10) || 8;

    const answerEl = document.getElementById('graphragAnswer');
    const entitiesEl = document.getElementById('graphragEntities');
    const sourcesEl = document.getElementById('graphragSources');
    const loadingEl = document.getElementById('graphragLoading');

    if (!query) {
        answerEl.innerHTML = '<div class="error">Enter a football question.</div>';
        return;
    }

    loadingEl.style.display = 'block';
    answerEl.innerHTML = '';
    entitiesEl.innerHTML = '';
    sourcesEl.innerHTML = '';

    try {
        const response = await fetch(`${API_BASE}/api/graphrag/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                query,
                max_results: maxResults,
                league: league || null,
                signal_type: signalType || null,
            }),
        });
        if (!response.ok) throw new Error('Query failed');

        const result = await response.json();
        answerEl.innerHTML = `
            <h3>Monitor Answer</h3>
            <p>${result.answer || 'No answer generated.'}</p>
        `;

        if (result.entities?.length) {
            entitiesEl.innerHTML = `
                <h3>Detected entities</h3>
                <div class="entities-list">
                    ${result.entities.map((entity) => `<span class="entity-tag">${entity}</span>`).join('')}
                </div>
            `;
        }

        if (result.sources?.length) {
            sourcesEl.innerHTML = `
                <h3>Supporting coverage</h3>
                <div class="articles-list">
                    ${result.sources.map(renderArticle).join('')}
                </div>
            `;
        } else {
            sourcesEl.innerHTML = '<div class="empty-state"><h3>No supporting coverage found</h3></div>';
        }
    } catch (error) {
        answerEl.innerHTML = `<div class="error">Error querying monitor: ${error.message}</div>`;
    } finally {
        loadingEl.style.display = 'none';
    }
}

async function loadStats() {
    const statsEl = document.getElementById('statsDisplay');
    statsEl.innerHTML = '<div class="loading">Loading football stats...</div>';

    try {
        const response = await fetch(`${API_BASE}/api/stats`);
        if (!response.ok) throw new Error('Failed to load stats');
        const stats = await response.json();

        const leagueCards = Object.values(stats.by_league || {}).map((item) => `
            <div class="stat-card stat-card-league">
                <div class="stat-value">${item.count}</div>
                <div class="stat-label">${item.label}</div>
            </div>
        `).join('');

        const signalCards = Object.entries(stats.by_signal_type || {}).map(([signal, count]) => `
            <div class="mini-stat">
                <span class="mini-stat-key">${humanizeSignal(signal)}</span>
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
                    <div class="stat-label">Tracked football articles</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${stats.unique_publishers || 0}</div>
                    <div class="stat-label">Active publishers</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${stats.date_range?.latest ? new Date(stats.date_range.latest).toLocaleDateString() : 'N/A'}</div>
                    <div class="stat-label">Latest coverage</div>
                </div>
            </section>

            <section class="stats-section">
                <h3>League volume</h3>
                <div class="stats-grid">${leagueCards}</div>
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
    } catch (error) {
        statsEl.innerHTML = `<div class="error">Error loading stats: ${error.message}</div>`;
    }
}

function humanizeSignal(signal) {
    if (signal === 'general_news') return 'General news';
    if (signal === 'transfers') return 'Transfers';
    if (signal === 'trends') return 'Trends';
    return signal || 'Unknown';
}

function renderArticle(article, showSimilarity = false) {
    const publishedDate = article.published_date ? new Date(article.published_date).toLocaleDateString() : 'Unknown date';
    const authors = article.authors?.length ? article.authors.join(', ') : 'Unknown author';
    const league = article.league_label || article.league || 'Football';
    const signal = humanizeSignal(article.signal_type);
    const source = article.source_type ? article.source_type.toUpperCase() : 'SOURCE';
    const similarityBadge = showSimilarity && article.similarity != null
        ? `<span class="similarity-badge" title="Semantic similarity">${(article.similarity * 100).toFixed(0)}% match</span>`
        : '';

    return `
        <article class="article-card">
            <div class="article-header">
                <div>
                    <div class="article-kicker">${league} · ${signal} ${similarityBadge}</div>
                    <div class="article-title">
                        ${article.url ? `<a href="${article.url}" target="_blank" rel="noreferrer">${article.title || 'Untitled'}</a>` : (article.title || 'Untitled')}
                    </div>
                </div>
                <span class="source-pill">${source}</span>
            </div>
            <div class="article-meta">
                <span>${article.publisher_name || article.publisher_domain || 'Unknown publisher'}</span>
                <span>${publishedDate}</span>
                <span>${authors}</span>
            </div>
            ${article.excerpt ? `<div class="article-excerpt">${article.excerpt}</div>` : ''}
            ${article.content ? `<div class="article-content">${article.content}</div>` : ''}
            ${article.keywords?.length ? `
                <div class="article-tags">
                    ${article.keywords.map((keyword) => `<span class="tag">${keyword}</span>`).join('')}
                </div>
            ` : ''}
        </article>
    `;
}

window.addEventListener('load', async () => {
    await loadArticles();
    await loadStats();
});
