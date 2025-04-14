document.addEventListener("DOMContentLoaded", () => {
    const searchForm = document.getElementById("searchForm");
    const channelSelect = document.getElementById("channelSelect");
    const newsResults = document.getElementById("newsResults");
    const loadMoreBtn = document.getElementById("loadMore");
    const resultsInfo = document.getElementById("resultsInfo");

    let currentPage = 1;
    const limit = 10;
    let total = 0;
    let currentQuery = "";
    let currentChannel = "";

    async function fetchChannels() {
        try {
            const res = await fetch("/api/proxy/channels");
            const data = await res.json();
            data.channels.forEach(channel => {
                const option = document.createElement("option");
                option.value = channel.channel_name;
                option.textContent = channel.title || channel.channel_name;
                channelSelect.appendChild(option);
            });
        } catch (err) {
            console.error("Ошибка при загрузке каналов", err);
        }
    }

    async function fetchNews(page = 1, freshSearch = false) {
        const query = document.getElementById("searchQuery").value.trim();
        const channel = channelSelect.value;
        const params = new URLSearchParams({
            limit,
            skip: (page - 1) * limit,
        });

        let endpoint = "/api/proxy/news/latest";

        if (query) {
            endpoint = "/api/proxy/news/search";
            params.append("q", query);
            currentQuery = query;
        }

        if (channel) {
            params.append("channel", channel);
            currentChannel = channel;
        }

        if (freshSearch) {
            newsResults.innerHTML = `<div class="text-center py-5"><div class="spinner-border" role="status"></div></div>`;
            resultsInfo.textContent = "";
        } else {
            loadMoreBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Загрузка...`;
        }

        try {
            const res = await fetch(`${endpoint}?${params}`);
            const data = await res.json();
            total = data.total;

            if (freshSearch) {
                resultsInfo.textContent = `Найдено: ${total}`;
            }

            renderNews(data.news, freshSearch);
            currentPage = page;

            const more = currentPage * limit < total;
            loadMoreBtn.style.display = more ? "inline-block" : "none";
            loadMoreBtn.innerHTML = "Загрузить еще";
        } catch (err) {
            newsResults.innerHTML = `<div class="alert alert-danger">Ошибка при загрузке новостей. Попробуйте позже.</div>`;
            console.error(err);
        }
    }

    function renderNews(items, fresh = false) {
        if (items.length === 0 && fresh) {
            newsResults.innerHTML = `<div class="alert alert-info">Ничего не найдено</div>`;
            return;
        }

        let html = "";
        for (const item of items) {
            const date = new Date(item.date).toLocaleString("ru-RU");
            const sentimentMap = {
                positive: { class: "bg-success", label: "Позитивно" },
                negative: { class: "bg-danger", label: "Негативно" },
                neutral: { class: "bg-secondary", label: "Нейтрально" },
            };

            const sentiment = item.sentiment?.label;
            const badge = sentiment ? sentimentMap[sentiment] || sentimentMap["neutral"] : null;
            const sentimentHtml = badge ? `<span class="badge ${badge.class}">${badge.label}</span>` : "";

            const topicsHtml = (item.topics || [])
                .map(topic => `<span class="badge bg-info me-1">${topic}</span>`)
                .join("");

            html += `
                <div class="card mb-3 shadow-sm">
                    <div class="card-body">
                        <div class="d-flex justify-content-between">
                            <h5 class="card-title mb-2">От: ${item.channel_name}</h5>
                            <small class="text-muted">${date}</small>
                        </div>
                        <p class="card-text">${item.text}</p>
                        <div class="d-flex justify-content-between align-items-center">
                            <div>${sentimentHtml} ${topicsHtml}</div>
                            <div class="text-muted"><small>Просмотры: ${item.views ?? "N/A"}</small></div>
                        </div>
                    </div>
                </div>`;
        }

        if (fresh) {
            newsResults.innerHTML = html;
        } else {
            newsResults.insertAdjacentHTML("beforeend", html);
        }
    }

    // Инициализация
    fetchChannels();
    fetchNews(1, true);

    searchForm.addEventListener("submit", e => {
        e.preventDefault();
        currentPage = 1;
        fetchNews(1, true);
    });

    loadMoreBtn.addEventListener("click", () => {
        fetchNews(currentPage + 1);
    });
});
