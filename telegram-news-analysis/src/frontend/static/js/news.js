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
            newsResults.innerHTML = `<div class="no-results">Ничего не найдено</div>`;
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
    
            // Исправленная обработка тональности
            let sentimentHtml = "";
            if (item.sentiment && item.sentiment.label) {
                const sentimentKey = item.sentiment.label;
                const sentimentInfo = sentimentMap[sentimentKey] || sentimentMap["neutral"];
                const score = item.sentiment.score ? Math.round(item.sentiment.score * 100) : "";
                const scoreText = score ? ` (${score}%)` : "";
                sentimentHtml = `<span class="badge ${sentimentInfo.class}">${sentimentInfo.label}${scoreText}</span>`;
            }
    
            // Исправленная обработка тем
            let topicsHtml = "";
            if (item.topics && Array.isArray(item.topics)) {
                // Фильтрация тем - проверяем, что тема релевантна тексту
                const keywords = extractKeywords(item.text.toLowerCase());
                
                topicsHtml = item.topics
                    .filter(topic => {
                        // Получаем метку темы
                        const topicLabel = typeof topic === 'object' && topic.label 
                            ? topic.label.toLowerCase() 
                            : (typeof topic === 'string' ? topic.toLowerCase() : "");
                        
                        // Проверяем, содержит ли текст слова из темы
                        return keywords.some(keyword => topicLabel.includes(keyword));
                    })
                    .map(topic => {
                        const topicLabel = typeof topic === 'object' && topic.label 
                            ? topic.label 
                            : (typeof topic === 'string' ? topic : "");
                        
                        if (topicLabel) {
                            return `<span class="badge bg-info mx-1">${topicLabel}</span>`;
                        }
                        return "";
                    })
                    .join("");
                 // Если после фильтрации не осталось тем, попробуем извлечь из текста
            if (!topicsHtml && item.text) {
                const extractedTopics = extractTopicsFromText(item.text);
                topicsHtml = extractedTopics.map(topic => 
                    `<span class="badge bg-info mx-1">${topic}</span>`
                ).join("");
                }
            }
            function extractKeywords(text) {
                // Удаляем пунктуацию и разбиваем на слова
                const words = text.replace(/[.,\/#!$%\^&\*;:{}=\-_`~()]/g, "").split(/\s+/);
                
                // Фильтруем слова длиннее 4 символов и не являющиеся стоп-словами
                const stopWords = ["этот", "быть", "весь", "этот", "один", "такой", "чтобы", "который"];
                return words.filter(word => word.length > 4 && !stopWords.includes(word));
            }
            function extractTopicsFromText(text) {
                // Проверяем наличие ключевых слов для определенных тем
                const topicMappings = {
                    "Экология": ["экологи", "загрязнен", "воздух", "окружающ"],
                    "Концерты": ["концерт", "выступлен", "билет", "зал", "сцен"],
                    "Знаменитости": ["брежнев", "певиц", "артист", "звезд"],
                    "Политика": ["власт", "правительств", "президент", "политик"],
                    "Путешествия": ["путешеств", "поездк", "страна"]
                };
                const lowerText = text.toLowerCase();
                const foundTopics = [];
                
                for (const [topic, keywords] of Object.entries(topicMappings)) {
                    if (keywords.some(keyword => lowerText.includes(keyword))) {
                        foundTopics.push(topic);
                    }
                }
                
                return foundTopics;
            }
                

    
            // Исправленная обработка основной сущности
            let entityHtml = "";
            if (item.main_entity && item.main_entity.name) {
                entityHtml = `<span class="badge bg-primary mx-1">${item.main_entity.name}</span>`;
            }
    
            html += `
                <div class="card fade-in">
                    <div class="card-body">
                        <div class="d-flex justify-content-between">
                            <h5 class="card-title">От: ${item.channel_name}</h5>
                            <small class="text-muted">${date}</small>
                        </div>
                        <p class="card-text">${item.text}</p>
                        <div class="d-flex justify-content-between">
                            <div>
                                ${sentimentHtml} ${topicsHtml} ${entityHtml}
                            </div>
                            <div><small class="text-muted">Просмотры: ${item.views ?? "N/A"}</small></div>
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
