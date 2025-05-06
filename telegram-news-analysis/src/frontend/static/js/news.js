class NewsAnalyzer {
  constructor() {
    // Состояние данных
    this.currentPage = 1;
    this.limit = 10;
    this.total = 0;
    this.currentQuery = "";
    this.currentChannel = "";
    this.isLoading = false;
    this.reportContainer = null;

    // Инициализация
    this.initElements();
    this.initEventListeners();
    this.loadInitialData();
  }

  // --- DOM Elements ---
  initElements() {
    this.searchForm = document.getElementById("searchForm");
    this.searchBox = document.getElementById("searchQuery");
    this.channelSelect = document.getElementById("channelSelect");
    this.newsResults = document.getElementById("newsResults");
    this.loadMoreBtn = document.getElementById("loadMore");
    this.resultsInfo = document.getElementById("resultsInfo");
    this.reportContainer = document.getElementById("reportContainer") || this.createReportContainer();
    this.errorContainer = document.getElementById("errorContainer") || this.createErrorContainer();
  }

  createReportContainer() {
    const div = document.createElement("div");
    div.id = "reportContainer";
    document.body.appendChild(div);
    return div;
  }

  createErrorContainer() {
    const div = document.createElement("div");
    div.id = "errorContainer";
    div.className = "alert alert-danger";
    div.style.display = "none";
    div.style.marginBottom = "15px";
    document.body.insertBefore(div, this.newsResults || document.body.firstChild);
    return div;
  }

  // --- Event listeners ---
  initEventListeners() {
    if (this.searchForm) {
      this.searchForm.addEventListener("submit", e => {
        e.preventDefault();
        this.currentPage = 1;
        this.fetchNews(1, true);
      });
    }

    if (this.loadMoreBtn) {
      this.loadMoreBtn.addEventListener("click", () => {
        this.fetchNews(this.currentPage + 1);
      });
    }

    if (this.channelSelect) {
      this.channelSelect.addEventListener("change", () => {
        this.currentPage = 1;
        this.fetchNews(1, true);
      });
    }
  }

  // --- Initial load ---
  async loadInitialData() {
    try {
      await this.fetchChannels();
      await this.fetchNews(1, true);
    } catch (err) {
      this.showError("Ошибка при инициализации приложения. Пожалуйста, обновите страницу.");
      console.error("Initialization error:", err);
    }
  }

  // --- Показать ошибку ---
  showError(message) {
    if (this.errorContainer) {
      this.errorContainer.textContent = message;
      this.errorContainer.style.display = "block";
      
      // Скрыть ошибку через 5 секунд
      setTimeout(() => {
        this.errorContainer.style.display = "none";
      }, 5000);
    }
  }

  // --- Каналы пользователя ---
  async fetchChannels() {
    if (!this.channelSelect) return;
    
    this.channelSelect.disabled = true;
    this.channelSelect.innerHTML = '<option value="">Загрузка каналов...</option>';
    
    try {
      const res = await fetch("/api/proxy/channels");
      const data = await res.json();
      
      this.channelSelect.innerHTML = '<option value="">Все каналы</option>';
      
      if (data.channels && Array.isArray(data.channels)) {
        data.channels.forEach(channel => {
          const option = document.createElement("option");
          option.value = channel.channel_name;
          option.textContent = channel.title || channel.channel_name;
          this.channelSelect.appendChild(option);
        });
      }
    } catch (err) {
      this.showError("Ошибка загрузки каналов");
      this.channelSelect.innerHTML = '<option value="">Ошибка загрузки</option>';
      console.error("Ошибка при загрузке каналов", err);
    } finally {
      this.channelSelect.disabled = false;
    }
  }

  // --- Получение новостей (latest или search) ---
  async fetchNews(page = 1, freshSearch = false) {
    if (this.isLoading) return;
    this.isLoading = true;
    
    const query = this.searchBox ? this.searchBox.value.trim() : "";
    const channel = this.channelSelect ? this.channelSelect.value : "";
    
    const params = new URLSearchParams({
      limit: this.limit,
      skip: (page - 1) * this.limit,
    });

    let endpoint = "/api/proxy/news/latest";
    if (query) {
      endpoint = "/api/proxy/news/search";
      params.append("q", query);
      this.currentQuery = query;
    }

    if (channel) {
      params.append("channel", channel);
      this.currentChannel = channel;
    }

    this.showLoadingIndicator(freshSearch);

    try {
      const res = await fetch(`${endpoint}?${params}`);
      const data = await res.json();
      
      const news = data.news || [];
      this.total = data.total || news.length;
      
      if (freshSearch && this.resultsInfo) {
        this.resultsInfo.textContent = `Найдено: ${this.total}`;
      }

      this.renderNews(news, freshSearch);
      this.currentPage = page;
      this.updateLoadMoreButton();
    } catch (err) {
      this.showNewsError();
      this.showError("Ошибка загрузки новостей");
      console.error("News loading error:", err);
    } finally {
      this.isLoading = false;
    }
  }

  // --- Рендер списка новостей ---
  renderNews(items, fresh = false) {
    if (!this.newsResults) return;
    
    if (!items || items.length === 0 && fresh) {
      this.newsResults.innerHTML = `<div class="alert alert-info">Ничего не найдено</div>`;
      return;
    }
    
    const html = items.map(item => this.createNewsItemHtml(item)).join('');
    
    if (fresh) {
      this.newsResults.innerHTML = html;
    } else {
      this.newsResults.insertAdjacentHTML("beforeend", html);
    }
    
    // Применяем форматирование Markdown прямо здесь
    this.formatAllMarkdown();
  }
  // Добавьте новый метод для обработки Markdown
  formatAllMarkdown() {
    // Находим все тексты с Markdown-разметкой
    const textElements = this.newsResults.querySelectorAll('.card-text');
    
    textElements.forEach(element => {
      if (!element) return;
      
      // Проверяем наличие маркеров Markdown
      const content = element.innerHTML;
      if (content.includes('**') || content.includes('*') || 
          content.includes('`') || content.includes('[')) {
        
        // Применяем простое Markdown-форматирование
        const formatted = this.formatMarkdown(content);
        element.innerHTML = formatted;
      }
    });
  }
  formatMarkdown(text) {
    if (!text) return '';
    
    return text
      // Заголовки - здесь не нужны в новостях
      // Жирный текст
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      // Курсив
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      // Код
      .replace(/`([^`]+)`/g, '<code>$1</code>')
      // Ссылки
      .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');
  }

  // --- HTML для одной новости ---
  createNewsItemHtml(item) {
    try {
      let dateStr = "Не указана";
      
      if (item.date) {
        if (typeof item.date === "number") {
          dateStr = new Date(item.date).toLocaleString("ru-RU");
        } else if (typeof item.date === "string") {
          if (item.date.includes('T')) {
            dateStr = new Date(item.date).toLocaleString("ru-RU");
          } else {
            dateStr = item.date;
          }
        }
      }

      return `
      <div class="card mb-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h5 class="mb-0">Канал: ${this.escapeHtml(item.channel_name)}</h5>
          <small>${dateStr}</small>
        </div>
        <div class="card-body">
          <p class="card-text">${this.escapeHtml(item.text)}</p>
          <div class="d-flex flex-wrap gap-2 mt-3">
            ${this.getSentimentHtml(item.sentiment)}
            ${this.getTopicsHtml(item.topics)}
            ${this.getEntityHtml(item.main_entity)}
            <small class="text-muted ms-auto">Просмотры: ${item.views ?? "N/A"}</small>
          </div>
        </div>
      </div>`;
    } catch (err) {
      console.error("Error rendering news item:", err, item);
      return `<div class="alert alert-danger">Ошибка отображения новости</div>`;
    }
  }

  // --- Форматирование тональности ---
  getSentimentHtml(sentiment) {
    if (!sentiment) return "";
    
    let bgClass = "bg-secondary";
    let txt = "Нейтрально";
    
    if (sentiment.label === "positive") { 
      bgClass = "bg-success"; 
      txt = "Позитив"; 
    } else if (sentiment.label === "negative") { 
      bgClass = "bg-danger"; 
      txt = "Негатив"; 
    } else if (sentiment.label === "neutral") { 
      bgClass = "bg-secondary"; 
      txt = "Нейтрально"; 
    } else { 
      txt = sentiment.label; 
    }
    
    if (typeof sentiment.score === "number") {
      txt += ` (${sentiment.score.toFixed(2)})`;
    }
    
    return `<span class="badge ${bgClass}">${txt}</span>`;
  }

  // --- Форматирование тем ---
  getTopicsHtml(topics) {
    if (!topics || !topics.length) return "";
    const topicsText = topics
      .filter(t => t && t.label)
      .map(t => this.escapeHtml(t.label))
      .join(', ');
      
    return topicsText ? `<span class="badge bg-info text-dark"><b>Темы:</b> ${topicsText}</span>` : "";
  }

  // --- Форматирование главной сущности ---
  getEntityHtml(entity) {
    if (!entity || !entity.name) return "";
    
    let txt = `<b>Сущность:</b> ${this.escapeHtml(entity.name)}`;
    
    if (entity.type) {
      txt += ` <span>(${entity.type})</span>`;
    }
    
    return `<span class="badge bg-light text-dark">${txt}</span>`;
  }

  // --- Кнопка Загрузить еще ---
  updateLoadMoreButton() {
    if (!this.loadMoreBtn) return;
    
    const more = this.currentPage * this.limit < this.total;
    this.loadMoreBtn.style.display = more ? "inline-block" : "none";
    this.loadMoreBtn.textContent = "Загрузить еще";
  }

  showLoadingIndicator(freshSearch) {
    if (freshSearch && this.newsResults) {
      this.newsResults.innerHTML = `
        <div class="text-center py-4">
          <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Загрузка...</span>
          </div>
          <p class="mt-2">Загрузка новостей...</p>
        </div>`;
      
      if (this.resultsInfo) {
        this.resultsInfo.textContent = "";
      }
    } else if (this.loadMoreBtn) {
      this.loadMoreBtn.innerHTML = `
        <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
        Загрузка...`;
    }
  }
  
  showNewsError() {
    if (this.newsResults) {
      this.newsResults.innerHTML = `
        <div class="alert alert-danger">
          Ошибка при загрузке новостей. Попробуйте позже.
        </div>`;
    }
  }

  // --- Генерация отчета ---
  async generateReport(entity = null, period = null) {
    entity = entity || this.currentQuery || (this.searchBox ? this.searchBox.value.trim() : "");
    period = period || "7d";
    
    if (!entity) {
      this.showError("Пожалуйста, введите запрос для генерации отчета");
      return;
    }
    
    this.showReportLoading(entity);

    try {
      const response = await fetch("/api/proxy/reports/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          entity: entity,
          time_period: period
        })
      });
      
      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      
      const data = await response.json();
      
      if (data.report_text || (data.report && data.report.generated_text)) {
        const reportText = data.report_text || (data.report && data.report.generated_text);
        this.renderReport(reportText, entity, period);
      } else if (data.request_id && !data.report_text) {
        if (typeof this.handlePendingReport === 'function') {
          this.handlePendingReport(data.request_id, entity, period);
        } else {
          this.showReportError("Отчет еще не готов. Пожалуйста, попробуйте позже.");
        }
      } else {
        this.showReportError("Не удалось создать отчет. Сервер вернул пустой ответ.");
      }
    } catch (error) {
      console.error("Error generating report:", error);
      this.showReportError(`Не удалось сгенерировать отчет: ${error.message}`);
    }
  }

  showReportLoading(entity) {
    if (this.reportContainer) {
      this.reportContainer.innerHTML = `
        <div class="card mb-4">
          <div class="card-header">
            <h4>Генерация отчета</h4>
          </div>
          <div class="card-body">
            <p>Генерируем аналитический отчет по "${this.escapeHtml(entity)}".</p>
            <p>Это может занять несколько минут...</p>
            <div class="progress">
              <div class="progress-bar progress-bar-striped progress-bar-animated" style="width: 100%"></div>
            </div>
          </div>
        </div>`;
    }
  }

  renderReport(text, entity, period) {
    if (this.reportContainer) {
      this.reportContainer.innerHTML = `
        <div class="card mb-4">
          <div class="card-header">
            <h4>Аналитический отчет по "<b>${this.escapeHtml(entity)}</b>" (период: ${period || '7d'})</h4>
          </div>
          <div class="card-body">
            <pre class="report-text">${this.escapeHtml(text)}</pre>
          </div>
        </div>`;
    }
  }

  showReportError(message) {
    if (this.reportContainer) {
      this.reportContainer.innerHTML = `
        <div class="card mb-4 border-danger">
          <div class="card-header bg-danger text-white">
            <h4>Ошибка генерации отчета</h4>
          </div>
          <div class="card-body">
            <p>${message}</p>
          </div>
        </div>`;
    }
  }

  // --- XSS защита ---
  escapeHtml(str) {
    if (typeof str !== "string") return String(str || "");
    return str
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;")
      .replace(/`/g, "&#96;");
  }

  static init() {
    console.log("TeleNews Application Initialized");
    document.addEventListener("DOMContentLoaded", () => {
      new NewsAnalyzer();
    });
  }
}

// Отдельная глобальная функция форматирования Markdown
function applyMarkdownFormatting() {
  const markdownElements = document.querySelectorAll('.markdown-content');
  
  markdownElements.forEach(element => {
    try {
      const content = element.innerHTML;
      
      // Простое Markdown-форматирование
      const formattedText = content
        // Жирный текст
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        // Курсив
        .replace(/\*(.*?)\*/g, '<em>$1</em>')
        // Код
        .replace(/`([^`]+)`/g, '<code>$1</code>')
        // Ссылки
        .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');
      
      // Проверяем, существует ли объект DOMPurify для безопасности
      if (typeof DOMPurify !== 'undefined') {
        element.innerHTML = DOMPurify.sanitize(formattedText);
      } else {
        element.innerHTML = formattedText;
      }
    } catch (error) {
      console.error('Ошибка при обработке Markdown:', error);
    }
  });
}

// Функция для скрытия загрузочных сообщений в заголовках
function cleanupLoadingMessages() {
  // Исправляем заголовки, содержащие "Загрузка"
  document.querySelectorAll('h3, h4, h5').forEach(heading => {
    const text = heading.textContent;
    if (text && text.includes('Загрузка')) {
      heading.textContent = text.replace(/\s*Загрузка\.+\s*/, '');
    }
  });
  
  // Скрываем все индикаторы загрузки рядом с диаграммами
  document.querySelectorAll('.loading-indicator, .loading-placeholder').forEach(loader => {
    loader.style.display = 'none';
  });
}

// Функция для скрытия текстов "Загрузка" в заголовках
function hideLoadingText() {
  document.querySelectorAll('h3, h4, h5').forEach(heading => {
    if (heading.textContent.includes('Загрузка')) {
      heading.classList.add('loading-hidden');
    }
  });
}

// Вызываем функцию очистки загрузочных сообщений после того, как все диаграммы будут загружены
document.addEventListener('DOMContentLoaded', () => {
  // Сразу очищаем сообщения загрузки
  cleanupLoadingMessages();
  hideLoadingText();
  
  // Повторно запускаем очистку через небольшой интервал для динамически загружаемых элементов
  setTimeout(cleanupLoadingMessages, 1000);
  setTimeout(cleanupLoadingMessages, 3000);
});

document.addEventListener('DOMContentLoaded', applyMarkdownFormatting);

// Добавляем стиль для скрытия элементов с классом loading-hidden
const style = document.createElement('style');
style.textContent = `
  .loading-hidden {
    display: none !important;
  }
`;
document.head.appendChild(style);

NewsAnalyzer.init();
