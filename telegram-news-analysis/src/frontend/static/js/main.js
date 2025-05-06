
document.addEventListener('DOMContentLoaded', function() {
  console.log('TeleNews Application Initialized');
  
  // Инициализация компонентов
  initializeAlerts();
  initializeTooltips();
  initializeDropdowns();
  initializeCollapsibles();
  handleMobileNavigation();
  setupDarkModeToggle();
});


function initializeAlerts() {
  const alerts = document.querySelectorAll('.alert:not(.alert-persistent)');
  
  alerts.forEach(alert => {
    // Добавляем кнопку закрытия, если её нет
    if (!alert.querySelector('.close-btn')) {
      const closeButton = document.createElement('button');
      closeButton.innerHTML = '&times;';
      closeButton.className = 'close-btn';
      closeButton.addEventListener('click', () => dismissAlert(alert));
      alert.appendChild(closeButton);
    }
    
    // Автоматически скрываем через заданное время
    setTimeout(() => dismissAlert(alert), 5000);
  });
}

/**
 * Плавное скрытие уведомления
 * @param {HTMLElement} alert - Элемент уведомления для скрытия
 */
function dismissAlert(alert) {
  alert.style.transition = 'opacity 0.5s, transform 0.5s';
  alert.style.opacity = '0';
  alert.style.transform = 'translateY(-10px)';
  
  setTimeout(() => {
    alert.style.height = alert.offsetHeight + 'px';
    alert.style.overflow = 'hidden';
    
    setTimeout(() => {
      alert.style.height = '0';
      alert.style.margin = '0';
      alert.style.padding = '0';
      
      setTimeout(() => {
        alert.remove();
      }, 300);
    }, 100);
  }, 500);
}

/**
 * Инициализация всплывающих подсказок
 */
function initializeTooltips() {
  const tooltipTriggers = document.querySelectorAll('[data-tooltip]');
  
  tooltipTriggers.forEach(element => {
    element.addEventListener('mouseenter', event => {
      const tooltip = document.createElement('div');
      tooltip.className = 'tooltip';
      tooltip.textContent = element.getAttribute('data-tooltip');
      
      document.body.appendChild(tooltip);
      
      const elementRect = element.getBoundingClientRect();
      tooltip.style.left = (elementRect.left + elementRect.width / 2 - tooltip.offsetWidth / 2) + 'px';
      tooltip.style.top = (elementRect.top - tooltip.offsetHeight - 10) + window.scrollY + 'px';
      
      tooltip.style.opacity = '1';
      tooltip.style.transform = 'translateY(0)';
      
      element.addEventListener('mouseleave', () => {
        tooltip.style.opacity = '0';
        tooltip.style.transform = 'translateY(10px)';
        
        setTimeout(() => tooltip.remove(), 300);
      }, { once: true });
    });
  });
}

/**
 * Инициализация выпадающих меню
 */
function initializeDropdowns() {
  const dropdownToggles = document.querySelectorAll('.dropdown-toggle');
  
  dropdownToggles.forEach(toggle => {
    toggle.addEventListener('click', event => {
      event.preventDefault();
      const dropdown = toggle.nextElementSibling;
      
      // Закрыть другие открытые дропдауны
      document.querySelectorAll('.dropdown-menu.show').forEach(menu => {
        if (menu !== dropdown) menu.classList.remove('show');
      });
      
      dropdown.classList.toggle('show');
      
      // Закрыть при клике вне дропдауна
      const closeDropdown = event => {
        if (!toggle.contains(event.target) && !dropdown.contains(event.target)) {
          dropdown.classList.remove('show');
          document.removeEventListener('click', closeDropdown);
        }
      };
      
      if (dropdown.classList.contains('show')) {
        setTimeout(() => document.addEventListener('click', closeDropdown), 0);
      }
    });
  });
}

/**
 * Инициализация сворачиваемых элементов
 */
function initializeCollapsibles() {
  const collapseTriggers = document.querySelectorAll('[data-toggle="collapse"]');
  
  collapseTriggers.forEach(trigger => {
    trigger.addEventListener('click', event => {
      event.preventDefault();
      
      const targetId = trigger.getAttribute('data-target');
      const target = document.querySelector(targetId);
      
      if (target) {
        // Переключение видимости
        const isVisible = target.classList.contains('show');
        
        if (isVisible) {
          target.style.height = target.scrollHeight + 'px';
          
          // Форсируем перерисовку
          target.offsetHeight;
          
          target.style.height = '0';
          target.style.opacity = '0';
          
          setTimeout(() => {
            target.classList.remove('show');
            target.style.height = '';
          }, 300);
        } else {
          target.classList.add('show');
          target.style.height = '0';
          target.style.opacity = '0';
          
          // Форсируем перерисовку
          target.offsetHeight;
          
          target.style.height = target.scrollHeight + 'px';
          target.style.opacity = '1';
          
          setTimeout(() => {
            target.style.height = '';
          }, 300);
        }
        
        // Переключаем иконку, если она есть
        const iconElement = trigger.querySelector('i.toggle-icon');
        if (iconElement) {
          iconElement.classList.toggle('fa-chevron-down');
          iconElement.classList.toggle('fa-chevron-up');
        }
      }
    });
  });
}

/**
 * Управление мобильной навигацией
 */
function handleMobileNavigation() {
  const navToggler = document.querySelector('.navbar-toggler');
  const sidebarToggler = document.querySelector('#sidebarCollapse');
  
  if (navToggler) {
    navToggler.addEventListener('click', () => {
      const target = document.querySelector(navToggler.getAttribute('data-bs-target'));
      if (target) target.classList.toggle('show');
    });
  }
  
  if (sidebarToggler) {
    sidebarToggler.addEventListener('click', () => {
      document.querySelector('#sidebar').classList.toggle('active');
      document.querySelector('#content').classList.toggle('active');
    });
  }
}

/**
 * Настройка переключателя темной темы
 */
function setupDarkModeToggle() {
  const darkModeToggle = document.querySelector('#darkModeToggle');
  
  if (darkModeToggle) {
    // Проверяем сохраненный выбор пользователя
    const savedDarkMode = localStorage.getItem('darkMode') === 'true';
    
    // Устанавливаем начальное состояние
    if (savedDarkMode || window.matchMedia('(prefers-color-scheme: dark)').matches) {
      document.body.classList.add('dark-theme');
      if (darkModeToggle.type === 'checkbox') {
        darkModeToggle.checked = true;
      }
    }
    
    // Обработчик переключения
    darkModeToggle.addEventListener('click', () => {
      document.body.classList.toggle('dark-theme');
      
      // Сохраняем выбор пользователя
      localStorage.setItem('darkMode', document.body.classList.contains('dark-theme'));
      
      // Анимация переключения
      const themeSwitch = document.createElement('div');
      themeSwitch.className = 'theme-switch-overlay';
      document.body.appendChild(themeSwitch);
      
      setTimeout(() => {
        themeSwitch.style.opacity = '0';
        setTimeout(() => themeSwitch.remove(), 500);
      }, 100);
    });
  }
}

/**
 * Функции форматирования и обработки данных
 */

/**
 * Форматирует дату и время в локализованный вид
 * @param {string|Date} dateStr - Дата для форматирования
 * @param {object} options - Параметры форматирования
 * @returns {string} Отформатированная дата
 */
function formatDateTime(dateStr, options = {}) {
  const date = new Date(dateStr);
  const defaultOptions = { 
    year: 'numeric', 
    month: 'short', 
    day: 'numeric', 
    hour: '2-digit', 
    minute: '2-digit'
  };
  
  // Проверка на валидность даты
  if (isNaN(date.getTime())) return 'Недействительная дата';
  
  try {
    return date.toLocaleString('ru-RU', { ...defaultOptions, ...options });
  } catch (e) {
    console.error('Ошибка форматирования даты:', e);
    return date.toLocaleString();
  }
}

/**
 * Форматирует только дату
 * @param {string|Date} dateStr - Дата для форматирования
 * @returns {string} Отформатированная дата
 */
function formatDate(dateStr) {
  return formatDateTime(dateStr, { hour: undefined, minute: undefined });
}

/**
 * Обрезает текст до указанной длины
 * @param {string} text - Исходный текст
 * @param {number} maxLength - Максимальная длина
 * @param {string} suffix - Окончание для обрезанного текста
 * @returns {string} Обрезанный текст
 */
function truncateText(text, maxLength, suffix = '...') {
  if (!text) return '';
  if (text.length <= maxLength) return text;
  
  // Обрезаем до пробела, чтобы не разрывать слова
  let truncated = text.substring(0, maxLength);
  const lastSpace = truncated.lastIndexOf(' ');
  
  if (lastSpace > maxLength * 0.8) {
    truncated = truncated.substring(0, lastSpace);
  }
  
  return truncated + suffix;
}

/**
 * Форматирует число с разделителями групп разрядов
 * @param {number} number - Число для форматирования
 * @returns {string} Отформатированное число
 */
function formatNumber(number) {
  return new Intl.NumberFormat('ru-RU').format(number);
}

/**
 * Загружает данные с сервера
 * @param {string} url - URL для запроса
 * @param {object} options - Опции запроса
 * @returns {Promise} Промис с результатом запроса
 */
async function fetchData(url, options = {}) {
  try {
    const response = await fetch(url, options);
    
    if (!response.ok) {
      throw new Error(`Ошибка запроса: ${response.status} ${response.statusText}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Ошибка получения данных:', error);
    showNotification('Ошибка загрузки данных. Пожалуйста, попробуйте позже.', 'error');
    throw error;
  }
}

/**
 * Показывает уведомление пользователю
 * @param {string} message - Текст уведомления
 * @param {string} type - Тип уведомления (success, error, warning, info)
 * @param {number} duration - Длительность показа в миллисекундах
 */
function showNotification(message, type = 'info', duration = 5000) {
  // Получаем или создаем контейнер для уведомлений
  let container = document.querySelector('.notification-container');
  
  if (!container) {
    container = document.createElement('div');
    container.className = 'notification-container';
    document.body.appendChild(container);
  }
  
  // Создаем уведомление
  const notification = document.createElement('div');
  notification.className = `notification notification-${type} fade-in`;
  
  // Добавляем соответствующую иконку
  let icon = 'info-circle';
  if (type === 'success') icon = 'check-circle';
  if (type === 'error') icon = 'exclamation-circle';
  if (type === 'warning') icon = 'exclamation-triangle';
  
  notification.innerHTML = `
    <div class="notification-icon">
      <i class="fas fa-${icon}"></i>
    </div>
    <div class="notification-content">${message}</div>
    <button class="notification-close">
      <i class="fas fa-times"></i>
    </button>
  `;
  
  // Добавляем в контейнер
  container.appendChild(notification);
  
  // Настраиваем закрытие
  const closeButton = notification.querySelector('.notification-close');
  closeButton.addEventListener('click', () => dismissNotification(notification));
  
  // Автоматическое скрытие
  if (duration > 0) {
    setTimeout(() => dismissNotification(notification), duration);
  }
  
  return notification;
}

/**
 * Скрывает уведомление
 * @param {HTMLElement} notification - Элемент уведомления
 */
function dismissNotification(notification) {
  notification.classList.add('fade-out');
  
  setTimeout(() => {
    notification.style.height = notification.offsetHeight + 'px';
    notification.style.marginTop = '0';
    notification.style.marginBottom = '0';
    
    setTimeout(() => {
      notification.style.height = '0';
      notification.style.marginTop = '0';
      notification.style.marginBottom = '0';
      notification.style.padding = '0';
      
      setTimeout(() => notification.remove(), 300);
    }, 10);
  }, 300);
}

/**
 * Экспорт утилит, доступных глобально
 */
window.TeleNewsUtils = {
  formatDateTime,
  formatDate,
  formatNumber,
  truncateText,
  fetchData,
  showNotification
};
