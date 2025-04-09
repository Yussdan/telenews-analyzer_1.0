// Общие функции и утилиты для всего приложения
document.addEventListener('DOMContentLoaded', function() {
  console.log('Приложение инициализировано');
  
  // Скрытие уведомлений после задержки
  const alerts = document.querySelectorAll('.alert');
  alerts.forEach(alert => {
      setTimeout(() => {
          alert.style.transition = 'opacity 0.5s';
          alert.style.opacity = '0';
          setTimeout(() => {
              alert.style.display = 'none';
          }, 500);
      }, 3000);
  });
});

// Форматирование даты
function formatDateTime(dateStr) {
  const date = new Date(dateStr);
  return date.toLocaleString('ru-RU');
}

// Обрезка текста
function truncateText(text, maxLength) {
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + '...';
}
