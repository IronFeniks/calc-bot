import time
import logging

logger = logging.getLogger(__name__)

class UserLock:
    """Класс для блокировки бота одному пользователю"""
    
    def __init__(self):
        self.current_user = None
        self.lock_time = 0
        self.username = None
        self.first_name = None
    
    def acquire(self, user_id, username=None, first_name=None):
        """Пытаемся захватить блокировку"""
        # Если блокировка свободна или истекла (10 минут бездействия)
        if self.current_user is None or (time.time() - self.lock_time) > 600:
            self.current_user = user_id
            self.lock_time = time.time()
            self.username = username
            self.first_name = first_name
            return True
        return False
    
    def release(self, user_id):
        """Освобождаем блокировку"""
        if self.current_user == user_id:
            self.current_user = None
            self.username = None
            self.first_name = None
            logger.info(f"🔓 Блокировка освобождена пользователем {user_id}")
    
    def is_locked(self):
        """Проверяет, заблокирован ли бот"""
        return self.current_user is not None
    
    def get_lock_info(self):
        """Возвращает информацию о текущем владельце блокировки"""
        if self.current_user:
            return {
                'user_id': self.current_user,
                'username': self.username,
                'first_name': self.first_name
            }
        return None
    
    def force_release(self):
        """Принудительно освобождаем блокировку"""
        self.current_user = None
        self.username = None
        self.first_name = None
        logger.info("🔓 Блокировка принудительно освобождена")

# Глобальный объект блокировки
bot_lock = UserLock()
