"""
Скрипт для создания структуры Data Lake в HDFS на удаленном сервере.
Создает директории согласно архитектуре из README.md.
Подключается к удаленному серверу через SSH и выполняет команды внутри Docker-контейнера.
"""

import subprocess
import logging
import os
from typing import List, Tuple

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HDFSSchemaCreator:
    """Класс для создания структуры Data Lake в HDFS на удаленном сервере."""

    def __init__(
        self,
        base_path: str = "/user/ajdaral1ev/project/geo",
        remote_host: str = "158.160.218.207",
        remote_user: str = "yc-user",
        ssh_key_path: str = "~/.ssh/ssh_private_key",
        container_name: str = None
    ):
        """
        Инициализация создателя схемы.

        Args:
            base_path: Базовый путь для проекта в HDFS
            remote_host: IP-адрес удаленного сервера
            remote_user: Пользователь на удаленном сервере
            ssh_key_path: Путь к SSH-ключу
            container_name: Имя Docker-контейнера (если None - определится автоматически)
        """
        self.base_path = base_path
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.ssh_key_path = os.path.expanduser(ssh_key_path)
        self.container_name = container_name
        self.directories = self._define_directories()

    def _define_directories(self) -> List[Tuple[str, str]]:
        """
        Определяет список директорий для создания.

        Returns:
            Список кортежей (путь, описание)
        """
        return [
            # RAW Layer
            (f"{self.base_path}/raw", "RAW Layer - сырые данные"),
            (f"{self.base_path}/raw/events", "События с координатами"),
            (f"{self.base_path}/raw/geo_csv", "Справочник городов Австралии"),

            # ODS Layer
            (f"{self.base_path}/ods", "ODS Layer - очищенные данные"),
            (f"{self.base_path}/ods/events", "Очищенные события"),
            (f"{self.base_path}/ods/geo", "Очищенные координаты городов"),

            # DDS Layer
            (f"{self.base_path}/dds", "DDS Layer - интегрированные данные"),
            (f"{self.base_path}/dds/users", "Профили пользователей с домашним регионом"),
            (f"{self.base_path}/dds/cities", "Справочник городов"),
            (f"{self.base_path}/dds/events_enriched", "События с гео-данными"),

            # Analytics/Sandbox Layer
            (f"{self.base_path}/analytics", "Analytics Layer - песочница для экспериментов"),
            (f"{self.base_path}/analytics/temp", "Временные таблицы"),
            (f"{self.base_path}/analytics/samples", "Выборки для тестирования"),

            # MART Layer
            (f"{self.base_path}/mart", "MART Layer - витрины данных"),
            (f"{self.base_path}/mart/friend_recommendations", "Рекомендации друзей"),
            (f"{self.base_path}/mart/user_geo_report", "Гео-аналитика пользователей"),
        ]

    def _get_containers_list(self) -> List[str]:
        """
        Получает список всех запущенных Docker-контейнеров.

        Returns:
            Список имен контейнеров
        """
        ssh_command = [
            'ssh',
            '-i', self.ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            f'{self.remote_user}@{self.remote_host}',
            'docker ps --format "{{.Names}}"'
        ]

        try:
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                check=False,
                timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                containers = [c.strip() for c in result.stdout.strip().split('\n') if c.strip()]
                return containers
            return []
        except Exception as e:
            logger.error(f"Ошибка получения списка контейнеров: {e}")
            return []

    def _detect_container_name(self) -> str:
        """
        Автоматически определяет имя Docker-контейнера с HDFS namenode.

        Returns:
            Имя контейнера или None, если не найден
        """
        logger.info("Получение списка запущенных контейнеров...")
        containers = self._get_containers_list()

        if not containers:
            logger.error("Не найдено запущенных контейнеров")
            return None

        logger.info(f"Найдено контейнеров: {len(containers)}")
        for container in containers:
            logger.info(f"  - {container}")

        # Ищем контейнер с ajdara1iev в имени
        for container in containers:
            if 'ajdara1iev' in container.lower():
                logger.info(f"✓ Выбран контейнер с HDFS: {container}")
                return container

        # Если не нашли ajdara1iev, ищем namenode
        for container in containers:
            if 'namenode' in container.lower():
                logger.warning(f"Контейнер с 'ajdara1iev' не найден, используется namenode: {container}")
                return container

        # Если не нашли ни того, ни другого, берем первый контейнер
        if containers:
            logger.warning(f"Контейнер с 'ajdara1iev' или 'namenode' не найден, используется первый: {containers[0]}")
            return containers[0]

        logger.error("Не удалось определить контейнер")
        return None

    def _run_remote_hdfs_command(self, hdfs_command: str) -> Tuple[bool, str]:
        """
        Выполняет команду HDFS на удаленном сервере через SSH и Docker.

        Args:
            hdfs_command: HDFS команда для выполнения

        Returns:
            Кортеж (успех, вывод)
        """
        # Определяем имя контейнера, если не задано
        if self.container_name is None:
            self.container_name = self._detect_container_name()
            if self.container_name is None:
                logger.error("Не удалось определить имя контейнера")
                return False, "Container not found"

        # Команда для выполнения HDFS команды внутри Docker-контейнера
        docker_command = f"docker exec {self.container_name} {hdfs_command}"

        # SSH команда
        ssh_command = [
            'ssh',
            '-i', self.ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            f'{self.remote_user}@{self.remote_host}',
            docker_command
        ]

        try:
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                check=False,
                timeout=30
            )
            return result.returncode == 0, result.stdout + result.stderr
        except subprocess.TimeoutExpired:
            logger.error("Таймаут выполнения команды")
            return False, "Timeout"
        except Exception as e:
            logger.error(f"Ошибка выполнения команды: {e}")
            return False, str(e)

    def _directory_exists(self, path: str) -> bool:
        """
        Проверяет существование директории в HDFS.

        Args:
            path: Путь к директории

        Returns:
            True, если директория существует
        """
        success, _ = self._run_remote_hdfs_command(f'hdfs dfs -test -d {path}')
        return success

    def create_directory(self, path: str, description: str) -> bool:
        """
        Создает директорию в HDFS.

        Args:
            path: Путь к директории
            description: Описание директории

        Returns:
            True, если директория создана успешно
        """
        if self._directory_exists(path):
            logger.info(f"Директория уже существует: {path}")
            return True

        logger.info(f"Создание директории: {path} ({description})")
        success, output = self._run_remote_hdfs_command(f'hdfs dfs -mkdir -p {path}')

        if success:
            logger.info(f"✓ Создана: {path}")
            return True
        else:
            logger.error(f"✗ Ошибка создания {path}: {output}")
            return False

    def create_schema(self) -> bool:
        """
        Создает всю структуру Data Lake.

        Returns:
            True, если все директории созданы успешно
        """
        logger.info("=" * 70)
        logger.info("Начало создания структуры Data Lake")
        logger.info(f"Базовый путь: {self.base_path}")
        logger.info("=" * 70)

        success_count = 0
        fail_count = 0

        for path, description in self.directories:
            if self.create_directory(path, description):
                success_count += 1
            else:
                fail_count += 1

        logger.info("=" * 70)
        logger.info(f"Завершено: {success_count} успешно, {fail_count} ошибок")
        logger.info("=" * 70)

        return fail_count == 0

    def verify_schema(self) -> bool:
        """
        Проверяет созданную структуру.

        Returns:
            True, если все директории существуют
        """
        logger.info("Проверка созданной структуры...")

        all_exist = True
        for path, description in self.directories:
            exists = self._directory_exists(path)
            status = "✓" if exists else "✗"
            logger.info(f"{status} {path}")

            if not exists:
                all_exist = False

        return all_exist

    def show_structure(self) -> None:
        """Отображает структуру Data Lake."""
        logger.info("\nПланируемая структура Data Lake:\n")

        for path, description in self.directories:
            level = path.replace(self.base_path, "").count("/")
            indent = "  " * (level - 1)
            folder_name = path.split("/")[-1]
            logger.info(f"{indent}├── {folder_name}/ — {description}")


def main():
    """Основная функция."""
    logger.info("Подключение к удаленному серверу yc-user@158.160.218.207")
    logger.info("Пользователь HDFS: /user/ajdaral1ev")

    # Создание экземпляра класса
    creator = HDFSSchemaCreator(
        base_path="/user/ajdaral1ev/project/geo",
        remote_host="158.160.218.207",
        remote_user="yc-user",
        ssh_key_path="~/.ssh/ssh_private_key",
        container_name=None  # Автоматическое определение
    )

    # Проверка SSH-ключа
    if not os.path.exists(creator.ssh_key_path):
        logger.error(f"SSH-ключ не найден: {creator.ssh_key_path}")
        return 1

    # Отображение планируемой структуры
    creator.show_structure()

    # Создание структуры
    success = creator.create_schema()

    # Проверка результата
    if success:
        logger.info("\n✓ Структура Data Lake успешно создана!")

        # Верификация
        if creator.verify_schema():
            logger.info("✓ Верификация пройдена успешно!")
        else:
            logger.warning("⚠ Некоторые директории не найдены")
    else:
        logger.error("✗ Ошибки при создании структуры Data Lake")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
