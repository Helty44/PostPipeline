import asyncio
import logging
import time
from pathlib import Path

from analyzer import analyze_all_posts
from fetcher import fetch_all_posts
from saver import save_all_posts

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    start = time.perf_counter()
    posts = asyncio.run(fetch_all_posts())

    valid_posts = [post for post in posts if post is not None]

    processed_posts, processed_count = analyze_all_posts(valid_posts)

    Path("results.log").write_text("", encoding="utf-8")
    saved_count = save_all_posts(processed_posts)

    time_work = time.perf_counter() - start

    logger.info("\n=== Отчёт о работе пайплайна ===")
    logger.info("Запрошено постов: %s", len(posts))
    logger.info("Успешно загружено: %s", len(valid_posts))
    logger.info("Успешно обработано (CPU): %s", processed_count)
    logger.info("Успешно сохранено (I/O): %s", saved_count)
    logger.info("Общее время: %.2f сек.", time_work)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    run_pipeline()
