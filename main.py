import asyncio
import time

from fetcher import fetch_all_posts
from analyzer import analyze_all_posts
from saver import save_all_posts

from pathlib import Path



def run_pipeline():
    start = time.perf_counter()
    posts = asyncio.run(fetch_all_posts())

    valid_posts = [
        post for post in posts
        if post is not None
    ]
    Path("results.log").write_text("", encoding="utf-8")

    processed_posts = analyze_all_posts(valid_posts)
    saved_count = save_all_posts(processed_posts)

    time_work = time.perf_counter() - start

    print("\n=== Отчёт о работе пайплайна ===")
    print(f"Запрошено постов: {len(posts)}")
    print(f"Успешно загружено: {len(valid_posts)}")
    print(f"Успешно обработано (CPU): {len(processed_posts)}")
    print(f"Успешно сохранено (I/O): {saved_count}")
    print(f"Общее время: {time_work:.2f} сек.")


if __name__ == "__main__":
    run_pipeline()