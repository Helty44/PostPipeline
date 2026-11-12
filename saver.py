import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from time import sleep

logger = logging.getLogger(__name__)
file_lock = threading.Lock()


def progress_monitor(
    stop_event: threading.Event, saved_counter: list[int], total: int
) -> None:
    while not stop_event.wait(2):
        logger.info(">>> Прогресс сохранения: %s / %s", saved_counter[0], total)


def save_all_posts(posts: list[dict[str, str | int | float]]) -> int:
    post_queue: Queue[dict[str, str | int | float] | None] = Queue()

    for post in posts:
        post_queue.put(post)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        saved_counter = [0]

        counter_lock = threading.Lock()

        stop_event = threading.Event()

        monitor_thread = threading.Thread(
            target=progress_monitor, args=(stop_event, saved_counter, len(posts))
        )

        monitor_thread.start()

        try:
            for _ in range(3):
                future = executor.submit(
                    save_worker, post_queue, saved_counter, counter_lock
                )
                futures.append(future)
            for _ in range(3):
                post_queue.put(None)

            for future in futures:
                future.result()

        finally:
            stop_event.set()
            monitor_thread.join()
        logger.info("Успешно сохранено: %s", saved_counter[0])

        return saved_counter[0]


def save_worker(
    post_queue: Queue[dict[str, str | int | float] | None],
    saved_counter: list[int],
    counter_lock: threading.Lock,
) -> None:
    while True:
        post = post_queue.get()

        if post is None:
            post_queue.task_done()
            break

        post_id = post.get("id", "unknown") if isinstance(post, dict) else "unknown"

        try:
            save_post(post)

            with counter_lock:
                saved_counter[0] += 1

        except OSError as e:
            logger.warning("Ошибка сохранения поста %s: %s", post_id, e)

        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Некорректные данные поста %s: %s", post_id, e)

        except Exception:
            logger.exception("Неожиданная ошибка сохранения поста %s", post_id)

        finally:
            post_queue.task_done()


def save_post(post_data: dict[str, str | int | float]) -> None:
    sleep(0.05)
    line = (
        f"Post ID: {post_data['id']} | "
        f"Complexity: {post_data['complexity_score']:.4f} | "
        f"Hash: {post_data['text_hash']}\n"
    )
    with file_lock, open("results.log", "a", encoding="utf-8") as f:
        f.write(line)


if __name__ == "__main__":
    test_posts = [
        {"id": 1, "complexity_score": 5.4, "text_hash": "abc123"},
        {"title": "Broken", "complexity_score": 3.2, "text_hash": "xyz"},
        {"id": 3, "complexity_score": 2.7, "text_hash": "def456"},
    ]

    print(save_all_posts(test_posts))
