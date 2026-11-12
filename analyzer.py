import hashlib
import logging
import math
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger(__name__)


shared_counter = None
shared_lock = None


def init_worker(counter, lock):
    global shared_counter, shared_lock

    shared_counter = counter
    shared_lock = lock


def analyze_post(post: dict[str, str | int]) -> dict[str, str | int | float] | None:
    try:
        logger.info("Обрабатываю пост %s в процессе %s", post["id"], os.getpid())
        title = post["title"]
        body = post["body"]

        complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(body)
        text_str = (title + body).encode("utf-8")
        text_hash = hashlib.sha256(text_str).hexdigest()
        result = post.copy()
        result["complexity_score"] = complexity_score
        result["text_hash"] = text_hash

        with shared_lock:
            shared_counter.value += 1

        return result
    except (KeyError, TypeError, ValueError) as e:
        logger.error("Ошибка обработки поста %s: %s", post.get("id", "unknown"), e)
        return None


def analyze_all_posts(posts: list[dict[str, str | int]]) -> tuple[list[dict[str, str | int | float]], int]:
    ctx = multiprocessing.get_context("spawn")
    counter = ctx.Value("i", 0)
    lock = ctx.Lock()
    queue = ctx.Queue()
    monitor_process = ctx.Process(target=monitor, args=(queue,))
    monitor_process.start()
    try:
        with ProcessPoolExecutor(
            initializer=init_worker, initargs=(counter, lock), mp_context=ctx) as executor:
            result = []

            for post in executor.map(analyze_post, posts):
                if post is not None:
                    queue.put(post["id"])
                    result.append(post)

        logger.info("Успешно обработано: %s", counter.value)
        return result, counter.value

    finally:
        queue.put(None)
        monitor_process.join()
        queue.close()
        queue.join_thread()


def monitor(queue: multiprocessing.Queue) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    while True:
        post_id = queue.get()

        if post_id is None:
            break

        logger.info("[Monitor] Пост %s обработан", post_id)


if __name__ == "__main__":

    test_posts = [
        {"id": 1, "title": "Python", "body": "Learning multiprocessing"},
        {"id": 2, "title": "Broken"},
        {"id": 3, "title": "Threading", "body": "Learning threads"},
    ]
    results = analyze_all_posts(test_posts)

    for post in results:
        print(post)
