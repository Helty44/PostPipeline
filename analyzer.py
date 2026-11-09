import math
import hashlib
from concurrent.futures import ProcessPoolExecutor
import os
import multiprocessing



shared_counter = None
shared_lock = None


def init_worker(counter, lock):
    global shared_counter, shared_lock

    shared_counter = counter
    shared_lock = lock


def analyze_post(post):
    print(f"Обрабатываю пост {post['id']} в процессе {os.getpid()}")
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


def analyze_all_posts(posts):
    ctx = multiprocessing.get_context("spawn")
    counter = ctx.Value("i", 0)
    lock = ctx.Lock()
    queue = ctx.Queue()
    monitor_process = ctx.Process(
        target=monitor,
        args=(queue,)
    )
    monitor_process.start()
    try:
        with ProcessPoolExecutor(
                initializer=init_worker,
                initargs=(counter, lock),
                mp_context=ctx
        ) as executor:
            result = list(executor.map(analyze_post, posts))

        for post in result:
            queue.put(post["id"])

        print(f"Успешно обработано: {counter.value}")
        return result

    finally:
        queue.put(None)
        monitor_process.join()
        queue.close()
        queue.join_thread()



def monitor(queue):
    while True:
        post_id = queue.get()

        if post_id is None:
            break

        print(f"[Monitor] Пост {post_id} обработан")


if __name__ == "__main__":
    test_posts = [
        {"id": 1, "title": "Python", "body": "Learning multiprocessing"},
        {"id": 2, "title": "Asyncio", "body": "Learning concurrency"},
        {"id": 3, "title": "Threading", "body": "Learning threads"},
    ]
    results = analyze_all_posts(test_posts)

    for post in results:
        print(post)