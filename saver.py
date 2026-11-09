from time import sleep
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor


file_lock = threading.Lock()

def progress_monitor(stop_event, saved_counter, total):
    while not stop_event.wait(2):
        print(f">>> Прогресс сохранения: {saved_counter[0]} / {total}")


def save_all_posts(posts):
    post_queue = Queue()

    for post in posts:
        post_queue.put(post)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        saved_counter = [0]

        counter_lock = threading.Lock()

        stop_event = threading.Event()

        monitor_thread = threading.Thread(
            target=progress_monitor,
            args=(stop_event, saved_counter, len(posts))
        )

        monitor_thread.start()

        try:
            for _ in range(3):
                future = executor.submit(
                    save_worker,
                    post_queue,
                    saved_counter,
                    counter_lock
                )
                futures.append(future)
            for _ in range(3):
                post_queue.put(None)
            post_queue.join()
            for future in futures:
                future.result()

        finally:
            stop_event.set()
            monitor_thread.join()
        print(f"Успешно сохранено: {saved_counter[0]}")

        return saved_counter[0]


def save_worker(post_queue, saved_counter, counter_lock):
    while True:
        post = post_queue.get()

        if post is None:
            post_queue.task_done()
            break

        try:
            save_post(post)

            with counter_lock:
                saved_counter[0] += 1
        except OSError as e:
            print(f"Ошибка сохранения поста {post['id']}: {e}")
        except (KeyError, TypeError, ValueError) as e:
            print(f"Некорректные данные поста: {e}")
        finally:
            post_queue.task_done()



def save_post(post_data):
    sleep(0.05)
    line = (
        f"Post ID: {post_data['id']} | "
        f"Complexity: {post_data['complexity_score']:.4f} | "
        f"Hash: {post_data['text_hash']}\n"
    )
    with file_lock:
        with open('results.log', 'a', encoding="utf-8") as f:
            f.write(line)