import asyncio
import json
import logging

import aiohttp

logger = logging.getLogger(__name__)


async def fetch_post(
    session: aiohttp.ClientSession,
    post_id: int,
    semaphore: asyncio.Semaphore,
) -> dict[str, str | int] | None:
    url = f"https://jsonplaceholder.typicode.com/posts/{post_id}"
    async with semaphore:
        task = asyncio.create_task(log_fetch_post(post_id))
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                return data
        except asyncio.TimeoutError:
            logger.warning("Превышено время ожидания поста %s", post_id)
            return None
        except aiohttp.ClientError as e:
            logger.warning("Ошибка загрузки поста %s: %s", post_id, e)
            return None
        except json.JSONDecodeError as e:
            logger.warning("Ошибка JSON при загрузке поста %s: %s", post_id, e)
            return None
        finally:
            await task


async def fetch_all_posts() -> list[dict[str, str | int] | None]:
    semaphore = asyncio.Semaphore(5)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
        tasks = [fetch_post(session, post_id, semaphore) for post_id in range(1, 101)]

        results = await asyncio.gather(*tasks)
        return results


async def log_fetch_post(post_id: int) -> None:
    logger.info("Начало загрузки поста %s", post_id)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    posts = asyncio.run(fetch_all_posts())
    failed_ids = [
        post_id for post_id, post in enumerate(posts, start=1) if post is None
    ]

    logger.info("Неудачные запросы: %s", failed_ids)

    successful = sum(post is not None for post in posts)

    logger.info("Количество успешно загруженных постов %s", successful)
    logger.info("Получено результатов: %s", len(posts))
    logger.info("Первый пост: %s", posts[0])
