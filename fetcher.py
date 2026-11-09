import asyncio
import aiohttp


async def fetch_post(session, post_id, semaphore):
    url = f'https://jsonplaceholder.typicode.com/posts/{post_id}'
    async with semaphore:
        await log_fetch_post(post_id)
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                return data
        except asyncio.TimeoutError:
            print(f"Превышено время ожидания поста {post_id}")
            return None
        except aiohttp.ClientError as e:
            print(f"Ошибка загрузки поста {post_id}: {e}")
            return None

async def fetch_all_posts():
    semaphore = asyncio.Semaphore(5)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(
            timeout=timeout,
            trust_env=True
    ) as session:
        tasks = [
            fetch_post(session, post_id, semaphore)
            for post_id in range(1, 101)
        ]

        results = await asyncio.gather(*tasks)
        return results

async def log_fetch_post(post_id):
    print(f'Начало загрузки поста {post_id}')


if __name__ == "__main__":
    posts = asyncio.run(fetch_all_posts())
    failed_ids = [
        post_id
        for post_id, post in enumerate(posts, start=1)
        if post is None
    ]

    print(f"Неудачные запросы: {failed_ids}")
    successful = sum(post is not None for post in posts)
    print(f'Количество успешно загруженных постов {successful}')
    print(f'Получено результатов: {len(posts)}')
    print(posts[0])
