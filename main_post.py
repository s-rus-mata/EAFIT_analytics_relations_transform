import aiohttp
import asyncio
import json
from connection import Connection
from asyncio import Semaphore

CONCURRENCY_LIMIT = 60  # Ajusta según tu capacidad

async def post_relationship(session, url, headers, data, sem):
    async with sem:  # Control de concurrencia
        try:
            async with session.post(url, json=data, headers=headers) as response:
                if response.status in (200, 201):
                    print(f"✅ Relationship created: {data.get('id', 'N/A')}")
                else:
                    error_text = await response.text()
                    print(f"❌ Failed to create relationship {data.get('id', 'N/A')}: {response.status} - {error_text}")
        except aiohttp.ClientError as e:
            print(f"🚫 Network/client error: {e}")

async def create_relationships_from_file(file_path):
    conn = Connection()
    token = await conn.get_token()
    headers = {
        "x-okapi-token": token,
        "x-okapi-tenant": conn.tenant,
        "Content-Type": "application/json"
    }
    url = f"{conn.okapi_url}/instance-storage/instance-relationships"
    sem = Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=conn.ssl_context)) as session:
        tasks = []

        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print(f"Número de elementos: {len(lines)}")

            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    tasks.append(post_relationship(session, url, headers, data, sem))
                except json.JSONDecodeError as e:
                    print(f"⚠️ Invalid JSON on line {i}: {e}")

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(create_relationships_from_file("20250606_145034_eafit_4_out.json"))
