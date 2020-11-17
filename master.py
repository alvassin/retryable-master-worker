import asyncio
from argparse import ArgumentParser

from aio_pika import connect_robust

from patterns import RetryableMaster


parser = ArgumentParser()
parser.add_argument(
    '--publisher-confirms', action='store_true', default=False
)


async def main(publisher_confirms):
    connection = await connect_robust("amqp://guest:guest@127.0.0.1/")
    async with connection:
        channel = await connection.channel(
            publisher_confirms=publisher_confirms
        )
        master = RetryableMaster(
            channel, publisher_confirms=publisher_confirms
        )
        await master.proxy.my_task_name(task_id=1)


if __name__ == "__main__":
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args.publisher_confirms))

