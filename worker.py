import asyncio
from argparse import ArgumentParser

from aio_pika import connect_robust

from patterns import RetryableMaster


parser = ArgumentParser()
parser.add_argument(
    '--publisher-confirms', action='store_true', default=False
)


async def worker(*, task_id):
    """
    Always failing job
    """
    await asyncio.sleep(1)
    raise Exception('some error %r' % task_id)


async def main(publisher_confirms: bool):
    connection = await connect_robust("amqp://guest:guest@127.0.0.1/")
    # Use publisher_confirms OR transaction
    channel = await connection.channel(
        publisher_confirms=publisher_confirms
    )
    master = RetryableMaster(
        channel,
        publisher_confirms=publisher_confirms
    )
    await master.create_worker(
        "my_task_name", worker,
        durable=True
    )
    return connection


if __name__ == "__main__":
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(main(args.publisher_confirms))
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())
