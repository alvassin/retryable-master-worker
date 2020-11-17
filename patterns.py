import asyncio
import logging

from aio_pika import Channel, IncomingMessage, Message
from aio_pika.patterns import Master, NackMessage, RejectMessage


log = logging.getLogger(__name__)


ATTEMPT_NUMBER = "X-Attempt-Number"


class RetryableMaster(Master):
    """
    Master/Worker pattern, that retries failing message multiple times and
    finally rejects it when all attempts are failed.
    """
    __slots__ = (
        "retries", "publisher_confirms"
    )

    def __init__(
        self,
        channel: Channel,
        retries: int = 5,
        requeue: bool = True,
        reject_on_redelivered: bool = False,
        publisher_confirms: bool = False
    ):
        if not publisher_confirms and channel.channel.publisher_confirms:
            raise ValueError(
                "Cannot use channel with publisher "
                "confirms, does not work with transactions",
            )
        super().__init__(channel, requeue, reject_on_redelivered)
        self.retries = retries
        self.publisher_confirms = publisher_confirms

    async def on_message(self, func, message: IncomingMessage):
        async with message.process(
                requeue=self._requeue,
                reject_on_redelivered=self._reject_on_redelivered,
                ignore_processed=True,
        ):
            data = self.deserialize(message.body)

            try:
                await self.execute(func, data)

            except RejectMessage as e:
                await message.reject(requeue=e.requeue)

            except NackMessage as e:
                await message.nack(requeue=e.requeue)

            except asyncio.CancelledError:
                # Do not retry cancelled tasks
                raise

            except Exception:
                await self._republish(message)
                raise

    async def _republish(self, message: IncomingMessage):
        attempt_number = message.headers.get(ATTEMPT_NUMBER, 1)

        # Message has reached max attempts and should be rejected.
        if attempt_number >= self.retries:
            log.warning(
                "Message with id %r failed max attempts %r, reject",
                message.message_id, self.retries,
            )
            return await message.reject(requeue=False)

        # Number of attempts is lower then allowed retries.
        # Requeue message with new X-Attempt-Number header value.
        log.warning(
            "Message with id %r failed attempt %r, requeue",
            message.message_id, attempt_number,
        )

        if self.publisher_confirms:
            # Without tx message is rejected successfully
            await self._republish_message(message, attempt_number)
        else:
            # Problem case
            async with self.channel.transaction():
                await self._republish_message(message, attempt_number)


    async def _republish_message(self, message, attempt_number):
        headers = {
            **message.headers,
            ATTEMPT_NUMBER: attempt_number + 1,
        }
        new_message = Message(
            body=message.body,
            headers=headers,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=message.delivery_mode,
            correlation_id=message.correlation_id,
            priority=message.priority,
            reply_to=message.reply_to,
            expiration=message.expiration,
            message_id=message.message_id,
            timestamp=message.timestamp,
            type=message.type,
            app_id=message.app_id,
            user_id=message.user_id,
        )
        await self.exchange.publish(new_message, message.routing_key,
                                    mandatory=True)
        await message.nack(requeue=False)