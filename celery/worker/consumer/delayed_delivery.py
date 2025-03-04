"""Worker Native Delayed Delivery Bootstep.

This module enables native delayed delivery support for RabbitMQ quorum queues,
allowing ETA/countdown tasks to be handled by the broker rather than held
in memory by the worker process.
"""
from kombu.transport.native_delayed_delivery import (
    bind_queue_to_native_delayed_delivery_exchange,
    declare_native_delayed_delivery_exchanges_and_queues,
)

from celery import Celery, bootsteps
from celery.utils.log import get_logger
from celery.utils.quorum_queues import detect_quorum_queues
from celery.worker.consumer import Consumer, Tasks

__all__ = ('DelayedDelivery',)

logger = get_logger(__name__)
debug, info = logger.debug, logger.info


class DelayedDelivery(bootsteps.StartStopStep):
    """Bootstep that configures native delayed delivery for quorum queues.

    This component declares the necessary exchanges and queues for native
    delayed delivery and binds all worker queues to them. When enabled,
    tasks with ETA/countdown will be delivered by the broker itself
    rather than being held in worker memory.

    Note:
        This bootstep is only active when quorum queues are detected.
        Direct exchanges are not supported by native delayed delivery.
    """

    requires = (Tasks,)

    def include_if(self, c):
        """Decide if this bootstep should be included.

        Only include this bootstep if quorum queues are detected in the broker.

        Arguments:
            c (Consumer): Consumer instance.

        Returns:
            bool: True if quorum queues are detected, False otherwise.
        """
        return detect_quorum_queues(
            c.app, c.app.connection_for_write().transport.driver_type)[0]

    def start(self, c: Consumer):
        """Set up native delayed delivery for quorum queues.

        This configures the necessary exchanges and bindings to enable
        broker-based delivery of delayed tasks.

        Arguments:
            c (Consumer): Consumer instance.
        """
        info('setting up native delayed delivery for quorum queues')
        self._setup_delayed_delivery(c)
        info('native delayed delivery setup completed')

    def _setup_delayed_delivery(self, c: Consumer):
        """Set up delayed delivery exchanges and queues for each broker.

        For each broker in a multi-broker configuration, this method:
        1. Creates a connection
        2. Declares the delayed delivery exchanges and queues
        3. Binds the worker queues to these exchanges

        Error handling ensures failures with one broker don't affect others.

        Arguments:
            c (Consumer): Consumer instance.
        """
        app: Celery = c.app

        for broker_url in app.conf.broker_url.split(';'):
            try:
                debug('configuring delayed delivery for broker: %s', broker_url)
                # We use connection for write directly to avoid using
                # ensure_connection()
                connection = c.app.connection_for_write(url=broker_url)

                # Declare delayed delivery exchanges and queues
                declare_native_delayed_delivery_exchanges_and_queues(
                    connection,
                    app.conf.broker_native_delayed_delivery_queue_type
                )

                # Bind each queue to its delayed delivery exchange
                self._bind_queues(connection, app.amqp.queues.values())

            except ConnectionRefusedError:
                # We may receive this error if a fail-over occurs
                logger.warning(
                    'connection refused when setting up delayed delivery '
                    'for broker: %s', broker_url)
            except Exception as exc:
                logger.exception(
                    'error setting up delayed delivery for broker %s: %r',
                    broker_url, exc)

    def _bind_queues(self, connection, queues):
        """Bind queues to their delayed delivery exchanges.

        For each worker queue, creates a binding to the corresponding
        delayed delivery exchange.

        Arguments:
            connection: The broker connection to use.
            queues: Collection of queue instances to bind.
        """
        for queue in queues:
            debug('binding queue %s to delayed delivery exchange', queue.name)
            bind_queue_to_native_delayed_delivery_exchange(
                connection, queue)
