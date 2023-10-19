"""POC how sync user functions can be executed from a async l1q session."""
import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from labone.core import (
    AnnotatedValue,
    DeviceKernelInfo,
    KernelSession,
    ServerInfo,
)
from labone.sync_wrapper.session import KernelSession as SyncKernelSession


class SyncSessionGuard:
    def __init__(self, kernel_info, server_info) -> None:
        self._kernel_info = kernel_info
        self._server_info = server_info
        self._async_thread_id = threading.get_ident()
        self._executor = ThreadPoolExecutor(max_workers=1).__enter__()
        self._sync_thread_id = None
        self._sync_core_session = None

    def __del__(self) -> None:
        self._executor.__exit__(None, None, None)

    @property
    def sync_executor(self) -> ThreadPoolExecutor:
        return self._executor

    @property
    def session(self) -> SyncKernelSession:
        logging.info("get sync session")
        if not self._sync_core_session:
            logging.info("create sync session")
            self._sync_core_session = SyncKernelSession(
                kernel_info=self._kernel_info,
                server_info=self._server_info,
            )
            self._sync_thread_id = threading.get_ident()
            assert threading.get_ident() != self._async_thread_id
        assert threading.get_ident() == self._sync_thread_id
        return self._sync_core_session


class L1QSession:
    def __init__(self, core_session: KernelSession) -> None:
        self._core_session = core_session
        self._sync_session_guard = SyncSessionGuard(
            kernel_info=self.core_session.kernel_info,
            server_info=self.core_session.server_info,
        )
        self.result = None

    async def create(dev_id):
        logging.info("create async session")
        session = await KernelSession.create(
            kernel_info=DeviceKernelInfo(
                device_id=dev_id,
                interface=DeviceKernelInfo.DeviceInterface.GbE,
            ),
            server_info=ServerInfo(host="localhost", port=8004),
        )
        return L1QSession(session)

    @property
    def sync_executor(self):
        return self._sync_session_guard.sync_executor

    @property
    def core_session(self):
        return self._core_session

    @property
    def sync_core_session(self):
        return self._sync_session_guard.session


def user_function(session: L1QSession, dev_id: str) -> None:
    """Dummy user function.

    This can be anything ...
    """
    logging.info(f"user_function 1: starting (result = {session.result})")
    time.sleep(0.5)
    logging.info(
        f"user_function 1: list nodes {session.sync_core_session.list_nodes(f'{dev_id}/hwmock')}",
    )
    time.sleep(0.5)
    logging.info(
        f"user_function 1: fetched {len(session.sync_core_session.get_with_expression(f'{dev_id}/hwmock/demods/0'))} values",
    )
    logging.info(f"user_function 1: finished (result = {session.result})")


def user_function2(session: L1QSession, dev_id: str) -> None:
    logging.info(f"user_function 2: starting (result = {session.result})")
    time.sleep(1)
    logging.info(f"user_function 2: finished (result = {session.result})")


async def poll_loop(session: L1QSession, dev_id: str, rate: int = 10) -> None:
    """Dummy poll demod data from the hwmock forever.

    This functions sets up a poll loop for the demodulator 0 sample node.
    Every time it receives data it puts the current time into the session result
    """
    await session.core_session.set(
        AnnotatedValue(path=f"/{dev_id}/hwmock/demods/0/enable", value=1),
    )
    await session.core_session.set(
        AnnotatedValue(path=f"/{dev_id}/hwmock/demods/0/rate", value=rate),
    )
    queue = await session.core_session.subscribe(f"/{dev_id}/demods/0/sample")
    while True:
        await queue.get()
        session.result = time.time()
        logging.info(f"Poll: result = {session.result}")


async def async_laboneq():
    """Dummy LabOneQ async main function.

    Demonstrates how laboneq can be async and still allow sync user functions.

    The approach used is the following:
        * Two separate sessions are used for the async part and the sync part.
          Both are stored within the L1QSession.
        * The the sync session is only accessed within a dedicated thread.
          This guarantees that the async event loop in the main thread is not
          affected by the sync blocking
        * The main thread calls the sync part through the `run_in_executor`
          function. This allows the async part to wait for the the sync part
          being finished without blocking the event loop.
        * Thanks to the GIL there is no need to align the threads and both the
          async part and the sync part can access data on the l1q session.
    """
    logging.info("laboneq: start")
    dev_id = "DEV90014"
    session = await L1QSession.create(dev_id)

    # Start a poll loop to demonstrate that the async session is still working.
    task = asyncio.create_task(poll_loop(session=session, dev_id=dev_id))

    await asyncio.get_event_loop().run_in_executor(
        session.sync_executor,
        user_function,
        session,
        dev_id,
    )
    logging.info(f"laboneq going to sleep (result = {session.result})")
    await asyncio.sleep(0.5)
    logging.info(f"laboneq waking up (result = {session.result})")
    await asyncio.get_event_loop().run_in_executor(
        session.sync_executor,
        user_function2,
        session,
        dev_id,
    )

    task.cancel()
    logging.info(f"laboneq: finished (result = {session.result})")


if __name__ == "__main__":
    logging.basicConfig(format="%(thread)d: %(message)s", level=logging.INFO)
    asyncio.run(async_laboneq())
