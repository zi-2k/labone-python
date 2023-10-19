"""This is just a poc how the sync session can be created with unsync."""
from __future__ import annotations

from typing import TYPE_CHECKING

from unsync import unsync

from labone.core.session import (
    KernelSession as AsyncKernelSession,
)
from labone.core.session import (
    ListNodesFlags,
    ListNodesInfoFlags,
    NodeInfo,
)

if TYPE_CHECKING:
    from labone.core.connection_layer import (
        KernelInfo,
        ServerInfo,
    )
    from labone.core.helper import LabOneNodePath
    from labone.core.subscription import DataQueue
    from labone.core.value import AnnotatedValue


class KernelSession:
    def __init__(
        self,
        *,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> None:
        self._async_session = unsync(AsyncKernelSession.create)(
            kernel_info=kernel_info,
            server_info=server_info,
        ).result()

    def list_nodes(
        self,
        path: LabOneNodePath,
        flags: ListNodesFlags | int = ListNodesFlags.ABSOLUTE,
    ) -> list[LabOneNodePath]:
        return unsync(self._async_session.list_nodes)(path, flags).result()

    def list_nodes_info(
        self,
        path: LabOneNodePath,
        flags: ListNodesInfoFlags | int = ListNodesInfoFlags.ALL,
    ) -> dict[LabOneNodePath, NodeInfo]:
        return unsync(self._async_session.list_nodes_info)(path, flags).result()

    def set(self, value: AnnotatedValue) -> AnnotatedValue:  # noqa: A003
        return unsync(self._async_session.set)(value).result()

    def set_with_expression(self, value: AnnotatedValue) -> list[AnnotatedValue]:
        return unsync(self._async_session.set_with_expression)(value).result()

    def get(
        self,
        path: LabOneNodePath,
    ) -> AnnotatedValue:
        return unsync(self._async_session.get)(path).result()

    def get_with_expression_promise(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags
        | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ):
        return unsync(self._async_session.get_with_expression)(path_expression, flags)

    def get_with_expression(
        self,
        path_expression: LabOneNodePath,
        flags: ListNodesFlags
        | int = ListNodesFlags.ABSOLUTE
        | ListNodesFlags.RECURSIVE
        | ListNodesFlags.LEAVES_ONLY
        | ListNodesFlags.EXCLUDE_STREAMING
        | ListNodesFlags.GET_ONLY,
    ) -> list[AnnotatedValue]:
        return self.get_with_expression_promise(path_expression, flags).result()

    async def subscribe(self, path: LabOneNodePath) -> DataQueue:
        return unsync(self._async_session.subscribe)(path).result()
