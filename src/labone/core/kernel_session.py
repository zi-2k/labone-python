"""Module for a session to a LabOne Kernel.

A Kernel is a remote server that provides access to a defined set of nodes.
It can be a device kernel that provides access to the device nodes but it
can also be a kernel that provides additional functionality, e.g. the
Data Server (ZI) kernel.

Every Kernel provides the same capnp interface and can therefore be handled
in the same way. The only difference is the set of nodes that are available
on the kernel.

The number of sessions to a kernel is not limited. However, due to the
asynchronous interface, it is often not necessary to have multiple sessions
to the same kernel.
"""

from __future__ import annotations

import typing as t

import capnp

from labone.core.connection_layer import (
    KernelInfo,
    ServerInfo,
    create_session_client_stream,
)
from labone.core.helper import (
    ensure_capnp_event_loop,
)
from labone.core.hpk_schema import get_schema
from labone.core.reflection.capnp_dynamic_type_system import build_type_system
from labone.core.reflection.parsed_wire_schema import ParsedWireSchema
from labone.core.session import Session

if t.TYPE_CHECKING:
    from labone.core.errors import (  # noqa: F401
        BadRequestError,
        InternalError,
    )

SCHEMA = ParsedWireSchema(get_schema().theSchema)
SESSION_SCHEMA_ID = 13390403837104530780


class DummyReflection:
    """Just a Dumy for a hotfix."""

    def __init__(self):
        self._parsed_schema = SCHEMA
        build_type_system(self._parsed_schema.full_schema, self)


REFLECTION = DummyReflection()
INTERFACE = capnp.lib.capnp._InterfaceModule(  # noqa: SLF001
    SCHEMA.full_schema[SESSION_SCHEMA_ID].schema.as_interface(),
    SCHEMA.full_schema[SESSION_SCHEMA_ID].name,
)


class KernelSession(Session):
    """Session to a LabOne kernel.

    Representation of a single session to a LabOne kernel. This class
    encapsulates the capnp interaction and exposes a Python native API.
    All functions are exposed as they are implemented in the kernel
    interface and are directly forwarded to the kernel through capnp.

    Each function implements the required error handling both for the
    capnp communication and the server errors. This means unless an Exception
    is raised the call was successful.

    The KernelSession class is instantiated through the staticmethod
    `create()`.
    This is due to the fact that the instantiation is done asynchronously.
    To call the constructor directly an already existing capnp io stream
    must be provided.

    !!! note

        Due to the asynchronous interface, one needs to use the static method
        `create` instead of the `__init__` method.

    ```python
    kernel_info = ZIKernelInfo()
    server_info = ServerInfo(host="localhost", port=8004)
    kernel_session = await KernelSession(
            kernel_info = kernel_info,
            server_info = server_info,
        )
    ```

    Args:
        reflection_server: The reflection server that is used for the session.
        kernel_info: Information about the target kernel.
        server_info: Information about the target data server.
    """

    def __init__(
        self,
        connection: capnp.AsyncIoStream,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> None:
        self.client = capnp.TwoPartyClient(connection)
        capability = self.client.bootstrap().cast_as(INTERFACE)

        super().__init__(
            capability,  # type: ignore[attr-defined]
            reflection_server=REFLECTION,  # type: ignore[arg-type]
        )
        self._kernel_info = kernel_info
        self._server_info = server_info

    @staticmethod
    async def create(
        *,
        kernel_info: KernelInfo,
        server_info: ServerInfo,
    ) -> KernelSession:
        """Create a new session to a LabOne kernel.

        Since the creation of a new session happens asynchronously, this method
        is required, instead of a simple constructor (since a constructor can
        not be asynchronous).

        !!! warning

            The initial socket creation and setup (handshake, ...) is
            currently not done asynchronously! The reason is that there is not
            easy way of doing this with the current capnp implementation.

        Args:
            kernel_info: Information about the target kernel.
            server_info: Information about the target data server.

        Returns:
            A new session to the specified kernel.

        Raises:
            UnavailableError: If the kernel was not found or unable to connect.
            BadRequestError: If there is a generic problem interpreting the incoming
                request
            InternalError: If the kernel could not be launched or another internal
                error occurred.
            LabOneCoreError: If another error happens during the session creation.
        """
        sock, kernel_info_extended, server_info_extended = create_session_client_stream(
            kernel_info=kernel_info,
            server_info=server_info,
        )
        await ensure_capnp_event_loop()
        connection = await capnp.AsyncIoStream.create_connection(sock=sock)

        session = KernelSession(
            connection=connection,
            kernel_info=kernel_info_extended,
            server_info=server_info_extended,
        )
        await session.ensure_compatibility()
        return session

    @property
    def kernel_info(self) -> KernelInfo:
        """Information about the kernel."""
        return self._kernel_info

    @property
    def server_info(self) -> ServerInfo:
        """Information about the server."""
        return self._server_info
