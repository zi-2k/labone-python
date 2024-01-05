import json

import pytest
from labone.core.connection_layer import (
    HELLO_MSG_FIXED_LENGTH,
    MIN_ORCHESTRATOR_CAPABILITY_VERSION,
    TESTED_ORCHESTRATOR_CAPABILITY_VERSION,
    DeviceKernelInfo,
    ServerInfo,
    ZIKernelInfo,
    _open_socket,
    create_session_client_stream,
)
from labone.core.kernel_session import KernelSession
from packaging import version

HWMOCK_SERIAL = "dev90037"
UHF_SERIAL = "dev2750"

# test correct length??

@pytest.mark.regression()
def test_hello_msg_decodable():
    sock = _open_socket(ServerInfo(host="localhost", port=8004))
    raw_hello_msg = sock.recv(HELLO_MSG_FIXED_LENGTH).rstrip(b"\x00")
    json.loads(raw_hello_msg)  # no exception


@pytest.mark.regression()
def test_hello_msg_version():
    """Version in received hello messages is equal to the assumed one.
    If not, we need to manually make sure everything still workes with
    the newer version."""
    sock = _open_socket(ServerInfo(host="localhost", port=8004))
    raw_hello_msg = sock.recv(HELLO_MSG_FIXED_LENGTH).rstrip(b"\x00")
    hello_msg = json.loads(raw_hello_msg)
    assert (
        MIN_ORCHESTRATOR_CAPABILITY_VERSION
        <= version.Version(hello_msg["schema"])
        <= TESTED_ORCHESTRATOR_CAPABILITY_VERSION
    )


@pytest.mark.regression()
@pytest.mark.asyncio()
async def test_http_upgrade_zi():
    await KernelSession.create(
        server_info=ServerInfo(host="localhost", port=8004), kernel_info=ZIKernelInfo()
    )


@pytest.mark.regression()
@pytest.mark.asyncio()
async def test_http_upgrade_hpk():
    await KernelSession.create(
        server_info=ServerInfo(host="localhost", port=8004),
        # need connected HPk device here
        kernel_info=DeviceKernelInfo(device_id=HWMOCK_SERIAL),
    )


@pytest.mark.regression()
@pytest.mark.asyncio()
async def test_mdk_wrapper():
    await KernelSession.create(
        server_info=ServerInfo(host="localhost", port=8004),
        # need connected device (using mdk wrapper) here
        kernel_info=DeviceKernelInfo(device_id=UHF_SERIAL),
    )
