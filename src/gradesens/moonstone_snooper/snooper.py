"""
GradeSens - Moonstone Snooper package - Main module

This module provides class :class:`SnooperSession`, which is the main entry
point of the Moonstone Snooper package, along with :class:`IOManager` that is
required to create a :class:`SnooperSession`.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from datetime import datetime
from typing import Iterable

from .configuration import MachineConfiguration
from .io_manager import IOManager


class SnooperSession:
    """
    Retrieve measurement data from one or more external sources via concurrent
    requests.
    """

    def __init__(
        self,
        client_session: "IOManager.ClientSession",
    ):
        self.client_session = client_session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        pass

    async def get_data(
        self,
        *,
        machine_id: MachineConfiguration.Id,
        timestamps: Iterable[datetime],
    ) -> MachineConfiguration.Results:
        machine_configuration = (
            await self.client_session.machine_configurations.get(machine_id)
        )

        return await machine_configuration.fetch_result(
            client_session=self.client_session,
            timestamps=timestamps,
        )
