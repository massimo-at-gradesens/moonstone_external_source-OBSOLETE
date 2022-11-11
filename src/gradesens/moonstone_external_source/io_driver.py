"""
GradeSens - External Source package - IO driver

Abstract class specifying the interface for IO operations, e.g. to retrieve
configuration data structures from a DB.
The actual implementation is to be provided by application-specific
integrations.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import abc

from .configuration import (
    AuthenticationContext,
    CommonConfiguration,
    MachineConfiguration,
)


class IODriver(abc.ABC):
    """
    Abstract class specifying the interface for IO operations, e.g. to retrieve
    configuration data structures from a DB.
    The actual implementation is to be provided by application-specific
    integrations.
    """

    @abc.abstractmethod
    async def load_authentication_context(
        self, identifier: AuthenticationContext.Identifier
    ) -> AuthenticationContext:
        """
        The actual load method, to be implemented by derived classes.
        """

    @abc.abstractmethod
    async def load_common_configuration(
        self, identifier: CommonConfiguration.Identifier
    ) -> CommonConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """

    @abc.abstractmethod
    async def load_machine_configuration(
        self, identifier: MachineConfiguration.Identifier
    ) -> MachineConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """
