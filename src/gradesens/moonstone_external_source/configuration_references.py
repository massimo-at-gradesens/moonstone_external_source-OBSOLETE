"""
GradeSens - External Source package - Configuration support

This file provides the configuration data classes to handle machine,
maeasurement and authorization configurations.
These configurations contain all the parameters requested to query the
external measurements on the target machines.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import asyncio
from typing import TYPE_CHECKING, Callable, Iterable, Optional, Set

if TYPE_CHECKING:
    from .io_manager import IOManager

from .error import ConfigurationError
from .settings import Settings


class ConfigurationReferences(
    Settings,
):
    """
    Settings mixin to add support for referencing, resolving, and merging zero
    or more of other settings.
    """

    def __init__(
        self,
        other: Optional["ConfigurationReferences"] = None,
        /,
        *,
        _configuration_ids_field: str = None,
        _configuration_ids_get: Callable = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert _configuration_ids_field is None
            assert _configuration_ids_get is None
            assert not kwargs
            super().__init__(other)
            _configuration_ids_field = other._configuration_ids_field
            _configuration_ids_get = other._configuration_ids_get
            saved_configuration_ids_field = _configuration_ids_field

        else:
            saved_configuration_ids_field = "_" + _configuration_ids_field

            configuration_ids = kwargs.pop(_configuration_ids_field, None)
            try:
                other_configuration_ids = kwargs.pop(
                    saved_configuration_ids_field
                )
                assert configuration_ids is None
                configuration_ids = other_configuration_ids
            except KeyError:
                pass

            if configuration_ids is None:
                configuration_ids = ()
            elif isinstance(configuration_ids, Iterable) and not isinstance(
                configuration_ids, str
            ):
                configuration_ids = tuple(configuration_ids)
            else:
                configuration_ids = (configuration_ids,)

            kwargs[saved_configuration_ids_field] = configuration_ids

            super().__init__(**kwargs)

        self._configuration_ids_field = saved_configuration_ids_field
        self._configuration_ids_get = _configuration_ids_get

    async def get_aggregated_settings(
        self, client_session: "IOManager.ClientSession"
    ) -> Settings:
        result = await self._get_merged_settings(client_session)
        return type(self)(
            **result,
            # This operation is merging other possibly-partial configurations
            # and the result may not contain all the mandatory fields, as only
            # the final object built on top of this merged configuration is
            # required to have all the mandatory fields.
            # Therefore, disable the checks on mandatory fields for the
            # construction of this (possibly-partial) object.
            _partial=True,
        )

    async def _get_merged_settings(
        self,
        client_session: "IOManager.ClientSession",
        already_visited: Optional[
            Set["ConfigurationReferenceTarget.Id"]
        ] = None,
    ) -> Settings:
        """
        See details in
        :meth:`ConfigurationReferenceTarget._get_merged_settings`
        """
        configuration_ids = self[self._configuration_ids_field]
        if len(configuration_ids) == 0:
            return Settings()

        tasks = [
            self._configuration_ids_get(client_session, configuration_id)
            for configuration_id in configuration_ids
        ]
        configurations = await asyncio.gather(*tasks)

        if already_visited is None:
            already_visited = set()
        tasks = [
            configuration._get_merged_settings(
                client_session=client_session, already_visited=already_visited
            )
            for configuration in configurations
        ]
        all_settings = await asyncio.gather(*tasks)

        result = all_settings[0]
        for settings in all_settings[1:]:
            result.update(settings)

        return result


class ConfigurationReferenceTarget(ConfigurationReferences):
    """
    An configuration containing references to other instances of its same
    type, plus the support to load and merge, hierarchically, all such
    configurations in a :class:`Settings` instance.

    See :meth:`._get_merged_settings` method for details about how the settings
    from the different configurations, including this one, are merged together.
    """

    Id = str

    def __init__(
        self,
        other: Optional["ConfigurationReferenceTarget"] = None,
        /,
        *,
        id: Optional[Id] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert id is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None:
            raise ConfigurationError("Missing common configuration's 'id'")
        super().__init__(id=id, **kwargs)

    async def get_aggregated_settings(
        self, client_session: "IOManager.ClientSession"
    ) -> Settings:
        result = await self._get_merged_settings(client_session)
        return type(self)(**result)

    async def _get_merged_settings(
        self,
        client_session: "IOManager.ClientSession",
        already_visited: Optional[
            Set["ConfigurationReferenceTarget.Id"]
        ] = None,
    ) -> Settings:
        """
        Return a :class:`Settings` instance containing all the
        (non-interpolated) settings specified by this
        :class:`ConfigurationReferenceTarget`.

        The resulting :class:`Settings` are computed by merging, by means of
        :attr:`Settings.update` method, the contents of all the (optional)
        :class:`ConfigurationReferenceTarget`s referenced by
        :class:`ConfigurationReferences`, in the same order in which they are
        listed at object construction, and finally merging the contents of this
        :class:`ConfigurationReferenceTarget`.

        Therefore, the settings from the aforementioned sequence of
        :class:`ConfigurationReferenceTarget` contributors are applied in
        increasing order of precedence. E.g. the settings from last contributor
        - i.e. from this :class:`ConfigurationReferenceTarget` - have higher
        precedence over same-name settings from all the other
        :class:`ConfigurationReferenceTarget`s
        """

        configuration_id = self["id"]
        if already_visited is None:
            already_visited = {configuration_id}
        else:
            if configuration_id in already_visited:
                raise ConfigurationError(
                    "Common configuration loop"
                    f" for {configuration_id!r}."
                    " All visited common configurations"
                    f" in this loop: {already_visited}"
                )
            already_visited.add(configuration_id)

        result = await super()._get_merged_settings(
            client_session=client_session,
            already_visited=already_visited,
        )
        result.update(self)
        return result
