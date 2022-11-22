"""
GradeSens - External Source package - Configuration support

This module provides the configuration data classes to handle generic HTTP
transactions and parse their responses into higher abstraction results,
according to user specified rules.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from typing import TYPE_CHECKING, Any, Dict, Optional, Union

if TYPE_CHECKING:
    from .io_manager import IOManager

from .error import ConfigurationError, Error
from .settings import Settings


class HTTPRequestSettings(Settings):
    """
    Configuration settings for a generic HTTP request.

    These configuration settings are used by
    :class:`_MeasurementSettings` and :class:`_AuthenticationSettings`
    """

    def __init__(
        self,
        other: Optional["HTTPRequestSettings"] = None,
        /,
        *,
        url: str = None,
        headers: Optional[Settings.InputType] = None,
        query_string: Optional[Settings.InputType] = None,
        data: Optional[Union[str, bytes]] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert url is None
            assert headers is None
            assert query_string is None
            assert data is None
            assert not kwargs
            super().__init__(other)
            return

        query_string = {} if query_string is None else query_string
        headers = {} if headers is None else headers

        super().__init__(
            url=url,
            headers=headers,
            query_string=query_string,
            data=data,
            **kwargs,
        )


class HTTPResultSettings(Settings):
    """
    Configuration settings for generic HTTP response processing to produce
    target results.

    Basically this a specialization of :class:`Settings` where the
    :class:`Processor`-based value processing is not performed immediately
    within :meth:`Settings.interpolate` calls, but rather it is deferred to a
    second phase, where the results within an HTTP response are passed as
    interpolation parameters. This way, the eventual output data can be
    computed from the HTTP response.

    .. note:
        The preliminary string interpolation (performed upon calls
        :meth:`Settings.interpolate` as described in :class:`Processor`) is
        always even over the fields of this class :class:`HTTPResultSettings`.

        Only the :class:`Processor`-based value processing is deferred.
        Therefore it is possible to interpolate configuration values into the
        fields of :class:`HTTPResultSettings`, in the first

    .. seealso::
        Preliminary string interpolation of :class:`Processor` fields.
    """

    RawResultValueType = Any
    RawResultType = Dict[str, Union[RawResultValueType, "RawResultType"]]
    ResultValueType = Any
    ResultType = Dict[str, ResultValueType]

    def __init__(
        self, other: Optional["HTTPResultSettings"] = None, /, **kwargs
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(
            # Set _interpolation_settings.interpolate to False to make sure
            # interpolation is only applied in a second phase, using HTTP
            # response data as interpolation parameters
            _interpolation_settings=Settings.InterpolationSettings(
                interpolate=False,
            ),
            **kwargs,
        )


class HTTPTransactionMeta(type):
    def __new__(
        cls,
        name,
        bases,
        kwargs,
        request_type: type(HTTPRequestSettings) = HTTPRequestSettings,
        result_type: type(HTTPResultSettings) = HTTPResultSettings,
    ):
        assert issubclass(request_type, HTTPRequestSettings)
        assert issubclass(result_type, HTTPResultSettings)
        kwargs.update(
            HTTPRequestSettings=request_type,
            HTTPResultSettings=result_type,
        )
        return super().__new__(cls, name, bases, kwargs)


class HTTPTransactionSettings(
    Settings,
    metaclass=HTTPTransactionMeta,
):
    # Initialized by metaclass
    HTTPRequestSettings = None
    HTTPResultSettings = None

    def __init__(
        self,
        other: Optional["HTTPTransactionSettings"] = None,
        /,
        *,
        request: Optional[HTTPRequestSettings] = None,
        result: Optional[HTTPResultSettings] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert request is None
            assert result is None
            assert not kwargs
            super().__init__(other)
            return

        # Force a non-copy creation, so as to instantiate the field-specific
        # settings classes, required for proper result parsing.
        # Furthermore, always force the creation of request and result settings
        # object, even if empty, to make sure any settings-specific features
        # are properly set, such as the _interpolation_settings field of
        # _MeasurementResultSettings
        if request is None:
            request = {}
        try:
            request = self.HTTPRequestSettings(**request)
        except Error as err:
            err.index.insert(0, "request")
            raise

        if result is None:
            result = {}
        try:
            result = self.HTTPResultSettings(**result)
        except Error as err:
            err.index.insert(0, "result")
            raise

        super().__init__(
            request=request,
            result=result,
            **kwargs,
        )

    class InterpolatedSettings(dict):
        async def fetch_result(
            self,
            io_manager: "IOManager",
        ) -> "HTTPResultSettings.ResultType":
            """
            Complete fetch-result cycle:
            * Execute the HTTP transaction specified in ``self["request"]``,
            * Parse the corresponding response according to the rules in
               ``self["result"]`` - see also :meth:`.process_result`
            * Return the parsed data
            """
            request = self["request"]
            request_kwargs = {}

            for key in (
                "url",
                "data",
                "query_string",
                "headers",
            ):
                value = request.get(key, None)
                if value is None:
                    continue
                request_kwargs[key] = value
            if "url" not in request_kwargs:
                raise ConfigurationError(
                    "No URL specified, the request cannot be carried out"
                )
            raw_result = await io_manager.backend_driver.get_raw_result(
                **request_kwargs
            )

            return self.process_result(raw_result.data)

        def process_result(
            self, raw_result: "HTTPResultSettings.RawResultType"
        ) -> "HTTPResultSettings.ResultType":
            """
            Parse a request's raw result, i.e. an HTTP response, according to
            the rules in ``self["result"]``
            """

            parameters = dict(self)
            parameters.update(raw_result)
            interpolation_context = Settings.InterpolationContext(
                parameters=parameters
            )
            return Settings.interpolate_dict(
                self["result"],
                context=interpolation_context,
            )

    def interpolate(
        self, *args, **kwargs
    ) -> ("HTTPTransactionSettings.InterpolatedSettings"):
        return self.InterpolatedSettings(
            self.interpolated_items(*args, **kwargs)
        )

    async def fetch_result(
        self, io_manager: "IOManager", **kwargs
    ) -> "HTTPResultSettings.ResultType":
        """
        A wrapper around :meth:`.InterpolatedSettings.fetch_result`, that
        first interpolates self's settings and the executes
        :meth:`.InterpolatedSettings.fetch_result` on them,
        """

        settings = await self.get_settings(io_manager=io_manager, **kwargs)
        return await settings.fetch_result(io_manager=io_manager)
