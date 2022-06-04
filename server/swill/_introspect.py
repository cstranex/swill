import typing as t
import typing as t
from msgspec import Struct
from ._request import _SwillRequestHandler


class RpcTypeDefinition(Struct, omit_defaults=True):
    type: str
    arguments: t.Optional[t.Dict[t.Union[str, int], "RpcTypeDefinition"]] = None


class RpcParameterDefinition(Struct):
    streams: bool
    type: RpcTypeDefinition


class IntrospectedRpc(Struct):
    """Information about the RPC"""
    name: str
    request: RpcParameterDefinition
    response: RpcParameterDefinition


def _get_type_name(type: t.Type, no_arguments=True) -> str:
    # Is this a generic type?
    if origin := t.get_origin(type):
        origin_name = _get_type_name(origin)
        if no_arguments:
            return origin_name
        arguments = [_get_type_name(arg, no_arguments) for arg in t.get_args(type)]
        return f'{origin_name}[{", ".join(arguments)}]'
    if hasattr(type, '__name__'):
        return type.__name__
    try:
        return type.__repr__()
    except TypeError:
        return 'Unknown'


def introspect_type(message_type: t.Type) -> RpcTypeDefinition:
    """Return an RpcTypeDefinition containing key -> type for the given message_type."""
    definition = RpcTypeDefinition(
        type=_get_type_name(message_type)
    )

    if not message_type:
        return definition
    if t.get_origin(message_type):
        definition.arguments = {}
        for n, argument in enumerate(t.get_args(message_type)):
            definition.arguments[n] = introspect_type(argument)
        return definition
    elif isinstance(message_type, Struct) or issubclass(message_type, Struct):
        definition.arguments = {}
        for field in message_type.__struct_fields__:
            definition.arguments[field] = introspect_type(message_type.__annotations__[field])

    return definition


def introspect_handler(name: str, request_handler: _SwillRequestHandler) -> IntrospectedRpc:
    """"Return an IntrospectedRpc message for the given request_handler"""
    return IntrospectedRpc(
        name=name,
        request=RpcParameterDefinition(
            type=introspect_type(request_handler.request_message_type),
            streams=request_handler.request_streams
        ),
        response=RpcParameterDefinition(
            type=introspect_type(request_handler.response_message_type),
            streams=request_handler.response_streams
        )
    )
