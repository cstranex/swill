"""Validation helpers for Swill

Swill supports annotated-types
"""
import inspect
from dataclasses import dataclass
from datetime import timezone
from collections import abc
import functools
import re
import typing as t

import annotated_types as at
from msgspec import Struct

from swill._exceptions import (
    SwillValidationError,
    ValidationExceptionItem,
    ComplexValidationError,
)

Constraint = t.Union[at.BaseMetadata, slice, "re.Pattern[bytes]", "re.Pattern[str]"]

# Consider these validation errors
ValidationExceptions = (
    TypeError,
    ValueError,
    SwillValidationError,
    ComplexValidationError,
)


@t.runtime_checkable
class Validatable(t.Protocol):
    """Implement the __validate__ method that validates the data in the object
    raising one of the ValidationExceptions on error or returning None"""

    def __validate__(self) -> None:
        ...


@dataclass
class ValidatorDefinition:
    """Definition of a validator"""

    fields: t.Optional[t.Union[t.List[str], t.Tuple[str], t.Tuple[int]]]
    callback: t.Callable
    each: bool = False
    optional: bool = True
    pre: bool = False
    options: t.List[str] = ""


def _check_gt(_, value: t.Any, constraint: Constraint):
    if value < constraint.gt:
        raise ValueError(f"less than or equal to {constraint.gt}")


def _check_ge(_, value: t.Any, constraint: Constraint):
    if value <= constraint.ge:
        raise ValueError(f"less than {constraint.ge}")


def _check_lt(_, value: t.Any, constraint: Constraint):
    if value > constraint.lt:
        raise ValueError(f"greater than or equal to {constraint.lt}")


def _check_le(_, value: t.Any, constraint: Constraint):
    if value > constraint.le:
        raise ValueError(f"greater than {constraint.le}")


def _check_multiple_of(_, value: t.Any, constraint: Constraint):
    if value % constraint.multiple_of != 0:
        raise ValueError(f"greater than {constraint.le}")


def _check_predicate(_, value: t.Any, constraint: Constraint):
    if not constraint.func(value):
        raise ValueError("failed predicate")


def _check_len(_, value: t.Any, constraint: Constraint):
    if isinstance(constraint, slice):
        constraint = at.Len(constraint.start or 0, constraint.stop)

    if constraint.min_inclusive is None:
        raise TypeError
    if len(value) < constraint.min_inclusive:
        raise ValueError(f"length less than {constraint.min_inclusive}")
    if constraint.max_exclusive is not None and len(value) >= constraint.max_exclusive:
        raise ValueError(f"length greater than or equal to {constraint.max_exclusive}")


def _check_timezone(_, value: t.Any, constraint: Constraint):
    if isinstance(constraint.tz, str):
        return value.tzinfo is not None and constraint.tz == value.tzname()
    if isinstance(constraint.tz, timezone):
        return value.tzinfo is not None and value.tzinfo == constraint.tz
    if constraint.tz is None:
        return value.tzinfo is None
    # ellipsis
    return value.tzinfo is not None


def _check_regex(_, value: t.Any, constraint: re.Pattern):
    if not constraint.match(value):
        raise ValueError(f"Does not match pattern: {constraint.pattern}")


def _nested_validator(_, value: t.Any, validators: t.List[ValidatorDefinition] = None):
    """Either call the list of validators with the given value or call
    value.__validate__()"""

    index = None
    try:
        if validators:
            for validator_def in validators:
                each = validator_def.each
                callback = validator_def.callback

                if each:
                    for index, v in enumerate(value):
                        callback(v.__class__, v)
                else:
                    callback(value.__class__, value)
        else:
            if _is_eachable(type(value)):
                for index, v in enumerate(value):
                    v.__validate__()
            else:
                value.__validate__()
    except ValidationExceptions as exc:
        if index is None:
            raise exc

        exc_data = {"index": index}
        raise ComplexValidationError([exc], exc_data) from exc


def _is_eachable(typ: t.Type):
    """Return True if typ is a Sequence (eg: List, Set). For Tuple types this will
    return True *only* if the tuple is defined with Ellipsis (eg: Tuple[str, ...])

    NB: String and bytes are not considered eachable
    """
    origin = t.get_origin(typ)
    if not origin:
        return issubclass(typ, t.Sequence) and not issubclass(typ, (str, bytes))
    if origin == tuple:
        # Check if we have ellipsis or not
        return Ellipsis in t.get_args(typ)

    return issubclass(origin, t.Sequence)


def _validate_tuple(
    _, value: t.Tuple[t.Any, ...], validators=t.Sequence[ValidatorDefinition]
):
    """Call the list of validators on each item of the given tuple"""
    index = None
    try:
        for definition in validators:
            index = definition.fields[0]
            definition.callback(value.__class__, value[index])
    except ValidationExceptions as exc:
        exc_data = {"index": index}
        raise ComplexValidationError([exc], exc_data) from exc


def _validate_dict_keys(
    _, value: t.Dict[t.Any, t.Any], validators=t.Sequence[ValidatorDefinition]
):
    """Call the list of validators on each key for the given dict"""
    for definition in validators:
        for key in value.keys():
            definition.callback(value.__class__, key)


def _validate_dict_values(
    _, value: t.Dict[t.Any, t.Any], validators=t.Sequence[ValidatorDefinition]
):
    """Call the list of validators on each value for a given dict"""
    key = None
    try:
        for definition in validators:
            for key, v in value.items():
                definition.callback(value.__class__, v)
    except ValidationExceptions as exc:
        exc_data = {"key": key}
        raise ComplexValidationError([exc], exc_data) from exc


_VALIDATORS: t.Dict[t.Type[Constraint], t.Callable] = {
    at.Gt: _check_gt,
    at.Lt: _check_lt,
    at.Ge: _check_ge,
    at.Le: _check_le,
    at.MultipleOf: _check_multiple_of,
    at.Predicate: _check_predicate,
    at.Len: _check_len,
    at.Timezone: _check_timezone,
    slice: _check_len,
    re.Pattern: _check_regex,
}


def _extract_validators_from_type_annotation(
    name: t.Union[str, int], annotation: t.Type, each=False, nested=False
):
    """Creates validators from the given type annotations. Used primarily to create
    validators for nested fields that are specified with typing annotations or for
    objects that implement the __validate__() method
    """

    validators = []
    origin = t.get_origin(annotation)
    if origin is None:  # eg: str, int or something that is Validatable
        if issubclass(annotation, Validatable):
            # The target type supports __validate__() so return that.
            return [ValidatorDefinition((name,), _nested_validator)]
    elif origin == t.Annotated:
        # The type annotation is annotated meaning we probably have constraints for it
        annotated_type, *annotated_args = t.get_args(annotation)

        arg_validators = _extract_validators_from_type_annotation(name, annotated_type)
        if arg_validators:
            validators.extend(arg_validators)

        for arg in annotated_args:
            if not isinstance(arg, (at.BaseMetadata, re.Pattern, slice)):
                continue
            if validator_callable := _VALIDATORS.get(type(arg)):
                validators.append(
                    ValidatorDefinition(
                        (name,),
                        functools.partial(validator_callable, constraint=arg),
                        each,
                    )
                )
        return validators
    elif issubclass(origin, t.Mapping):
        # Validators can be applied on either the key or the value of the mapping
        # depending on where they are defined

        key_type, value_type = t.get_args(annotation)

        key_validators = list(
            _extract_validators_from_type_annotation("", key_type, each=True)
        )
        if key_validators:
            validators.append(
                ValidatorDefinition(
                    (name,),
                    functools.partial(_validate_dict_keys, validators=key_validators),
                )
            )

        value_validators = list(
            _extract_validators_from_type_annotation("", value_type, each=True)
        )
        if value_validators:
            validators.append(
                ValidatorDefinition(
                    (name,),
                    functools.partial(_validate_dict_values, validators=value_validators),
                )
            )

    elif _is_eachable(annotation):  # Iterable fields such as List, Set, Sequence
        annotated_type = t.get_args(annotation)[0]
        validators.extend(
            _extract_validators_from_type_annotation(
                name, annotated_type, each=True, nested=True
            )
        )
    elif issubclass(origin, tuple):  # Tuple types that don't have ellipsis
        tuple_args = t.get_args(annotation)
        tuple_validators = []
        for n, arg in enumerate(tuple_args):
            tuple_validators.extend(
                _extract_validators_from_type_annotation(n, arg, False)
            )

        if tuple_validators:
            validators.append(
                ValidatorDefinition(
                    (name,),
                    functools.partial(_validate_tuple, validators=tuple_validators),
                )
            )

    if nested and validators:
        return [
            ValidatorDefinition(
                (name,),
                functools.partial(_nested_validator, validators=validators),
                each,
            )
        ]

    return validators


def _is_iterable(hint: t.Any):
    if not isinstance(hint, type):
        hint = t.get_origin(hint)
    return issubclass(hint, abc.Iterable)


class ValidatedStruct(Struct):
    """ValidatedStructs allow an extra pass of validation after the message has been
    deserialized and validated against the type annotations. Using the @validator
    decorator, callable functions can be used to perform run-time constraint checks

    These validations are performed after deserialization by calling the __validate__()
    method.

    Type annotations can also be used for validation by using the `annotated-types`
    package.

    You can specify additional settings for type validations:

    class MyStruct(ValidatedStruct):
        class Meta:
            return_all_errors = True  # Return all validation errors instead of just one

    """

    __validators__: t.List[ValidatorDefinition] = []

    @classmethod
    def _add_validator(cls, definition: ValidatorDefinition):
        if not definition.fields:
            cls.__validators__.append(definition)
        else:
            hints = t.get_type_hints(cls)
            for field in definition.fields:
                if field not in cls.__slots__:
                    raise RuntimeError(f"{field} is not a valid struct field")
                if definition.each:
                    hint = hints.get(field)
                    if t.get_origin(hint) == t.Union:
                        for arg in t.get_args(hint):
                            is_none = isinstance(arg, type) and issubclass(
                                arg, type(None)
                            )
                            if not is_none and not _is_iterable(arg):
                                raise RuntimeError(
                                    f"{field} is not iterable. Cannot use `each`"
                                )
                    elif not _is_iterable(hint):
                        raise RuntimeError(f"{field} is not iterable. Cannot use `each`")

                cls.__validators__.append(definition)

    def __init_subclass__(cls, **kwargs):
        cls.__validators__ = []
        type_definitions = t.get_type_hints(cls, include_extras=True)
        for field in cls.__slots__:
            definitions = _extract_validators_from_type_annotation(
                field, type_definitions.get(field), each=False, nested=True
            )
            for definition in definitions:
                cls._add_validator(definition)

        # Get validators defined by the @validator decorator
        for name in cls.__dict__:
            if validator_def := getattr(getattr(cls, name), "__validator__", None):
                cls._add_validator(validator_def)

        # Re-order the validators
        cls.__validators__.sort(key=lambda definition: definition.pre)

    def __validate__(self):
        exceptions = []
        all_exceptions = hasattr(self, "Meta") and getattr(self.Meta, "return_all_errors")

        print("We called validate!", self, all_exceptions)

        for validator_def in self.__validators__:
            optional = validator_def.optional
            fields = validator_def.fields
            each = validator_def.each
            callback = validator_def.callback
            options = validator_def.options

            for field in fields:
                items = []
                if not field:
                    items.append(self)
                else:
                    field_item = getattr(self, field)
                    if each:
                        items.extend(field_item)
                    else:
                        items.append(field_item)

                    if optional and field_item == getattr(self.__class__, field):
                        continue

                for n, item in enumerate(items):
                    kwargs = {}
                    if "field" in options:
                        kwargs["field"] = field
                    if "model" in options:
                        kwargs["model"] = self
                    if "index" in options and each:
                        kwargs["index"] = n
                    try:
                        callback(self.__class__, item, **kwargs)
                    except (
                        ValueError,
                        TypeError,
                        ComplexValidationError,
                        SwillValidationError,
                    ) as exc:
                        exceptions.append(
                            ValidationExceptionItem(field, None if not each else n, exc)
                        )
                        if not all_exceptions:
                            break

        if exceptions:
            raise SwillValidationError(exceptions)


def validator(*fields: str, each=False, optional=True, pre=False):
    """The `validator` decorator marks methods on a Struct that can perform validations
    after decoding and type validation takes place. The decorator specifies which fields
    it should receive and should return None on success or raise a ValueError.

    If the field is an iterable type (eg: List then each=True can be
    specified which will then call the validator method *on each item* of the iterable.

    If optional=True (the default) then the validator *will not* be called if the
    field is optional and the field was not specified.

    If no fields are specified then the method will receive the full instance.

    The decorated method should be a class method. If it is not, it will be converted to
    one.
    """

    def wrapper(f: t.Callable):

        options = []
        sig = inspect.signature(f)
        if "field" in sig.parameters:
            options.append("field")
        elif "index" in sig.parameters:
            options.append("index")
        elif "model" in sig.parameters:
            options.append("model")

        f.__validator__ = ValidatorDefinition(
            fields, f, each, optional, pre, options=options
        )
        f_cls = f if isinstance(f, classmethod) else classmethod(f)
        return f_cls

    return wrapper


def validate_constraints(message: t.Any):
    """With the given message, validate constraints. Currently, the message must be
    of type ValidatedStruct and can contain either validation methods (see @validator)
    or annotated constraints using `annotated-types`.

    Raises a SwillValidationError on failure
    """

    if isinstance(message, ValidatedStruct):
        message.__validate__()
