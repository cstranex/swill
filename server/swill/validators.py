"""Validation helpers for Swill

Swill supports annotated-types
"""
from datetime import timezone
from collections import abc
import functools
import re
import typing as t

import annotated_types as at
from msgspec import Struct

from swill._exceptions import SwillValidationError


Constraint = t.Union[at.BaseMetadata, slice, "re.Pattern[bytes]", "re.Pattern[str]"]


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


def _nested_validator(_, value: t.Any):
    if _is_iterable(type(value)):
        for v in value:
            v.__validate__()
    else:
        value.__validate__()


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


def _constraints_for_type(
    tp: t.Type,
) -> t.Iterator[t.Tuple[str, t.Iterable[Constraint]]]:
    """Yield a tuple of field name, constraint arguments for the given type"""

    for name, hint in t.get_type_hints(tp, include_extras=True).items():
        if not t.get_origin(hint) is t.Annotated:
            continue
        args = iter(t.get_args(hint))
        next(args)  # skip the first argument which is the type
        args = iter(t.get_args(hint))
        yield (
            name,
            filter(lambda a: isinstance(a, (at.BaseMetadata, re.Pattern, slice)), args),
        )


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

    __validators__ = []

    @classmethod
    def _add_validator(cls, validator_def):
        fields, callback, each = validator_def
        if not fields:
            cls.__validators__.append((tuple(), callback, False))
        else:
            hints = t.get_type_hints(cls)
            for field in fields:
                if field not in cls.__slots__:
                    raise RuntimeError(f"{field} is not a valid struct field")
                if each:
                    # We only accept one field and that field must be iterable
                    assert len(field) == 1
                    hint = hints.get(field[0])
                    if not _is_iterable(hint):
                        raise RuntimeError(f"{field} is not iterable. Cannot use `each`")

                cls.__validators__.append(validator_def)

    def __init_subclass__(cls, **kwargs):
        cls.__validators__ = []
        # Find any constraint validators defined by type annotations
        for field, constraints in _constraints_for_type(cls):
            for constraint in constraints:
                if validator_callable := _VALIDATORS.get(type(constraint)):
                    cls._add_validator(
                        (
                            (field,),
                            functools.partial(validator_callable, constraint=constraint),
                            False,
                        )
                    )

        # Get validators defined by the @validator decorator
        for name in cls.__dict__:
            if validator_def := getattr(getattr(cls, name), "__validator__", None):
                cls._add_validator(validator_def)

        hints = t.get_type_hints(cls)
        for field in cls.__slots__:
            # Find any nested ValidatedStructs
            hint = hints.get(field)
            args = list(t.get_args(hint))
            if not t.get_origin(hint):
                args.append(hint)
            for arg in args:
                if issubclass(arg, ValidatedStruct):
                    cls._add_validator(((field,), _nested_validator, False))

    def __validate__(self):
        exceptions = []
        all_exceptions = hasattr(self, "Meta") and getattr(self.Meta, "return_all_errors")

        for fields, callback, each in self.__validators__:
            if not fields:
                values = [self]
            else:
                values = [getattr(self, field) for field in fields]
            try:
                if each:
                    # If each is True, then the validator can only accept exactly one
                    # field, so we call the validator for every item in that field
                    for value in getattr(self, fields[0]):
                        callback(self.__class__, value)
                else:
                    callback(self.__class__, *values)
            except (ValueError, TypeError, SwillValidationError) as exc:
                exceptions.append((fields, exc))
                if not all_exceptions:
                    break

        if exceptions:
            raise SwillValidationError(exceptions)


def validator(*fields: t.List[str], each=False):
    """The `validator` decorator marks methods on a Struct that can perform validations
    after decoding and type validation takes place. The decorator specifies which fields
    it should receive and should return None on success or raise a ValueError

    If no fields are specified then the method will receive the full instance.

    The decorated method should be a class method. If it is not, it will be converted to
    one.
    """

    if (not fields or len(fields) > 1) and each:
        raise RuntimeError("Each keyword may only be specified with one field")

    def wrapper(f: t.Callable):
        f.__validator__ = (fields, f, each)
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
