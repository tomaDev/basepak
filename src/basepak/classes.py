from __future__ import annotations


class ConstMeta(type):
    """Metaclass for creating immutable classes

    Inheriting from this class will prevent setting and deleting attributes. This differs from the frozen class in
    dataclasses in that it ensures immutability of the class itself, and not just the instances of the class.

    Usage example:
        class IgzVersionCutoffs(metaclass=ConstMeta):
            ROCKYLINUX8_SUPPORT = '3.6.0'
            ASM_MERGE = '3.5.5'
    """
    def __setattr__(cls, key, value):
        if key in cls.__dict__:
            raise AttributeError(f"Class {cls.__name__} immutable! Cannot modify constant attribute '{key}'")
        super().__setattr__(key, value)

    def __delattr__(cls, key):
        if key in cls.__dict__:
            raise AttributeError(f"Class {cls.__name__} immutable! Cannot delete constant attribute '{key}'")
        super().__delattr__(cls, key)
