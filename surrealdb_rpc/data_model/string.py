import warnings
from typing import Self


class String(str):
    @classmethod
    def auto_escape(cls, s: str, use_backtick=False) -> str:
        """Automatically escape a string using the appropriate method.

        Examples:
            >>> String.auto_escape("simple_string")
            'simple_string'
            >>> String.auto_escape("complex-string")
            '⟨complex-string⟩'
            >>> String.auto_escape("complex-string", use_backtick=True)
            '`complex-string`'
        """
        if cls.is_simple(s):
            return s
        return cls.escape_backtick(s) if use_backtick else cls.escape_angle(s)

    @classmethod
    def auto_quote(cls, s: str, use_backtick=False) -> str:
        """Automatically quote a string using double quotes

        Examples:
            >>> String.auto_quote("simple_string")
            '"simple_string"'
            >>> String.auto_quote("complex-string")
            '⟨complex-string⟩'
            >>> String.auto_quote("complex-string", use_backtick=True)
            '`complex-string`'
        """
        if cls.is_simple(s):
            return f'"{s}"'
        return cls.escape_backtick(s) if use_backtick else cls.escape_angle(s)

    @classmethod
    def escape_angle(cls, s: str) -> str:
        """Escape a string using angle brackets.

        Examples:
            >>> String.escape_angle("simple_string")
            '⟨simple_string⟩'
            >>> String.escape_angle("complex-string")
            '⟨complex-string⟩'
        """
        return EscapedString.angle(s)

    @classmethod
    def escape_backtick(cls, s: str) -> str:
        """Escape a string using backticks.

        Examples:
            >>> String.escape_backtick("simple_string")
            '`simple_string`'
            >>> String.escape_backtick("complex-string")
            '`complex-string`'
        """
        return EscapedString.backtick(s)

    @classmethod
    def _is_simple_char(cls, c: str) -> bool:
        return c.isalnum() or c == "_"

    @classmethod
    def is_simple(cls, s: str) -> bool:
        return all(map(String._is_simple_char, s))


class EscapedString(String):
    @classmethod
    def angle(cls, string) -> Self:
        if not isinstance(string, cls):
            if string.startswith("⟨") and string.endswith("⟩"):
                warnings.warn(
                    f"The string {string} is already angle-escaped, are you sure you want to escape it again?"
                )
            return cls(f"⟨{string.replace('⟩', '\\⟩')}⟩")
        return cls(string)

    @classmethod
    def backtick(cls, string) -> Self:
        if not isinstance(string, cls):
            if string.startswith("`") and string.endswith("`"):
                warnings.warn(
                    f"The string {string} is already backtick-escaped, are you sure you want to escape it again?"
                )
            return cls(f"`{string.replace('`', '\\`')}`")
        return cls(string)
