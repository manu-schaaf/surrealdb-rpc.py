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
        return EscapedString.backtick(s) if use_backtick else EscapedString.angle(s)

    @classmethod
    def auto_quote(cls, s: str, use_backtick=False) -> str:
        """Automatically quote a string using double quotes

        Examples:
            >>> String.auto_quote("simple_string")
            "'simple_string'"
            >>> String.auto_quote("complex'string")
            '"complex\\'string"'
            >>> String.auto_quote("complex-string", use_backtick=True)
            '`complex-string`'
        """
        return (
            EscapedString.backtick(s)
            if use_backtick
            else (EscapedString.single(s) if "'" not in s else EscapedString.double(s))
        )

    @classmethod
    def _is_simple_char(cls, c: str) -> bool:
        return c.isalnum() or c == "_"

    @classmethod
    def is_simple(cls, s: str) -> bool:
        return all(map(String._is_simple_char, s))


class EscapedString(String):
    @classmethod
    def angle(cls, string) -> Self:
        """Escape a string using angle brackets.

        Examples:
            >>> EscapedString.angle("simple_string")
            '⟨simple_string⟩'
            >>> EscapedString.angle("complex⟨-⟩string")
            '⟨complex⟨-\\\\⟩string⟩'
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return EscapedString(f"⟨{string.replace('⟩', '\\⟩')}⟩")

    @classmethod
    def backtick(cls, string) -> Self:
        """Escape a string using backticks.

        Examples:
            >>> EscapedString.backtick("simple_string")
            '`simple_string`'
            >>> EscapedString.backtick("complex`-`string")
            '`complex\\\\`-\\\\`string`'
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return EscapedString(f"`{string.replace('`', '\\`')}`")

    @classmethod
    def single(cls, string) -> Self:
        """Escape a string using single-qoutes.

        Examples:
            >>> EscapedString.single("simple_string")
            "'simple_string'"
            >>> EscapedString.single("complex'-'string")
            "'complex\\\\'-\\\\'string'"
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return EscapedString(f"'{string.replace("'", "\\'")}'")

    @classmethod
    def double(cls, string) -> Self:
        """Escape a string using single-qoutes.

        Examples:
            >>> EscapedString.double('simple_string')
            '"simple_string"'
            >>> EscapedString.double('complex"-"string')
            '"complex\\\\"-\\\\"string"'
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return EscapedString(f'"{string.replace('"', '\\"')}"')
