import warnings
from typing import Self


class String(str):
    @classmethod
    def auto_escape(cls, s: str, use_backtick=False) -> str:
        """Automatically escape a string using either angle brackets or backticks.

        Note:
            If the string is already escaped, it will be unescaped before escaping it with the appropriate method.

        Examples:
            >>> print(String.auto_escape("simple_string"))
            simple_string
            >>> print(String.auto_escape("complex-string"))
            ⟨complex-string⟩
            >>> print(String.auto_escape("complex-string", use_backtick=True))
            `complex-string`
            >>> print(String.auto_escape("`complex-string`"))
            ⟨complex-string⟩
            >>> print(String.auto_escape("`complex-string`", use_backtick=True))
            `complex-string`
        """
        if cls.is_simple(s):
            return s
        if cls.is_escaped(s):
            s = s[1:-1]
        return EscapedString.backtick(s) if use_backtick else EscapedString.angle(s)

    @staticmethod
    def is_escaped(s: str) -> bool:
        match s[0], s[-1]:
            case "⟨", "⟩" if not s.endswith("\\⟩"):
                return True
            case "`", "`" if not s.endswith("\\`"):
                return True
            case _:
                return False

    @classmethod
    def auto_quote(cls, s: str, use_backtick=False) -> str:
        """Automatically quote a string using the appropriate quote type.
        Uses single quotes by default and double quotes if `s` contains a single quote.
        Double quotes within a double-quoted string are escaped.

        Alternatively, you may set `use_backtick` to `True` to use backticks instead of single or double quotes.

        Note:
            This method will *NOT* check if the string is already quoted!

        Examples:
            >>> print(String.auto_quote("simple_string"))
            'simple_string'
            >>> print(String.auto_quote("complex'string"))
            "complex'string"
            >>> print(String.auto_quote("\\\"complex'string\\\""))
            "\\\"complex'string\\\""
            >>> print(String.auto_quote("complex-string", use_backtick=True))
            `complex-string`
        """
        if use_backtick:
            return EscapedString.backtick(s)
        elif "'" in s:
            return EscapedString.double(s)
        else:
            return EscapedString.single(s)

    @staticmethod
    def _is_simple_char(c: str) -> bool:
        return c.isalnum() or c == "_"

    @staticmethod
    def is_simple(s: str) -> bool:
        return all(map(String._is_simple_char, s))


class EscapedString(String):
    @classmethod
    def angle(cls, string) -> Self:
        """Escape a string using angle brackets.

        Examples:
            >>> print(EscapedString.angle("simple_string"))
            ⟨simple_string⟩
            >>> print(EscapedString.angle("complex⟨-⟩string"))
            ⟨complex⟨-\\⟩string⟩
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return cls(f"⟨{string.replace('⟩', '\\⟩')}⟩")

    @classmethod
    def backtick(cls, string) -> Self:
        """Escape a string using backticks.

        Examples:
            >>> print(EscapedString.backtick("simple_string"))
            `simple_string`
            >>> print(EscapedString.backtick('"quoted" string'))
            `"quoted" string`
            >>> print(EscapedString.backtick("complex`-`string"))
            `complex\\`-\\`string`
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return cls(f"`{string.replace('`', '\\`')}`")

    @classmethod
    def single(cls, string) -> Self:
        """Escape a string using single-qoutes.

        Examples:
            >>> print(EscapedString.single("simple_string"))
            'simple_string'
            >>> print(EscapedString.single("complex'-'string"))
            'complex\\'-\\'string'
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return cls(f"'{string.replace("'", "\\'")}'")

    @classmethod
    def double(cls, string) -> Self:
        """Escape a string using single-qoutes.

        Examples:
            >>> print(EscapedString.double('simple_string'))
            "simple_string"
            >>> print(EscapedString.double('complex"-"string'))
            "complex\\"-\\"string"
        """
        if isinstance(string, cls):
            warnings.warn(
                f"The string {string} is already escaped with {string[0]}, are you sure you want to escape it again?"
            )
        return cls(f'"{string.replace('"', '\\"')}"')
