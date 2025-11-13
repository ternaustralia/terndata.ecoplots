"""Custom exceptions for the EcoPlots library.

This module defines custom exception classes that provide cleaner error messages
in Jupyter/IPython environments while maintaining standard Python exception behavior
in scripts.
"""


class EcoPlotsError(Exception):
    """Base exception class for EcoPlots library errors.

    This exception provides clean, user-friendly error messages in Jupyter notebooks
    by displaying errors with an ❌ emoji prefix instead of verbose Python tracebacks
    with file paths. In standard Python scripts, it behaves like a normal exception.

    The clean display is automatically enabled when running in IPython/Jupyter
    environments and falls back to standard exception behavior otherwise.

    Examples:
        In a Jupyter notebook:
            >>> raise EcoPlotsError("Invalid filter configuration")
            ❌ Invalid filter configuration

        In a Python script:
            >>> raise EcoPlotsError("Invalid filter configuration")
            Traceback (most recent call last):
              ...
            EcoPlotsError: Invalid filter configuration

    Notes:
        - Detection of Jupyter environment is done via `get_ipython()` builtin
        - The `__str__` method is called by Jupyter for display representation
        - Standard exception message is preserved for programmatic access via `str(e)`
    """

    def __str__(self) -> str:
        """Return a clean, formatted error message.

        In Jupyter/IPython environments, returns a message prefixed with ❌ emoji.
        In standard Python scripts, returns the standard exception message.

        Returns:
            Formatted error message string.
        """
        # Check if we're in IPython/Jupyter
        try:
            get_ipython  # type: ignore  # noqa: F821
            # In Jupyter/IPython - use clean display with emoji
            return f"\n❌ {super().__str__()}\n"
        except NameError:
            # Not in IPython - use standard exception format
            return super().__str__()
