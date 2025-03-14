"""Confirmation prompts

If writing to log files, these will be omitted
"""

from typing import Optional


def default(banner: Optional[str] = 'Continue?', style_kwargs: Optional[dict] = None,
            confirm_kwargs: Optional[dict] = None) -> None:
    """Default confirmation prompt
    :param banner: message to display
    :param style_kwargs: style kwargs for click.style
    :param confirm_kwargs: kwargs for click.confirm
    """
    import click
    if style_kwargs is None:
        style_kwargs = {'fg': 'yellow'}
    if confirm_kwargs is None:
        confirm_kwargs = {'abort': True}
    click.confirm(click.style(banner, **style_kwargs), **confirm_kwargs)
