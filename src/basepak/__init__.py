__version__ = '0.0.113'

import os

if os.environ.get('NO_COLOR'):
    import click

    def no_color_style(
            text,
            *args,    # noqa unused
            **kwargs, # noqa unused
    ):
        return text

    click.style = no_color_style
