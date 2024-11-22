from unittest.mock import patch

import click

from basepak.confirm import default


def test_custom_banner():
    with patch('click.confirm', return_value=True) as mock_confirm:
        default(banner='Proceed?')
        mock_confirm.assert_called_with(click.style('Proceed?', fg='yellow'), abort=True)
