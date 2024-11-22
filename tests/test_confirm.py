import pytest
from unittest.mock import patch
import click
from basepak.confirm import default

pytest.mark.parametrize('return_value', [True, False])
def test_default_accepts_by_default():
    with patch('click.confirm', return_value=True) as mock_confirm:
        default()
        mock_confirm.assert_called_once()

def test_default_with_custom_banner():
    with patch('click.confirm', return_value=True) as mock_confirm:
        default(banner='Proceed?')
        mock_confirm.assert_called_with(click.style('Proceed?', fg='yellow'), abort=True)
