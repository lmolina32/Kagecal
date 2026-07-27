#!/usr/bin/env python3
"""Tests for the interactive shell front-end (the ``kagecal`` script).

The entry point is an extensionless executable, so it is loaded by path. Only
the non-interactive surface is tested here (command dispatch, usage validation,
file import/export, calendar switching); the ``input()``-driven flows are not.
"""

import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path

import pytest

from utils import create_event

ROOT = Path(__file__).resolve().parents[1]


def _load_cli():
    # The entry point has no .py extension, so attach an explicit source loader.
    path = str(ROOT / "kagecal")
    loader = SourceFileLoader("kagecal_cli", path)
    spec = importlib.util.spec_from_loader("kagecal_cli", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


kagecal = _load_cli()


@pytest.fixture
def app():
    return kagecal.Application()


# --------------------------------------------------------------------------- #
# Command table / dispatch
# --------------------------------------------------------------------------- #
def test_commands_registered(app):
    expected = {"join", "list", "create", "remove", "modify", "show", "dump", "switch", "clear"}
    assert expected <= set(app.COMMANDS)


def test_perform_join_without_arg_prints_usage(app, capsys):
    app.perform("join", [])
    assert "Usage:" in capsys.readouterr().out


def test_perform_dump_wrong_arity_prints_usage(app, capsys):
    app.perform("dump", ["only-one-arg"])
    assert "Usage:" in capsys.readouterr().out


def test_perform_unknown_command(app, capsys):
    app.perform("frobnicate", [])
    assert "Unknown command: frobnicate" in capsys.readouterr().out


def test_help_lists_commands(app, capsys):
    app._help([])
    out = capsys.readouterr().out
    assert "join" in out and "list" in out


def test_help_unknown_command(app, capsys):
    app._help(["frobnicate"])
    assert "Unknown command: frobnicate" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# Local calendar operations
# --------------------------------------------------------------------------- #
def test_local_join_and_list(app, capsys):
    app._local_join("home")
    assert app.curr_calendar == "home"
    app.calendars["home"].create(**create_event(name="standup").__dict__)

    app._list()
    out = capsys.readouterr().out
    assert "standup" in out
    assert "Event Name" in out  # table header rendered


def test_switch_to_joined_calendar(app):
    app._local_join("a")
    app._local_join("b")
    app._switch("a")
    assert app.curr_calendar == "a"


def test_switch_to_unjoined_calendar_warns(app, capsys):
    app._local_join("a")
    app._switch("ghost")
    assert "ghost" in capsys.readouterr().out
    assert app.curr_calendar == "a"


def test_clear_invokes_system(app, mocker):
    system = mocker.patch.object(kagecal.os, "system")
    app._clear()
    system.assert_called_once()


# --------------------------------------------------------------------------- #
# Event file import / export
# --------------------------------------------------------------------------- #
def test_dump_then_load_round_trips(app, tmp_path):
    app._local_join("home")
    event = create_event(name="bday", description="party", location="house")
    ident = app.calendars["home"].create(**event.__dict__)

    out_file = tmp_path / "event.cal"
    app._dump(ident, str(out_file))
    assert out_file.exists()

    loaded = app._load_event_file(str(out_file))
    assert loaded == event


def test_load_missing_file_returns_none(app, capsys):
    assert app._load_event_file(str(ROOT / "does-not-exist.cal")) is None


def test_load_invalid_event_returns_none(app, tmp_path):
    bad = tmp_path / "bad.cal"
    bad.write_bytes(b"this is not a pickled event")
    assert app._load_event_file(str(bad)) is None
