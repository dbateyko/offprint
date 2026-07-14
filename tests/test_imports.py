import importlib


def test_core_imports():
    for module in ("offprint.orchestrator", "offprint.command_cli", "offprint.adapters.registry"):
        importlib.import_module(module)


def test_registry_loads_without_error():
    from offprint.adapters.registry import ADAPTERS

    assert isinstance(ADAPTERS, dict)
