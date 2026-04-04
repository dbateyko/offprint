import pytest

def test_core_imports():
    from offprint import orchestrator
    from offprint import command_cli
    from offprint.adapters import registry
    
def test_registry_loads_without_error():
    from offprint.adapters.registry import ADAPTER_REGISTRY
    assert isinstance(ADAPTER_REGISTRY, dict)
