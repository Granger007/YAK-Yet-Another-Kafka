"""
cmd package - Provides producer and consumer CLI binaries.
Also proxies Python's standard library 'cmd' module so pdb and pytest
are not broken by namespace collision.
"""
import os
import sys
import importlib.util

try:
    _stdlib_dir = os.path.dirname(os.__file__)
    _cmd_file = os.path.join(_stdlib_dir, "cmd.py")
    if os.path.isfile(_cmd_file):
        _spec = importlib.util.spec_from_file_location("_stdlib_cmd", _cmd_file)
        if _spec and _spec.loader:
            _std_cmd = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_std_cmd)
            for _attr in dir(_std_cmd):
                if not _attr.startswith("__") or _attr in ("__all__", "__doc__"):
                    globals()[_attr] = getattr(_std_cmd, _attr)
except Exception:
    pass
