# adapters

Platform and site-specific adapter implementations.

- Shared platform bases (high impact): WordPress, Digital Commons, OJS, Drupal.
- Site-specific adapters: host/journal overrides for non-standard layouts.
- Routing entrypoint: `registry.py`.

Before changing shared base adapters, follow regression-safety guidance in `CONTRIBUTING.md` and `docs/ADAPTER_DEVELOPMENT.md`.
