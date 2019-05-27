## Usage examples
-----

Invoke luigi with toml config in local scheduler 

```bash
PYTHONPATH='.' LUIGI_CONFIG_PARSER=toml LUIGI_CONFIG_PATH=configs/basic_fetch.toml luigi --local-scheduler --module transformers SarimaModeler
```