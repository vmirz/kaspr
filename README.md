# Kaspr - Kafka Stream Processing

Kaspr is a Python library for Kafka stream processing, extending the capabilities of [Faust](kaspr/faust), its stream processing engine, to provide a more configuration-driven approach to building streaming pipelines. With Kaspr, developers can easily configure event routing, filtering, and transformation, alongside integrating custom logic in Python to create robust, exactly-once processing pipelines.


## Features

- *Easy Configuration*: Kaspr utilizes a configuration-driven approach, allowing for the straightforward setup of streaming patterns like event routing, filtering, and transformation.
- *Custom Logic Integration*: Incorporate your Python code to add specific logic to your streaming pipeline, enhancing flexibility and functionality.
Prebuilt Components:
- *Message Scheduler*: Schedule Kafka messages to be dispatched at a future date and time.
- *Deduplicate*: Simplify the deduplication of Kafka messages based on their keys, ensuring message uniqueness.

## Development

| Path | What it holds |
|------|---------------|
| `kaspr/` | The package |
| `kaspr/faust/` | Its engine, Faust: agents, streams, tables, Kafka transport |
| `kaspr/mode/` | The services the engine is made of, mode: lifecycle, supervision, timers, logging |
| `tests/` | The tests, the engine's in `tests/faust/` and mode's in `tests/mode/` |

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest
```

- Faust and mode are part of the package: import them as `kaspr.faust` and `kaspr.mode`. `kaspr/__init__.py` holds the one version, which a change to any of them bumps.
- The engine uses these packages where they are installed: `aiodns>=1.1`, `aiomonitor>=0.4.4`, `charset-normalizer>=3.0`, `ciso8601`, `confluent-kafka>=2.0`, `datadog`, `orjson>=3.0`, `python-rocksdb>=0.6.7`, `redis>=4.0`, `setproctitle>=1.1`, `statsd~=3.3.0`, `uvloop>=0.8.1`.

## License

Kaspr is released under the MIT License. See the LICENSE file for more details.

Faust and mode are released under the BSD 3-Clause License. See [kaspr/faust/LICENSE](kaspr/faust/LICENSE) and [kaspr/mode/LICENSE](kaspr/mode/LICENSE).

## Acknowledgements

Kaspr builds upon the excellent work done by the [Faust](https://github.com/robinhood/faust) library and is designed to make stream processing with Kafka more accessible and manageable. Its engine is the fork that was released as `twm-faust` until 1.18.0, from [TotalWineLabs/faust](https://github.com/TotalWineLabs/faust). The engine's services are [mode](https://github.com/ask/mode), in the fork released as [mode-streaming](https://github.com/faust-streaming/mode), from 0.4.1.