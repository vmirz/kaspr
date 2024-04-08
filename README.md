# Kaspr - Kafka Stream Processing

Kaspr is a Python library for Kafka stream processing, extending the capabilities of the [Faust](https://github.com/TotalWineLabs/faust) library to provide a more configuration-driven approach to building streaming pipelines. With Kaspr, developers can easily configure event routing, filtering, and transformation, alongside integrating custom logic in Python to create robust, exactly-once processing pipelines.


## Features

- *Easy Configuration*: Kaspr utilizes a configuration-driven approach, allowing for the straightforward setup of streaming patterns like event routing, filtering, and transformation.
- *Custom Logic Integration*: Incorporate your Python code to add specific logic to your streaming pipeline, enhancing flexibility and functionality.
Prebuilt Components:
- *Message Scheduler*: Schedule Kafka messages to be dispatched at a future date and time.
- *Deduplicate*: Simplify the deduplication of Kafka messages based on their keys, ensuring message uniqueness.

## License

Kaspr is released under the MIT License. See the LICENSE file for more details.

## Acknowledgements

Kaspr builds upon the excellent work done by the [Faust](https://github.com/TotalWineLabs/faust) library and is designed to make stream processing with Kafka more accessible and manageable.