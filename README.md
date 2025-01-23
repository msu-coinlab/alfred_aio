# Alfred

Alfred is a robust framework developed in C++20, designed to manage and assess environmental scenarios using the Chesapeake Assessment Scenario Tool (CAST). This tool helps in estimating nitrogen, phosphorus, and sediment loads by applying best management practices (BMPs) over specified geographic areas.

## Features

- **Messaging with RabbitMQ**: Utilizes AMQP protocol to manage communications between clients and the framework.
- **Data sharing with Redis**: Shares scenario information efficiently using Redis.
- **Storage with AWS S3**: Manages the storage and retrieval of scenario files using AWS S3, ensuring scalability and performance.
- **Comprehensive Library Support**: Built with essential C++ libraries like Boost, cURLpp, hiredis, redis++, SimpleAmqpClient, and aws-cpp-sdk-core.

## Dependencies

### System Packages

Install the following packages via `apt`:

```bash
sudo apt install libboost-log-dev libcurlpp-dev libfmt-dev libhiredis-dev librabbitmq-dev
```

### GitHub Repositories

The following external libraries need to be installed from GitHub:

- **AWS SDK for C++**: [aws-cpp-sdk](https://github.com/aws/aws-sdk-cpp)
- **Redis-plus-plus**: [redis-plus-plus](https://github.com/sewenew/redis-plus-plus)
- **SimpleAmqpClient**: [SimpleAmqpClient](https://github.com/alanxz/SimpleAmqpClient)

## Installation

To install Alfred, follow these steps:

```bash
git clone https://github.com/gtoscano/alfred.git
cd alfred/build
cmake ..
make
```

## Usage

Alfred operates with two main commands:

- **Sending Scenarios to CAST**:
  ```bash
  ./alfred send
  ```
  Starts the daemon responsible for sending scenarios to CAST.

- **Retrieving Assessed Scenarios**:
  ```bash
  ./alfred retrieve
  ```
  Initiates the process to retrieve assessed scenarios from CAST and communicates results back to clients.

## Contributing

Contributions to Alfred are welcome! Please fork the repository and submit pull requests with your proposed changes.

## License

Alfred is licensed under the [MIT License](LICENSE). See the LICENSE file for more details.


## Support

For support and queries, feel free to open an issue on the GitHub repository.

---

For more details on CAST and how it helps in environmental planning, please visit the [CAST homepage](https://www.casttool.org).

