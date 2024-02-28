# MicroBatcherDemo

`MicroBatcherDemo` is a .NET 6 library demonstrating the micro-batching pattern, which groups multiple tasks into a single batch to optimize performance and resource utilization.

## Prerequisites

- .NET 6 SDK
- An IDE or editor that supports .NET (e.g., Visual Studio 2022, Visual Studio Code, JetBrains Rider)

## Building the Projects

To build the projects, you will need to navigate to the respective project directories and use the .NET CLI.

### MicroBatcherDemo Library

To build the `MicroBatcherDemo` library:

```
cd MicroBatcherDemo
dotnet build
```
This command compiles the library and places the output in the `bin/` directory.

### MicroBatcherDemoTests

To build the test project for the `MicroBatcherDemo` library:

```
cd MicroBatcherDemoTests
dotnet build
```

This ensures that the test project compiles successfully.

## Running the Tests
To run the unit tests for the `MicroBatcherDemo` library:

```
cd MicroBatcherDemoTests
dotnet test
```

This will execute the tests and output the results to the console.

## Usage
After compiling, you can reference the `MicroBatcherDemo` library in your .NET projects. Include the compiled DLL as a reference in your project file, or use a project reference if you're working within the same solution.

## License
This project is licensed under the MIT License - see the `LICENSE` file for details.