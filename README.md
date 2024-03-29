# mini-match
Miniature stock market matching engine

* Written in C++14
* Uses only the STL
* Maintains limit order book
* Implements FIFO matching algorithm
* Processes commands from stdin
* Includes unit tests with simple built-in framework
* Optional multi-threading with thread-safe queue for producer-consumer

Commands:
* BUY - Place buy order - BUY GFD|IOC price qty order_id
* SELL - Place sell order - SELL GFD|IOC price qty order_id
* CANCEL - Cancel order - CANCEL order_id
* MODIFY - Modify order - MODIFY order_id BUY|SELL price qty
* PRINT - Print order book
* CLEAR - Clear order book

# Build and Run
$ ./run.sh # Compile and run with cmd.txt as input

$ ./mini-match --run-tests # Run unit tests

$ ./mini-match --run-threads # Run with multiple threads
