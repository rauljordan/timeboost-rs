# Timeboost-rs

This library implements the **time boost ordering policy** for blockchain transactions in Rust. 

[![Security audit](https://github.com/rauljordan/timeboost-rs/actions/workflows/audit.yml/badge.svg)](https://github.com/rauljordan/timeboost-rs/actions/workflows/audit.yml)

[![Rust](https://github.com/rauljordan/timeboost-rs/actions/workflows/general.yml/badge.svg)](https://github.com/rauljordan/timeboost-rs/actions/workflows/general.yml)

The protocol is described by a research paper titled ["Buying Time: Latency Racing vs. Bidding for Transaction Ordering"](https://arxiv.org/pdf/2306.02179.pdf)
created by researchers at @OffchainLabs, Akaki Mamageishvili, Mahimna Kelkar, Ed Felten, and Jan Christoph Schlegel from University of London. All ideas implemented in this crate are a result of the aforementioned research.

Time Boost is an approach for transaction fair ordering that accounts timestamps and bids to create a score that can be used to sort transactions for rollup sequencers. It supports low-latency finality, similar to first-come, first-serve, but is more fair towards users. With time boost, bids can help buy time in a final ordering, but only up to a constant factor `G`, defined in milliseconds.

This library contains Rust code that defines an asynchronous `TimeBoostService` that can take in transactions via an input channel, and output a final ordered list continuously by applying the protocol internally. This implementation is a "discrete" version of the time boost protocol because it operates in rounds of time `G`. Here's how it works:

1. Record an initial timestamp, `T`
2. Receive transactions in the background, pushing them onto a priority queue ordered by bid (ties are broken by earliest arrival timestamp)
3. At time `T+G`, where `G` is a constant defined in milliseconds, all txs are in the priority queue are released and their timestamps are modified to be the time they are emitted in the output feed
4. Start counting again from `T = T_now` until the next round

Credits to Ed Felten for the idea.

## Dependencies

[Rust](https://www.rust-lang.org/tools/install) stable version `1.71.1`

## Usage

The main way of using the library is by initializing a `TimeBoostService` struct with a transaction 
output feed channel.

```rust
use timeboost_rs::TimeBoostService;
use tokio::sync::broadcast;

let (tx_output_feed, mut rx) = broadcast::channel(100);
let mut service = TimeBoostService::new(tx_output_feed);
```

The service can be configured with options to customize the max boost factor, `G`, or the capacity of the
transaction input channel:

```rust
let mut service = TimeBoostService::new(tx_output_feed)
                    .input_feed_buffer_capacity(1000)
                    .g_factor(200 /* millis */);
```

Here's a full example of using time boost, sending txs into it, and receiving its output:

```rust
use timeboost_rs::{TimeBoostService, BoostableTx};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    let (tx_output_feed, mut rx) = broadcast::channel(100);
    let mut service = TimeBoostService::new(tx_output_feed);

    // Obtain a channel handle to send txs to the TimeBoostService.
    let sender = service.sender();

    // Spawn a dedicated thread for the time boost service.
    std::thread::spawn(move || service.run());

    let mut txs = vec![
        BoostableTx::new(0 /* id */, 1 /* bid */, 100 /* unix timestamp millis */),
        BoostableTx::new(1 /* id */, 100 /* bid */, 101 /* unix timestamp millis */),
    ];

    // Send the txs to the time boost service.
    for tx in txs.iter() {
        sender.send(tx.clone()).unwrap();
    }

    // Await receipt of both txs from the timeboost service's output feed.
    let mut got_txs = vec![];
    for _ in 0..2 {
        let tx = rx.recv().await.unwrap();
        got_txs.push(tx);
    }

    // Assert we received 2 txs from the output feed.
    assert_eq!(txs.len(), 2);

    // Assert the output is the same as the reversed input, as
    // the highest bid txs will be released first.
    txs.reverse();
    let want = txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
    let got = got_txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
    assert_eq!(want, got);
}
```

## Metrics

The library exposes a `TIME_BOOST_ROUNDS_TOTAL` prometheus counter for inspecting the number of rounds elapsed.

```rust
lazy_static! {
    static ref TIME_BOOST_ROUNDS_TOTAL: IntCounter = register_int_counter!(
        "timeboost_rounds_total",
        "Number of time boost rounds elapsed"
    )
    .unwrap();
}
```

## License

Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this repository by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.