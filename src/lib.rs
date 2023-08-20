use crossbeam_channel::{select, Receiver, Sender};
use std::cmp::{Eq, Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tracing::error;

const DEFAULT_MAX_BOOST_FACTOR: u64 = 500;

/// Implements the discrete time boost protocol for blockchain transactions.
/// TimeBoost is a long-running service that will receive transactions from an input channel,
/// push them to a priority queue where they are sorted by max bid, and release them at
/// discrete time intervals defined by a parameter G (in milliseconds).
/// At the end of each round of "G" milliseconds, the service will release all the transactions
/// that were in the priority queue .
pub struct TimeBoostService {
    g_factor: u64,
    tx_sender: Sender<BoostableTx>,
    txs_recv: Receiver<BoostableTx>,
    tx_heap: BinaryHeap<BoostableTx>,
    elapsed_boost_rounds: u64,
    output_feed: broadcast::Sender<BoostableTx>,
}

impl TimeBoostService {
    pub fn new(output_feed: broadcast::Sender<BoostableTx>) -> Self {
        let (tx_sender, txs_recv) = crossbeam_channel::unbounded();
        TimeBoostService {
            g_factor: DEFAULT_MAX_BOOST_FACTOR,
            tx_sender,
            txs_recv,
            tx_heap: BinaryHeap::new(),
            output_feed,
            elapsed_boost_rounds: 0,
        }
    }
    // Entities wishing to send boostable txs to the timeboost service can acquire
    // a handle to the sender channel via this method.
    pub fn sender(&self) -> Sender<BoostableTx> {
        self.tx_sender.clone()
    }
    /// Runs the loop of the timeboost service, which will collect received txs from an input
    /// channel into a priority queue that sorts them by max bid. At time T+G, the service will
    /// release all the txs in the priority queue into a broadcast channel.
    pub fn run(&mut self) {
        'next: loop {
            select! {
                // Transactions received from an input channel are pushed into
                // a priority queue by max bid where ties are broken by timestamp.
                recv(self.txs_recv) -> tx => {
                    match tx {
                        Ok(tx) => self.tx_heap.push(tx),
                        Err(e) => error!("TimeBoostService got receive error from tx input channel: {}", e),
                    }
                },
                default(Duration::from_millis(self.g_factor)) => {
                    // We release all the txs in the priority queue into the output sequence
                    // until the queue is empty and then we can restart the timer once again.
                    while let Some(tx) = self.tx_heap.pop() {
                        let output_tx = BoostableTx {
                            id: tx.id,
                            bid: tx.bid,
                            // The output sequence must have monotonically increasing timestamps,
                            // so if we had a reordering by bid, we preserve this property by outputting
                            // a transaction with a new timestamp representing the time it is emitted
                            // into the output feed.
                            timestamp_millis: unix_millis_now(),
                        };
                        if let Err(e) = self.output_feed.send(output_tx) {
                            error!(
                                "TimeBoostService got send error when broadcasting tx into output sequence: {}",
                                e,
                            );
                        }
                    }
                    self.elapsed_boost_rounds += 1;
                    continue 'next;
                }
            }
        }
    }
}

fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug, Clone, Eq)]
pub struct BoostableTx {
    id: u64,
    bid: u64,
    timestamp_millis: u64,
}

impl BoostableTx {
    #[allow(dead_code)]
    fn new(id: u64, bid: u64, timestamp_millis: u64) -> Self {
        Self {
            id,
            bid,
            timestamp_millis,
        }
    }
}

impl PartialEq for BoostableTx {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.bid == other.bid
            && self.timestamp_millis == other.timestamp_millis
    }
}

/// Transactions are comparable by bid and ties are broken by timestamp.
impl PartialOrd for BoostableTx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.bid.cmp(&other.bid) {
            Ordering::Equal => self.timestamp_millis.partial_cmp(&other.timestamp_millis),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Less => Some(Ordering::Less),
        }
    }
}

impl Ord for BoostableTx {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }

    fn max(self, other: Self) -> Self {
        if self > other {
            self
        } else {
            other
        }
    }
    fn min(self, other: Self) -> Self {
        if self < other {
            self
        } else {
            other
        }
    }
    fn clamp(self, min: Self, max: Self) -> Self {
        if self < min {
            min
        } else if self > max {
            max
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! bid {
        ($id:expr, $bid:expr, $millis:expr) => {
            BoostableTx::new($id, $bid, $millis)
        };
    }

    #[tokio::test]
    async fn normalization_no_bid_no_boost() {}

    #[tokio::test]
    async fn max_bid_offers_no_additional_boost() {}

    #[tokio::test]
    async fn bid_just_high_enough_to_boost() {}

    #[tokio::test]
    async fn bid_not_high_enough_to_boost() {}

    #[tokio::test]
    async fn timeboost_same_interval_sort_by_bid() {
        // TODO: Decide on capacity.
        let (tx_feed, mut timeboost_output_feed) = broadcast::channel(100);
        let mut service = TimeBoostService::new(tx_feed);
        let sender = service.sender();

        // Spawn a dedicated thread for the time boost service.
        std::thread::spawn(move || service.run());

        // Prepare a list of time boostable txs with bids and timestamps
        let mut original_txs = vec![
            bid!(
                0, /* ID */
                1, /* bid */
                0  /* unix timestamp millis */
            ),
            bid!(1, 2, 1),
            bid!(2, 3, 2),
            bid!(3, 4, 3),
            bid!(4, 5, 4),
            bid!(5, 6, 5),
            bid!(6, 7, 6),
        ];
        for tx in original_txs.iter() {
            sender.send(tx.clone()).unwrap();
        }

        let mut txs = vec![];
        for _ in 0..7 {
            let tx = timeboost_output_feed.recv().await.unwrap();
            txs.push(tx);
        }

        // Assert we received 7 txs from the output feed.
        assert_eq!(txs.len(), 7);

        // Assert the output is the same as the reversed input, as
        // the highest bid txs will be released first.
        original_txs.reverse();
        let want = original_txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        let got = txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        assert_eq!(want, got);
    }
}
