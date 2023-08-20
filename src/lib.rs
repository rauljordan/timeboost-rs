/// timeboost-rs implements a discrete version time boost protocol for blockchain transactions as specified in:
/// https://arxiv.org/pdf/2306.02179.pdf. Instead of individually boosting transactions, the discrete
/// version of the protocol operates in fixed rounds of length G milliseconds, where G is the parameter
/// defined in the paper.
use std::cmp::{Eq, Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
use crossbeam_channel::{select, Receiver, Sender};
use lazy_static::lazy_static;
use prometheus::register_int_counter;
use prometheus::{self, IntCounter};
use tokio::sync::broadcast;
use tracing::error;

lazy_static! {
    static ref TIME_BOOST_ROUNDS_TOTAL: IntCounter = register_int_counter!(
        "timeboost_rounds_total",
        "Number of time boost rounds elapsed"
    )
    .unwrap();
}

/// A default max boost factor, set to 500ms after empirical evaluations for Ethereum Layer 2s from
/// the time boost paper.
pub const DEFAULT_MAX_BOOST_FACTOR: u64 = 500;

/// The default capacity for the transaction input channel used by [`TimeBoostService`] to receive txs
/// from outside sources. Can be adjusted using the [`input_feed_buffer_capacity`] method when building
/// a TimeBoostService struct.
pub const DEFAULT_INPUT_FEED_BUFFER_CAP: usize = 1000;

/// The TimeBoostService struct is a long-running service that will receive transactions from an input channel,
/// push them to a priority queue where they are sorted by max bid, and then releases them at
/// discrete time intervals defined by a parameter G (in milliseconds).
///
/// At the end of each round of "G" milliseconds, the service will release all the transactions
/// that were in the priority queue and start the next round. The timestamps of transactions in the output
/// feed are the timestamp at the time of release from the priority queue.
pub struct TimeBoostService {
    g_factor: u64,
    tx_sender: Sender<BoostableTx>,
    txs_recv: Receiver<BoostableTx>,
    tx_heap: Mutex<BinaryHeap<BoostableTx>>,
    output_feed: broadcast::Sender<BoostableTx>,
}

impl TimeBoostService {
    /// Takes in an output feed for broadcasting txs released by the TimeBoostService.
    pub fn new(output_feed: broadcast::Sender<BoostableTx>) -> Self {
        let (tx_sender, txs_recv) = crossbeam_channel::bounded(DEFAULT_INPUT_FEED_BUFFER_CAP);
        TimeBoostService {
            g_factor: DEFAULT_MAX_BOOST_FACTOR,
            tx_sender,
            txs_recv,
            tx_heap: Mutex::new(BinaryHeap::new()),
            output_feed,
        }
    }
    /// Customize the buffer capacity of the input channel for the time boost service to receive transactions.
    /// [`TimeBoostService`] is listening for newly received txs in a select statement via this feed.
    /// Adjust this parameter to an estimated max throughput of txs that is satisfactory every G milliseconds.
    #[allow(dead_code)]
    fn input_feed_buffer_capacity(mut self, buffer_size: usize) -> Self {
        let (tx_sender, txs_recv) = crossbeam_channel::bounded(buffer_size);
        self.tx_sender = tx_sender;
        self.txs_recv = txs_recv;
        self
    }
    // Entities wishing to send boostable txs to the timeboost service can acquire
    // a handle to the sender channel via this method.
    pub fn sender(&self) -> Sender<BoostableTx> {
        self.tx_sender.clone()
    }
    /// Runs the loop of the timeboost service, which will collect received txs from an input
    /// channel into a priority queue that sorts them by max bid. At intervals of G milliseconds, the service will
    /// release all the txs in the priority queue into a broadcast channel.
    pub fn run(&mut self) {
        'next: loop {
            select! {
                // Transactions received from an input channel are pushed into
                // a priority queue by max bid where ties are broken by timestamp.
                recv(self.txs_recv) -> tx => {
                    let mut heap = self.tx_heap.lock().unwrap();
                    match tx {
                        Ok(tx) => heap.push(tx),
                        Err(e) => error!("TimeBoostService got receive error from tx input channel: {}", e),
                    }
                },
                default(Duration::from_millis(self.g_factor)) => {
                    // We release all the txs in the priority queue into the output sequence
                    // until the queue is empty and then we can restart the timer once again.
                    let mut heap = self.tx_heap.lock().unwrap();
                    while let Some(tx) = heap.pop() {
                        let timestamp = Utc::now().naive_utc();
                        let output_tx = BoostableTx {
                            id: tx.id,
                            bid: tx.bid,
                            // The output sequence must have monotonically increasing timestamps,
                            // so if we had a reordering by bid, we preserve this property by outputting
                            // a transaction with a new timestamp representing the time it is emitted
                            // into the output feed.
                            timestamp,
                        };
                        if let Err(e) = self.output_feed.send(output_tx) {
                            error!(
                                "TimeBoostService got send error when broadcasting tx into output sequence: {}",
                                e,
                            );
                        }
                    }
                    TIME_BOOST_ROUNDS_TOTAL.inc();
                    continue 'next;
                }
            }
        }
    }
}

/// A BoostableTx represents three important values: a unique id, a bid, and a timestamp.
/// Bid and timestamp values are used when performing the time boost protocol by the [`TimeBoostService`]
/// at intervals of G milliseconds.
#[derive(Debug, Clone, Eq)]
pub struct BoostableTx {
    id: u64,
    bid: u64,
    timestamp: NaiveDateTime,
}

impl BoostableTx {
    #[allow(dead_code)]
    fn new(id: u64, bid: u64, timestamp_millis: u64) -> Self {
        Self {
            id,
            bid,
            // TODO: Better handling of fallible conversion.
            timestamp: NaiveDateTime::from_timestamp_millis(timestamp_millis as i64).unwrap(),
        }
    }
}

/// We consider a boostable tx equal if all its fields are equal.
impl PartialEq for BoostableTx {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.bid == other.bid && self.timestamp == other.timestamp
    }
}

/// BoostableTx are comparable by bid and ties are broken by timestamp.
impl PartialOrd for BoostableTx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.bid.cmp(&other.bid) {
            Ordering::Equal => {
                // A tx is better if its timestamp is earlier than another tx.
                match self.timestamp.partial_cmp(&other.timestamp) {
                    Some(Ordering::Less) => Some(Ordering::Greater),
                    Some(Ordering::Equal) => Some(Ordering::Equal),
                    Some(Ordering::Greater) => Some(Ordering::Less),
                    _ => unreachable!(),
                }
            }
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
    async fn normalization_no_bid_no_boost() {
        let (tx_feed, mut timeboost_output_feed) = broadcast::channel(100);
        let mut service = TimeBoostService::new(tx_feed);

        // Obtain a channel handle to send txs to the TimeBoostService.
        let sender = service.sender();

        // Spawn a dedicated thread for the time boost service.
        std::thread::spawn(move || service.run());

        // Prepare a list of txs with 0 bid and monotonically increasing timestamp.
        let original_txs = vec![
            bid!(
                0, /* ID */
                0, /* bid */
                1  /* unix timestamp millis */
            ),
            bid!(1, 0, 2),
            bid!(2, 0, 3),
            bid!(3, 0, 4),
        ];
        for tx in original_txs.iter() {
            sender.send(tx.clone()).unwrap();
        }

        let mut txs = vec![];
        for _ in 0..4 {
            let tx = timeboost_output_feed.recv().await.unwrap();
            txs.push(tx);
        }

        // Assert that one boost round elapsed.
        assert_eq!(TIME_BOOST_ROUNDS_TOTAL.get(), 1);

        // Assert we received 4 txs from the output feed.
        assert_eq!(txs.len(), 4);

        // Assert the output is the same as the input input, as transactions had no bids present
        // to create any reordering in the output sequence.
        let want = original_txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        let got = txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        assert_eq!(want, got);
    }

    #[tokio::test]
    async fn tx_arrived_until_next_boost_round_with_bid_no_advantage() {
        let (tx_feed, mut timeboost_output_feed) = broadcast::channel(100);
        let mut service = TimeBoostService::new(tx_feed);

        // Obtain a channel handle to send txs to the TimeBoostService.
        let sender = service.sender();

        // Spawn a dedicated thread for the time boost service.
        std::thread::spawn(move || service.run());

        // Prepare a list of txs with 0 bid and monotonically increasing timestamp.
        let mut original_txs = vec![
            bid!(
                0, /* ID */
                0, /* bid */
                1  /* unix timestamp millis */
            ),
            bid!(1, 0, 2),
            bid!(2, 0, 3),
            bid!(3, 0, 4),
        ];
        for tx in original_txs.iter() {
            sender.send(tx.clone()).unwrap();
        }

        let late_tx = bid!(4, 100 /* large bid */, 4 + DEFAULT_MAX_BOOST_FACTOR);
        original_txs.push(late_tx.clone());

        // Wait a boost round and then send the tx.
        tokio::time::sleep(Duration::from_millis(DEFAULT_MAX_BOOST_FACTOR + 1)).await;

        sender.send(late_tx).unwrap();

        let mut txs = vec![];
        for _ in 0..5 {
            let tx = timeboost_output_feed.recv().await.unwrap();
            txs.push(tx);
        }

        // Assert that one boost round elapsed.
        assert_eq!(TIME_BOOST_ROUNDS_TOTAL.get(), 1);

        // Assert we received 5 txs from the output feed.
        assert_eq!(txs.len(), 5);

        // Assert the output is the same as the input input, as the late tx cannot gain an advantage
        // even with a high bid because it did not arrive until the second boost round.
        // to create any reordering in the output sequence.
        let want = original_txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        let got = txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        assert_eq!(want, got);
    }

    #[tokio::test]
    async fn three_boost_rounds() {}

    #[tokio::test]
    async fn all_equal_bids_tiebreak_by_arrival_timestamp() {
        let (tx_feed, mut timeboost_output_feed) = broadcast::channel(100);
        let mut service = TimeBoostService::new(tx_feed);

        // Obtain a channel handle to send txs to the TimeBoostService.
        let sender = service.sender();

        // Spawn a dedicated thread for the time boost service.
        std::thread::spawn(move || service.run());

        // Prepare a list of time boostable txs with all bids equal.
        let original_txs = vec![
            bid!(
                0,   /* ID */
                100, /* bid */
                0    /* unix timestamp millis */
            ),
            bid!(1, 100, 3),
            bid!(2, 100, 2), // The two below have the same bid, and we expect tiebreaks by timestamp if this is the case.
            bid!(3, 100, 1),
            bid!(4, 100, 6), // The two below have the same bid.
            bid!(5, 100, 5),
            bid!(6, 100, 4), // Highest bid, will come first in the output.
        ];
        for tx in original_txs.iter() {
            sender.send(tx.clone()).unwrap();
        }

        let mut txs = vec![];
        for _ in 0..7 {
            let tx = timeboost_output_feed.recv().await.unwrap();
            txs.push(tx);
        }

        // Assert that one boost round elapsed.
        assert_eq!(TIME_BOOST_ROUNDS_TOTAL.get(), 1);

        // Assert we received 7 txs from the output feed.
        assert_eq!(txs.len(), 7);

        // Expect txs to be sorted by arrival timestamp as all bids were equal.
        let got = txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        let want: Vec<u64> = vec![0, 3, 2, 1, 6, 5, 4];
        assert_eq!(want, got);
    }

    #[tokio::test]
    async fn some_equal_bids_tiebreak_by_timestamp() {
        let (tx_feed, mut timeboost_output_feed) = broadcast::channel(100);
        let mut service = TimeBoostService::new(tx_feed);

        // Obtain a channel handle to send txs to the TimeBoostService.
        let sender = service.sender();

        // Spawn a dedicated thread for the time boost service.
        std::thread::spawn(move || service.run());

        // Prepare a list of time boostable txs with some bids equal.
        let original_txs = vec![
            bid!(
                0, /* ID */
                1, /* bid */
                0  /* unix timestamp millis */
            ),
            bid!(1, 2, 1),
            bid!(2, 3, 2), // The two below have the same bid, and we expect tiebreaks by timestamp if this is the case.
            bid!(3, 3, 3),
            bid!(4, 5, 4), // The two below have the same bid.
            bid!(5, 5, 5),
            bid!(6, 7, 6), // Highest bid, will come first in the output.
        ];
        for tx in original_txs.iter() {
            sender.send(tx.clone()).unwrap();
        }

        let mut txs = vec![];
        for _ in 0..7 {
            let tx = timeboost_output_feed.recv().await.unwrap();
            txs.push(tx);
        }

        // Assert that one boost round elapsed.
        assert_eq!(TIME_BOOST_ROUNDS_TOTAL.get(), 1);

        // Assert we received 7 txs from the output feed.
        assert_eq!(txs.len(), 7);

        // Assert the output is the same as the reversed input, as
        // the highest bid txs will be released first.
        let got = txs.into_iter().map(|tx| tx.id).collect::<Vec<_>>();
        let want: Vec<u64> = vec![6, 4, 5, 2, 3, 1, 0];
        assert_eq!(want, got);
    }

    #[tokio::test]
    async fn timeboost_same_interval_sort_by_bid() {
        let (tx_feed, mut timeboost_output_feed) = broadcast::channel(100);
        let mut service = TimeBoostService::new(tx_feed);

        // Obtain a channel handle to send txs to the TimeBoostService.
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

        // Assert that one boost round elapsed.
        assert_eq!(TIME_BOOST_ROUNDS_TOTAL.get(), 1);

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
