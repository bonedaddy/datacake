# Datacake CRDT

An implementation of Riak's ORSWOT CRDT which is a CRDT which allows for removal of old
tombstones once a new event has been observed.

The set is built upon the second supported structure `HLCTimestamp` which is a Hybrid Logical Clock
which guarantees the timestamp will always be unique and monotonic (providing it's used correctly.)

### Basic Example
```rust
use datacake_crdt::{OrSWotSet, HLCTimestamp, get_unix_timestamp_ms};

fn main() {
    let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
    
    // Simulating a node begin slightly ahead.
    let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms() + 5000, 0, 1);
    
    let mut node_a_set = OrSWotSet::default();
    let mut node_b_set = OrSWotSet::default();
    
    // Insert a new key with a new timestamp in set A.
    node_a_set.insert(1, node_a.send().unwrap());
    
    // Insert a new entry in set B.
    node_b_set.insert(2, node_b.send().unwrap());
    
    // Let some time pass for demonstration purposes.
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    // Set A has key `1` removed.
    node_a_set.delete(1, node_a.send().unwrap());
    
    // Merging set B with set A and vice versa. 
    // Our sets are now aligned without conflicts.
    node_b_set.merge(node_a_set.clone());
    node_a_set.merge(node_b_set);
    
    // Set A and B should both see that key `1` has been deleted.
    assert!(node_a_set.get(&1).is_none(), "Key a was not correctly removed.");
    assert!(node_b_set.get(&1).is_none(), "Key a was not correctly removed.");
}
```


### Inspirations
- [CRDTs for Mortals by James Long](https://www.youtube.com/watch?v=iEFcmfmdh2w)
- [Big(ger) Sets: Making CRDT Sets Scale in Riak by Russell Brown](https://www.youtube.com/watch?v=f20882ZSdkU)
- ["CRDTs Illustrated" by Arnout Engelen](https://www.youtube.com/watch?v=9xFfOhasiOE)