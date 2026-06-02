# Assumptions
* column-based, structured database
* thread-per-core architecture
* custom driver, no jdbc

# Storage format
```text
.../database
└── <db_name>
    ├── meta.json
    └── table_<n>
        ├── meta.json
        └── col_<n>
            ├── meta.json
            ├── wal.log
            ├── strtab.dat
            │   ├── hashmaps in memory
            │   ├── compressed on disk
            │   ├── probably have to chunk this into several files/blocks
            │   │   ├── use bloom filters to easily check whether a string is contained in a block
            |   │   └── false negatives are not an issue, we probably still end up >99% deduplication
            └── blocks
                ├── block_<n>.dat
                │   ├── _raw, compact data depending on the col type_
                │   ├── always sorted by RID
                │   └── max 256KiB (tunable)
                └── meta_<n>.json
                    ├── first and last RID
                    ├── mix/max value of this block
                    └── some metadata that helps with indexing?

```
<details>
  <summary>Example <code>users</code></summary>
  <pre>
.../database
└── users
    ├── meta.json
    └── table_0
        ├── meta.json
        ├── col_0
        │   ├── meta.json
        │   │   ├── name: tombstones
        │   │   ├── type: bool
        │   │   └── compression: none
        │   ├── wal.log
        │   ├── ~strtab.dat~ does not exist for bool cols
        │   └── blocks
        │       ├── block_0.dat
        │       │   ├── up to 256KiB of 1-byte aligned bools
        │       │   └── sorted by RID
        │       └── meta_0.json
        │           ├── first RID: 0
        │           ├── last RID: 4
        │           └── min/max: 0/1 (false/true, doesn't make sense for bool)
        ├── col_1
        │   ├── meta.json
        │   │   ├── name: transaction
        │   │   ├── type: uint64
        │   │   └── compression: none
        │   ├── wal.log
        │   ├── ~strtab.dat~ does not exist for uint64 cols
        │   └── blocks
        │       ├── block_0.dat
        │       │   ├── up to 32768 (256KiB of 8-byte aligned) uint64
        │       │   └── sorted by RID
        │       └── meta_0.json
        │           ├── first RID: 0
        │           ├── last RID: 4 (up to 65536)
        │           └── min/max: 0/0
        ├── col_2
        │   ├── meta.json
        │   │   ├── name: age
        │   │   ├── type: uint32
        │   │   └── compression: none
        │   ├── wal.log
        │   ├── ~strtab.dat~ does not exist for uint32 cols
        │   └── blocks
        │       ├── block_0.dat
        │       │   ├── up to 65536 (256KiB of 4-byte aligned) uint32
        │       │   └── sorted by RID
        │       └── meta_0.json
        │           ├── first RID: 0
        │           ├── last RID: 4 (up to 65536)
        │           ├── min: 21
        │           └── max: 55
        └── col_3
            ├── meta.json
            │   ├── name: name
            │   ├── type: string
            │   └── compression: lz4
            ├── wal.log
            ├── strtab.dat
            │   ├── `[0]`: Arne
            │   ├── `[1]`: Betty
            │   └── `[2]`: Charly
            └── blocks
                ├── block_0.dat
                │   ├── up to 65536 (256KiB of 4-byte aligned) uint32
                │   ├── indices into strtab
                │   └── sorted by RID
                └── meta_0.json
                    ├── first RID: 0
                    ├── last RID: 4 (up to 65536)
                    └── min/max: 0/2 (some indices into strtab as well)
  </pre>
</details>

## Accessing data
Every file in the structure has exactly one core, that own that file.
That owner relationship is defined as:

$$core\\_id\_{owner} \equiv hash(table\\_num, col\\_num, block\\_num) \mod{num\\_cores}$$

Where

$$\forall n, v \in \mathbb{N}, \ hash(v\_{0}, \ldots, v\_{n}) \ne hash(v\_{0}, \ldots, v\_{n}, 0)$$

Owners for some files from the above example:

|file|owner $$\mod{num\\_cores}$$|
|---|---|
|`.../database/users/table_0/meta.json`|$$core\\_id\_{owner} = hash(0)$$|
|`.../database/users/table_0/col_0/meta.json`|$$core\\_id\_{owner} = hash(0, 0)$$|
|`.../database/users/table_0/col_1/wal.log`|$$core\\_id\_{owner} = hash(0, 1)$$|
|`.../database/users/table_0/col_2/blocks/block_7.dat`|$$core\\_id\_{owner} = hash(0, 2, 7)$$|

## Processing data
On an incoming connection, the core that receives it, handles it from start to finish.
That implies that any core must be able to read any data.
This happens by cross-core message passing.

### Cross-core reads
When `core 0` needs to read a block that belongs to `core 3`, an asynchronous message is sent from `0` to `3`.
`3` can then use his cache or read the block newly, and then send an async message back to `0`.
That response message contains an `Arc<Data>` (whatever `Data` is), which `3` owns, but can pass out copies of it.

### Cross-core inserts
When `core 0` needs to insert into a block that belongs to `core 3`, again, `0` sends a message.
`3` must update the block however he wants (probably in-mem, maybe `fsync`) and sends back a copy to `0`.
Additionally, `3` must now send a message to all other cores (or at least the ones that requested that block in the past) with a fresh `Arc<Data>`.
Other cores that are interested, can (and should) then update their version of the block with the new data.

### Deletions
Deletions don't happen. Instead, every table has a `tombstones` column of type `bool` that tracks deleted rows.
Every query implicitly contains a `WHERE deleted=false` clause.

### Updates
Updates are implemented as `delete+insert`.

## Transactionality
Similar to deletions, there exists a column `transaction` for every table.
Every thread keeps a copy of a transaction ledger that holds the state of any ongoing transaction.
When a transaction commits, its id in the ledger is removed, and a message with that update is broadcasted to all cores.

An ongoing transaction ignores rows which aren't committed yet (implicitly adding a `WHERE committed = true` to every query).

## Notes
* my keeping names in the metadata files and not in the directory structure, we can allow stuff to be renamed
  * during query compilation, names are resolved to numbers, so compiled queries are not affected by renames in any way
* keeping the data blocks raw/compact and the data aligned, allows us to make heavy use of SIMD for filtering (which we'll need because of the implicit `WHERE deleted = false AND committed = true` in every query)
* compression is a disk-only thing, not an in-memory
  * if we held compressed data in memory, we might as well flush it an free the memory
  * `strtab.dat` should be compressed on disk
  * blocks can be sparse (especially for bool columns), meaning no `block_<n>.dat` if it only contains 256KiB of the same byte (`sparse_value` can be added in the block metadata)

## Indices
* no idea yet
