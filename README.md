Starting the rust core

    cd rust_blockchain/
    mkdir dbs/
    mkdir dbs/state
    RUSTFLAGS="-Awarnings" cargo run <port_num, default 8001> <dbs dir> <node GEID> <signing key GRID>

Starting Python orchestrator

    cd orchestrator
    source <virtual enviornment>
    fastapi run main.py
**Basic overview**
- **Core blockchain**: this handles the blocks, resource validation and maintaining the state. Written entitely in Rust
  - **Functions**
    - **Networking:** it is connected to the blockchain p2p network to synchronize state and resources.
    - **State tracking**: it stores and updates the state of the chain, validates resources
  - **Implementation**
    - Networking: rust Libp2p
    - Cryptography: SHA256 (hahsing), ML-KEM (Ciphering), ML-DSA (Signatures)
    - Encoding: Protocol buffers
    - Storage: SQLite + RocksDB
    - Virtual machine: self implemented stack based bytecode interpreter
    - Merkle tree: self implementation
    - State Merkle Patricia Trie: self implementation
    - RocksDB caching: self implementation for sessions and caching on top of RocksDB
- Python orchestraror: this layer provides a API to interact with the chain.
            
**Everything is a resource**

- Similar to the “everything is a file” doctrine of Linux, everything is a resource. With some abstraction and wasted bytes, all resources are fundamentally identical low level.
- Global Resource Identifier (GRID): this is used as a flat ID for all resources. This can be used to perform a quick search or bloom filter to find resources.
    - Data contained
        - The GEID of the entity which owns it 
        - The resource type
        - The resource identifier string
            - Example: FedGov:StateGov::OfficeOfWhatever[KEY]=random
        - Fields: each resource has a collection of key value pairs, which is declard in a rules.json file. These fields are accessed and stored identically for all fields.
            - Field name
            - Field type (string, integer, boolean, etc)
            - Field value (encoded to bytes)
        - Attached resources: it can have nested resources that can be used for validation.
    - Declarative: resources and their behavior are declared using a single file.
- Global Entity Identifier (GEID): this is a flat string that identifies a entity.
    - An entity is a actor on the network who can
        - Own resources
        - Participate in currency (Cash) transfers
        - Consensus: anchor nodes participate in the consensus and validation.
    - Structure: it contains a chain of anchors with the entity name being the last item
        - anchor_chain::entity
        - anchor_1:anchor_2:anchor_3::entity
    - Types of nodes
        - Anchor: an anchor is an entity which has the ability to issue new entity GEID under it's domain
            - Entity management: it can create entities under itself
            - Key management: it manages the keys of entities under it, with the ability to generate and revoke keys.
            - Anchor chains: anchors can also manage sub anchors that form a chain
        - Organization: for now, this is a global role. They can participate in the document, document family and grant system
        - User: these are limited entities who can access resources and interact with contracts
      
**Optimizations:** due to the GEID/GRID introducing performance overhead and reduce statelessness, optimizations ensure that the performance of the system is not too negatively effected

- Bloom filter: each block header contains a bloom filter. The GRID of all on chain modifications are added. A light node can use the bloom filter to determine if a resource was possibly modified in that block. With dynamic sizing, each block can have as low as a 0.1% false positive rate.
    - Addition: +{GRID}
    - Revocation: -{GRID}
- Individual SQLite databases: each block has it’s own SQLite database which stores the resources created or modified in it, the merkle roots and field values
    - Parallel reads: each DB can be loaded to RAM independently and read through in a dedicated thread in parallel. This lets multipel blocks to be scanned at once 
    - As needed download: light nodes can rely on the bloom filter to skip downloading the database for blocks which have nothing of interest and download blocks only if the bloom filter returns a match


**Mutilated Merkle Tree (MMT):** This is a modified Merkle Tree structure where the leaf nodes are placed at variable depths in high density regions. This allows for a node to share a small number of merkle proofs without revealing the size of the tree.
- Merkle tree anchoring: organizations can have their data be kept nice and hidden on their private server, have a local Merkle tree and anchor it to the main chain. But this leaks metadata. The number of nodes in a Merkle tree can give away the number of resources within.
- Access control: nodes can share individual proofs to nodes that request a resource. The whole resource tree is kept private, while the node serves resources and individual poofs over it’s off chain HTTPS API
- The Mutilated Merkle Tree lets nodes share individual proofs without leaking the data size
  - Variable depth: the depth of leaf nodes and thus the size of the proofs is variable. One proof may be 8 levels deep, while another may be 21
  - Ghost nodes: leaf nodes may not contain any data and be dummy nodes.
- Algorithm
  - For each resource being inserted
   - Choose a random integer which will be the depth at which the resource leaf node will be added. 
     - D → Depth
     - 2<D<Dmax
  - Start at root, traverse down
    - Choose either	
        - The only empty node 
        - If both are empty, either the left or right node with the modifier
          - p = (1-s)*s
          - 0<s<1
          - s → ratio of available vs occupied paths
              - If the left node has one empty leaf and right has 2, s for the left node is 0.33 and that for the right is 0.66    

   
**Consensus:**: it uses a mix of Proof of Stake (PoS) and Proof of Work (PoW). Strictly speaking, it is a PoS system which has PoW like attributes as an emergent property

- **Commit**: each node commits at some block N-K. 
    - Select a 256 bit value (V) and 256 bit nonce (N)
    - Get hash of the combined value
      - C = hash(V||N)
    - Sign the combined hash with
      - S = SIGN(C, K)
    - Broadcast a COMMIT message that contains
      - Combined hash (H)
      - Signature (S)
      - Key (K)
      - Other nodes receive the COMMIT message and validate it
    - Check conditions for inclusion
      - Check if K is a ON CHAIN resource that has not been revoked
      - Check if signature S is valid for given hash H with key K
      - Check if the node has had any previous commit
- **Reveal:** nodes reveal their 265 value V with a Nonce NB
    - Create a block head B with resources
    - Nonce NB
    - Merkle root of included resources (M)
    - Pick nonce and root such that the XOR distance (d) between the hash of the header and value V is minimized
        - d = |hash(B), V|

**Executable contracts:** a Domain Specific Language (DSL) which allows for on chain code execution. 
- Language: not implemented
- Virtual machine: a working virtual machine exists which can execute the custom ISA 
    - Execute standard op codes
    - Memory operations
    - Arithmetic operations
    - Cryptographic functions: hashing, ciphers, etc
    - State read and modification: operators that directly perform state change operations such as
        - Transfer currency
        - Issue resources
        - Revoke resources
        - Read account balances
    - Memory and operand addressing: operands are referred to with a 2 value pair in the bytecode, which is evaluated at run time to give the raw operand value
        - Memory/RAM: create and read global or local variables by name.
        - Direct inline: values can be directly defined inline
        - Execution details: this lets the coad read the current execution details like which GEID called it, current block height, account balances, etc.
        - State: all changes during execution occur to a CoW buffer. The buffer stores all writes and is transparently read through to maintain continuity during execution.
          
**Basic overview**

- Client centric: the system relies on a combination of on chain validation and off chain checks.
  - Irrevokability: the design priotirizes proving data exists. The primary concern is the immutability of data
  - Managed chain issuance: all data on the main blockchain is validated and checked before being confirmed. This data is public, always available and guranteed immutable.
  - Decentralized issuance: nodes can issue any resources they want on their local tree. This is inherently impossible to validate and manage.
  - A resource can be packaged to include data like
    - The resource itself
    - Merkle proof, either to a merkle root on chain or to a local merkle root
    - Same for any includes resources like keys, local root, signatures, etc.
  - Client validation: the client device holds this information and can validate the resource as both
    - Guranteed to be issued by that node
    - Guranteed to be intact
    - “Green check”: the user device shows a small icon that acts as a bulletproof check: the resource (ie document) they hold is valid, issued by who it shows and verifiable
    - Currency: it has a currency “cash” (I’m aware of the originality of the name).
      
**Document management**

- Documents: a document is a resource, like any other.
- Issuer (included in the GRID
- Document family: this is a categorization and adressal mechanism. Documents types can be part of a “family”.
  - Controller node: this is the node which owns the document family and has control over things like
  - Rules: define attributes of the documents like
    - Structure: fields, encoding, etc
    - Contracts: executable code to check the validity of the document
    - Mutability: defines the availibility slashing
        - Defines the requirement of a resource to be always available, and slashing punishments if not served. All messages, including not found, are signed by the server or they are rejected by the client.
    - Client reporting address: it contains the address the client can send a copy of a resource issued under a grant if enforcement is set to reactive. This verifies the validity of the response and any automated action/slashing if needed.
- Grants: a grant is a signature which lets the controller of a family grant a different node or document type to issue documents under it.

                    



