
# Ethereum Blockchain Toolkit

A collection of Python tools for Ethereum blockchain analysis.

This repository currently includes:

* **Ethereum Public Key Finder**
* **Ethereum Addresses Extractor**
* **Ethereum TXID Extractor**

These tools are designed for blockchain research, data analysis, and educational purposes.

---

# Features

## Ethereum Public Key Finder

Recovers Ethereum **public keys** from addresses that have previously signed outgoing transactions.

### Features

* Reads addresses from `addresses.txt`
* Automatically finds the first outgoing transaction
* Recovers the public key from the ECDSA signature
* Verifies that the recovered public key matches the original address
* Supports multiple public Ethereum RPC endpoints
* Automatic failover between RPC providers
* Multi-threaded processing
* Beautiful real-time terminal dashboard
* Automatically saves results to `public.txt`

Run:

```bash
python ethereum_public_key_finder.py
```

Input:

```
addresses.txt
```

Output:

```
public.txt
not_found.txt
errors.txt
```

---

## Ethereum Addresses Extractor

Extracts every Ethereum address found while scanning blockchain transactions.

### Features

* Multi-threaded extraction
* Automatic RPC failover
* Duplicate removal
* Real-time progress display
* Automatic saving

Run:

```bash
python ethereum_addresses_extractor.py
```

Output:

```
addresses.txt
```

---

## Ethereum TXID Extractor

Extracts transaction hashes (TXIDs) from Ethereum blocks.

### Features

* Block-by-block scanning
* Multi-threaded processing
* Automatic RPC switching
* High-speed extraction
* Real-time dashboard

Run:

```bash
python ethereum_txid_extractor.py
```

Output:

```
txids.txt
```

---

# Installation

Clone the repository:

```bash
git clone https://github.com/rijol95-web3/ethereum-tools.git

cd ethereum-tools
```

Install dependencies:

```bash
pip install -r requirements.txt
```

or

```bash
pip install requests eth-keys eth-utils rlp rich
```

---

# Requirements

* Python 3.10+
* Internet connection
* Public Ethereum RPC endpoints

---

# Repository Structure

```
.
├── ethereum_public_key_finder.py
├── ethereum_addresses_extractor.py
├── ethereum_txid_extractor.py
├── addresses.txt
├── public.txt
├── txids.txt
├── not_found.txt
├── errors.txt
├── requirements.txt
└── README.md
```

---

# Output Files

### Ethereum Public Key Finder

| File          | Description                               |
| ------------- | ----------------------------------------- |
| public.txt    | Recovered public keys                     |
| not_found.txt | Addresses without recoverable public keys |
| errors.txt    | Processing errors                         |

### Ethereum Addresses Extractor

| File          | Description                  |
| ------------- | ---------------------------- |
| addresses.txt | Extracted Ethereum addresses |

### Ethereum TXID Extractor

| File      | Description                  |
| --------- | ---------------------------- |
| txids.txt | Extracted transaction hashes |

---

# Performance

* Multi-threaded architecture
* Automatic RPC load balancing
* Automatic retry mechanism
* Automatic rate-limit handling
* Real-time progress dashboard
* Resume support
* Low memory usage

---

# Terminal Dashboard

The Public Key Finder displays a live dashboard including:

* Addresses processed
* Public keys recovered
* Addresses not found
* Processing speed
* Elapsed time
* Estimated time remaining (ETA)
* Active workers
* Active RPC endpoints
* Explorer requests
* RPC requests
* Current address
* Latest transaction
* Latest result

The dashboard updates in place, keeping the terminal clean without continuously scrolling.

---

# Disclaimer

This software is intended for blockchain research, educational purposes, interoperability testing, and analysis of publicly available blockchain data.

It does **not** bypass cryptographic security, recover private keys, or access non-public information.

---

# License

MIT License

Feel free to use, modify, and contribute.
