<div align="center">

# ⚡ lifeDb

### A Mini Relational Database Engine — Built from Scratch in C++17

[![Language](https://img.shields.io/badge/Language-C%2B%2B17-blue?style=for-the-badge&logo=cplusplus)](https://isocpp.org/)
[![Build](https://img.shields.io/badge/Build-CMake-red?style=for-the-badge&logo=cmake)](https://cmake.org/)
[![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey?style=for-the-badge&logo=windows)](https://www.microsoft.com/windows)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)]()

> *"If you want to understand databases — build one."*

</div>

---

## 🧠 What is lifeDb?

**lifeDb** is a SQLite-inspired, file-based relational database engine written entirely in **C++17 from scratch** — no libraries, no shortcuts.

It supports a **standard SQL subset**, persistent on-disk storage using **4KB pages**, a **B+Tree index** on primary keys, **EXPLAIN query plans**, and **WAL-backed transactions** with crash recovery.

---

## ✨ Features

| Feature | Description |
|--------|-------------|
| 📝 **SQL Parser** | Recursive-descent lexer + parser for standard SQL |
| 💾 **Page Storage** | 4KB page-based heap file (`.db`) |
| 🔢 **Row Codec** | Binary row serialization — `INT` (8B) + `TEXT` (len-prefix) |
| 📋 **Catalog** | Schema persistence across restarts (`.cat`) |
| 🌳 **B+Tree Index** | Primary key index for `O(log n)` lookups |
| 🔍 **EXPLAIN** | Query plan output — `SeqScan` vs `IndexScan` |
| 🔐 **WAL Transactions** | `BEGIN / COMMIT / ROLLBACK` with append-only log |
| 🛡️ **Crash Recovery** | On startup: committed txns replayed, uncommitted ignored |

---

## 🗄️ Supported SQL

```sql
-- DDL
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);

-- DML
INSERT INTO users (id, name) VALUES (1, 'Aman');

-- Queries
SELECT * FROM users;
SELECT id, name FROM users WHERE id = 1;

-- Query Plan
EXPLAIN SELECT * FROM users WHERE id = 1;

-- Transactions
BEGIN;
INSERT INTO users (id, name) VALUES (2, 'Bob');
COMMIT;

ROLLBACK;

-- Meta
.tables
.exit
```

---

## 🏗️ Architecture

```
SQL Input
    │
    ▼
┌─────────────┐
│   Lexer     │  tokenizes SQL string → tokens
└──────┬──────┘
       │
    ▼
┌─────────────┐
│   Parser    │  recursive-descent → AST
└──────┬──────┘
       │
    ▼
┌─────────────┐
│   Planner   │  SeqScan or IndexScan?
└──────┬──────┘
       │
    ▼
┌─────────────────────────────────┐
│           Executor              │
│  CREATE / INSERT / SELECT /     │
│  EXPLAIN / BEGIN / COMMIT       │
└────┬──────────┬─────────────────┘
     │          │
     ▼          ▼
┌─────────┐  ┌──────────┐
│  Pager  │  │  BTree   │
│ (pages) │  │ (index)  │
└────┬────┘  └──────────┘
     │
     ▼
┌──────────┐  ┌──────────┐
│  .db     │  │  .wal    │
│  file    │  │  file    │
└──────────┘  └──────────┘
```

---

## 📁 Project Structure

```
lifeDb/
├── CMakeLists.txt
└── src/
    ├── main.cpp                  # REPL entry point
    ├── sql/
    │   ├── ast.h                 # AST node definitions
    │   ├── lexer.h / lexer.cpp   # Tokenizer
    │   └── parser.h / parser.cpp # Recursive-descent parser
    └── engine/
        ├── catalog.h / .cpp      # Schema: save/load (binary)
        ├── row_codec.h / .cpp    # Row encode/decode
        ├── pager.h / .cpp        # 4KB page I/O
        ├── btree.h / .cpp        # B+Tree index
        ├── wal.h / .cpp          # Write-Ahead Log
        └── executor.h / .cpp     # Statement execution
```

---

## 🚀 Build & Run

### Requirements
- `g++` (MinGW-w64) with C++17
- `CMake 3.20+`

### Build

```bat
cmake -S . -B build -G "MinGW Makefiles"
cmake --build build -j
```

### Run

```bat
.\build\minisql.exe
```

---

## 🎬 Demo

```
MiniSQL> CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
OK: table 'users' created.

MiniSQL> INSERT INTO users (id, name) VALUES (1, 'Aman');
OK: 1 row inserted.

MiniSQL> INSERT INTO users (id, name) VALUES (2, 'Bob');
OK: 1 row inserted.

MiniSQL> SELECT * FROM users;
+----+------+
| id | name |
+----+------+
|  1 | Aman |
|  2 | Bob  |
+----+------+
2 rows returned.

MiniSQL> EXPLAIN SELECT * FROM users WHERE id = 1;
>> Plan: IndexScan on 'users' using PRIMARY KEY (id = 1)

MiniSQL> SELECT * FROM users WHERE id = 1;
+----+------+
| id | name |
+----+------+
|  1 | Aman |
+----+------+
1 row returned.

MiniSQL> BEGIN;
MiniSQL> INSERT INTO users (id, name) VALUES (3, 'Charlie');
MiniSQL> ROLLBACK;

MiniSQL> SELECT * FROM users;
+----+------+
| id | name |
+----+------+
|  1 | Aman |
|  2 | Bob  |
+----+------+
2 rows returned.  ← Charlie never committed ✓
```

---

## 💡 Key Concepts Implemented

```
Binary File I/O       →  fstream, fixed-width pages, offset arithmetic
Row Serialization     →  variant<int64_t, string>, reinterpret_cast, memcpy
B+Tree                →  node split, leaf linked list, O(log n) search
WAL                   →  append-only log, txn_id tracking, replay on recovery
Crash Recovery        →  scan WAL → apply committed → discard uncommitted
Query Planning        →  rule-based: WHERE pk=const + index exists → IndexScan
```

---

## 📊 Performance (rough)

| Operation | Without Index | With B+Tree Index |
|-----------|:-------------:|:-----------------:|
| INSERT    | O(1) append   | O(log n) + append |
| SELECT *  | O(n) scan     | O(n) scan         |
| WHERE pk= | O(n) scan     | **O(log n)**      |

---

## 🗺️ Roadmap

- [x] SQL Lexer + Parser (AST)
- [x] Persistent page storage
- [x] Row binary codec
- [x] Schema catalog
- [x] INSERT / SELECT / WHERE
- [x] EXPLAIN (SeqScan / IndexScan)
- [x] B+Tree index on primary key
- [x] WAL transactions + crash recovery
- [ ] DELETE / UPDATE
- [ ] Multi-table JOIN (future)
- [ ] Buffer pool eviction (future)

---

## 👨‍💻 Author

Built with ❤️ as a systems programming project to deeply understand how relational databases work internally.

> *Every query you've ever run on MySQL or PostgreSQL goes through these exact same stages — just at scale.*

---

<div align="center">

**If this project helped you understand databases better — give it a ⭐**

</div>