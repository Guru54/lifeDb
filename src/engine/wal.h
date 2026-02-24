#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>

namespace minisql::engine {

// ── Write-Ahead Log ────────────────────────────────────────────────────────────
// Log record format (binary):
//   [txn_id : 8 bytes]
//   [op     : 1 byte ]   1=Begin 2=Insert 3=Commit 4=Rollback
//   [payload_len : 4 bytes]
//   [payload : N bytes]
//
// Insert payload: [table_len:2][table:N][row_len:4][row:N]
// Begin/Commit/Rollback payload: empty (payload_len = 0)
// ──────────────────────────────────────────────────────────────────────────────

class Wal {
public:
    enum class Op : uint8_t { Begin = 1, Insert = 2, Commit = 3, Rollback = 4 };

    struct Entry {
        uint64_t             txn_id  = 0;
        Op                   op      = Op::Begin;
        std::string          table;        // filled for Insert
        std::vector<uint8_t> row_data;     // filled for Insert
    };

    explicit Wal(const std::string& filepath);
    ~Wal();

    void logBegin   (uint64_t txn_id);
    void logInsert  (uint64_t txn_id, const std::string& table,
                     const std::vector<uint8_t>& row_data);
    void logCommit  (uint64_t txn_id);
    void logRollback(uint64_t txn_id);

    // Read all entries — used for crash recovery on startup
    std::vector<Entry> readAll();

private:
    void appendEntry(const Entry& e);

    std::string  filepath_;
    std::fstream file_;
};

} // namespace minisql::engine
