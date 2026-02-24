#pragma once
#include <string>
#include <map>
#include <memory>
#include <vector>
#include <utility>
#include "engine/catalog.h"
#include "engine/btree.h"
#include "engine/wal.h"
#include "sql/ast.h"

namespace minisql::engine
{

    class Executor
    {
    public:
        explicit Executor(const std::string &data_dir = ".");
        void execute(const minisql::sql::Stmt &stmt);

        Catalog catalog;

    private:
        std::string data_dir_;

        std::string catalogPath() const;
        std::string tablePath(const std::string &name) const;
        std::string indexPath(const std::string &name) const;
        std::string walPath() const;
        BTree *openIndex(const std::string &table_name);

        void execCreateTable(const minisql::sql::CreateTableStmt &s);
        void execInsert(const minisql::sql::InsertStmt &s);
        void execSelect(const minisql::sql::SelectStmt &s);
        void execDelete(const minisql::sql::DeleteStmt &s);
        void execUpdate(const minisql::sql::UpdateStmt &s);
        void execBegin();
        void execCommit();
        void execRollback();
        void execShowTables();

        // WAL crash recovery: replay committed txns on startup
        void recoverFromWal();

        std::map<std::string, std::unique_ptr<BTree>> indices_;

        // Transaction state
        bool txn_active_ = false;
        uint64_t txn_id_ = 0;
        // Pending inserts: {table_name, encoded_row}
        std::vector<std::pair<std::string, std::vector<uint8_t>>> pending_;
        std::unique_ptr<Wal> wal_;
    };

} // namespace minisql::engine
