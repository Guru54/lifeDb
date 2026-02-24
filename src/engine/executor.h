#pragma once
#include <string>
#include <map>
#include <memory>
#include "engine/catalog.h"
#include "engine/btree.h"
#include "sql/ast.h"

namespace minisql::engine {

class Executor {
public:
    explicit Executor(const std::string& data_dir = ".");
    void execute(const minisql::sql::Stmt& stmt);

    Catalog catalog;

private:
    std::string data_dir_;

    std::string catalogPath() const;
    std::string tablePath(const std::string& name) const;
    std::string indexPath(const std::string& name) const;
    BTree* openIndex(const std::string& table_name);

    void execCreateTable(const minisql::sql::CreateTableStmt& s);
    void execInsert    (const minisql::sql::InsertStmt& s);
    void execSelect    (const minisql::sql::SelectStmt& s);

    std::map<std::string, std::unique_ptr<BTree>> indices_;
};

} // namespace minisql::engine
