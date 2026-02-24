#pragma once
#include <string>

#include "engine/catalog.h"
#include "sql/ast.h"

namespace minisql::engine
{

    using namespace std;
    using namespace minisql::sql;

    class Executor
    {
    public:
        // data_dir: jahan catalog.cat aur <table>.db files stored honge
        explicit Executor(const string &data_dir = ".");

        // Koi bhi parsed statement execute karo
        void execute(const Stmt &stmt);

        Catalog catalog;

    private:
        string data_dir_;

        string catalogPath() const;
        string tablePath(const string &name) const;

        void execCreateTable(const CreateTableStmt &s);
        void execInsert(const InsertStmt &s);
        void execSelect(const SelectStmt &s);
    };

} // namespace minisql::engine
