#pragma once
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <cstdint>

namespace minisql::sql
{

    enum class StmtKind
    {
        CreateTable,
        Insert,
        Select,
        Begin,
        Commit,
        Rollback,
        Explain,
        Delete,
        Update,
        ShowTables
    };

    inline const char *to_string(StmtKind k)
    {
        switch (k)
        {
        case StmtKind::CreateTable:
            return "CreateTable";
        case StmtKind::Insert:
            return "Insert";
        case StmtKind::Select:
            return "Select";
        case StmtKind::Begin:
            return "Begin";
        case StmtKind::Commit:
            return "Commit";
        case StmtKind::Rollback:
            return "Rollback";
        case StmtKind::Explain:
            return "Explain";
        case StmtKind::Delete:
            return "Delete";
        case StmtKind::Update:
            return "Update";
        case StmtKind::ShowTables:
            return "ShowTables";
        }
        return "Unknown";
    }

    enum class DataType
    {
        Int,
        Text
    };

    struct ColumnDef
    {
        std::string name;
        DataType type;
        bool primary_key = false;
    };

    using Value = std::variant<int64_t, std::string>;

    struct ExprEq
    {
        std::string column;
        Value value;
    };

    struct CreateTableStmt
    {
        std::string table_name;
        std::vector<ColumnDef> columns;
    };

    struct InsertStmt
    {
        std::string table_name;
        std::vector<std::string> columns;
        std::vector<Value> values;
    };

    struct SelectStmt
    {
        std::vector<std::string> columns; // "*" allowed as single entry
        std::string table_name;
        std::optional<ExprEq> where_eq; // only col = value for now
    };

    struct ExplainStmt
    {
        SelectStmt select; // only EXPLAIN SELECT ...
    };

    // DELETE FROM table WHERE col = value
    struct DeleteStmt
    {
        std::string table_name;
        std::optional<ExprEq> where_eq;
    };

    // UPDATE table SET col = value WHERE col = value
    struct UpdateStmt
    {
        std::string table_name;
        std::string set_column;
        Value set_value;
        std::optional<ExprEq> where_eq;
    };

    // SHOW TABLES
    struct ShowTablesStmt
    {
    };

    struct Stmt
    {
        StmtKind kind;
        std::variant<CreateTableStmt, InsertStmt, SelectStmt, ExplainStmt,
                     DeleteStmt, UpdateStmt, ShowTablesStmt, std::monostate>
            node;
    };

} // namespace minisql::sql