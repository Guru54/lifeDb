#include "engine/executor.h"
#include "engine/row_codec.h"
#include "engine/pager.h"

#include <iostream>
#include <variant>
#include <stdexcept>
#include <filesystem>

namespace minisql::engine
{

    using namespace std;
    using namespace minisql::sql;
    namespace fs = filesystem;

    // ── Constructor: catalog load karo ───────────────────────────────────────────
    Executor::Executor(const string &data_dir) : data_dir_(data_dir)
    {
        fs::create_directories(data_dir_);
        string cp = catalogPath();
        if (fs::exists(cp))
            catalog.loadFromDisk(cp);
    }

    string Executor::catalogPath() const { return data_dir_ + "/catalog.cat"; }
    string Executor::tablePath(const string &name) const { return data_dir_ + "/" + name + ".db"; }

    // ── Dispatch ─────────────────────────────────────────────────────────────────
    void Executor::execute(const Stmt &stmt)
    {
        switch (stmt.kind)
        {
        case StmtKind::CreateTable:
            execCreateTable(get<CreateTableStmt>(stmt.node));
            break;
        case StmtKind::Insert:
            execInsert(get<InsertStmt>(stmt.node));
            break;
        case StmtKind::Select:
            execSelect(get<SelectStmt>(stmt.node));
            break;
        case StmtKind::Begin:
            cout << "BEGIN  (transactions coming soon)\n";
            break;
        case StmtKind::Commit:
            cout << "COMMIT (transactions coming soon)\n";
            break;
        case StmtKind::Rollback:
            cout << "ROLLBACK (transactions coming soon)\n";
            break;
        case StmtKind::Explain:
        {
            const auto &ex = get<ExplainStmt>(stmt.node);
            cout << "EXPLAIN: SeqScan on table '" << ex.select.table_name << "'\n";
            break;
        }
        default:
            throw runtime_error("executor: unsupported statement");
        }
    }

    // ── CREATE TABLE ─────────────────────────────────────────────────────────────
    void Executor::execCreateTable(const CreateTableStmt &s)
    {
        if (catalog.getTable(s.table_name))
            throw runtime_error("Table already exists: " + s.table_name);

        TableMeta meta;
        meta.table_name = s.table_name;
        for (auto &col : s.columns)
        {
            ColumnMeta cm;
            cm.name = col.name;
            cm.type = col.type;
            cm.is_primary_key = col.primary_key;
            meta.columns.push_back(cm);
        }

        catalog.createTable(meta);
        catalog.saveToDisk(catalogPath());

        // Table ke liye blank .db file banao
        Pager pager(tablePath(s.table_name));

        cout << "Table '" << s.table_name << "' created.\n";
    }

    // ── INSERT ────────────────────────────────────────────────────────────────────
    void Executor::execInsert(const InsertStmt &s)
    {
        TableMeta *meta = catalog.getTable(s.table_name);
        if (!meta)
            throw runtime_error("Table not found: " + s.table_name);

        if (s.columns.size() != meta->columns.size())
            throw runtime_error("INSERT: column count mismatch (schema has " + std::to_string(meta->columns.size()) + " cols)");
        if (s.values.size() != s.columns.size())
            throw runtime_error("INSERT: values count != columns count");

        // Values ko schema order mein arrange karo
        vector<Value> ordered(meta->columns.size());
        for (size_t i = 0; i < s.columns.size(); i++)
        {
            bool found = false;
            for (size_t j = 0; j < meta->columns.size(); j++)
            {
                if (meta->columns[j].name == s.columns[i])
                {
                    ordered[j] = s.values[i];
                    found = true;
                    break;
                }
            }
            if (!found)
                throw runtime_error("INSERT: unknown column '" + s.columns[i] + "'");
        }

        // Type check
        for (size_t i = 0; i < meta->columns.size(); i++)
        {
            if (meta->columns[i].type == DataType::Int && !holds_alternative<int64_t>(ordered[i]))
                throw runtime_error("INSERT: expected INT for column '" + meta->columns[i].name + "'");
            if (meta->columns[i].type == DataType::Text && !holds_alternative<string>(ordered[i]))
                throw runtime_error("INSERT: expected TEXT for column '" + meta->columns[i].name + "'");
        }

        auto encoded = encodeRow(*meta, ordered);

        Pager pager(tablePath(s.table_name));
        pager.appendRow(encoded);

        cout << "1 row inserted.\n";
    }

    // ── SELECT ────────────────────────────────────────────────────────────────────
    void Executor::execSelect(const SelectStmt &s)
    {
        TableMeta *meta = catalog.getTable(s.table_name);
        if (!meta)
            throw runtime_error("Table not found: " + s.table_name);

        // Print karne wale column indices decide karo
        vector<size_t> col_idx;
        if (s.columns.size() == 1 && s.columns[0] == "*")
        {
            for (size_t i = 0; i < meta->columns.size(); i++)
                col_idx.push_back(i);
        }
        else
        {
            for (auto &cname : s.columns)
            {
                bool found = false;
                for (size_t i = 0; i < meta->columns.size(); i++)
                {
                    if (meta->columns[i].name == cname)
                    {
                        col_idx.push_back(i);
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw runtime_error("SELECT: unknown column '" + cname + "'");
            }
        }

        // Header line print karo
        for (size_t k = 0; k < col_idx.size(); k++)
        {
            if (k)
                cout << " | ";
            cout << meta->columns[col_idx[k]].name;
        }
        cout << "\n"
             << string(40, '-') << "\n";

        int row_count = 0;
        Pager pager(tablePath(s.table_name));

        pager.scan([&](const uint8_t *data, uint32_t len)
                   {
        auto row = decodeRow(*meta, data, len);

        // WHERE filter (sirf col = value supported)
        if (s.where_eq.has_value()) {
            const auto& eq = s.where_eq.value();
            for (size_t i = 0; i < meta->columns.size(); i++) {
                if (meta->columns[i].name == eq.column) {
                    if (row[i] != eq.value) return;
                    break;
                }
            }
        }

        // Row print
        for (size_t k = 0; k < col_idx.size(); k++) {
            if (k) cout << " | ";
            visit([](auto& v) { cout << v; }, row[col_idx[k]]);
        }
        cout << "\n";
        row_count++; });

        cout << "(" << row_count << (row_count == 1 ? " row)\n" : " rows)\n");
    }

} // namespace minisql::engine
