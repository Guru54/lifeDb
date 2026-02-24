#include "engine/executor.h"
#include "engine/row_codec.h"
#include "engine/pager.h"
#include "engine/btree.h"

#include <iostream>
#include <variant>
#include <stdexcept>
#include <filesystem>

namespace minisql::engine
{

    using namespace std;
    using namespace minisql::sql;
    namespace fs = filesystem;

    // ── Constructor: catalog load + WAL recovery ──────────────────────────────
    Executor::Executor(const string &data_dir) : data_dir_(data_dir)
    {
        fs::create_directories(data_dir_);
        string cp = catalogPath();
        if (fs::exists(cp))
            catalog.loadFromDisk(cp);
        wal_ = make_unique<Wal>(walPath());
        recoverFromWal();
    }

    string Executor::catalogPath() const { return data_dir_ + "/catalog.cat"; }
    string Executor::tablePath(const string &name) const { return data_dir_ + "/" + name + ".db"; }
    string Executor::indexPath(const string &name) const { return data_dir_ + "/" + name + ".idx"; }
    string Executor::walPath()   const { return data_dir_ + "/wal.log"; }

    BTree *Executor::openIndex(const string &tname)
    {
        // Check if table has a primary key
        TableMeta *meta = catalog.getTable(tname);
        if (!meta)
            return nullptr;
        bool has_pk = false;
        for (auto &c : meta->columns)
            if (c.is_primary_key && c.type == DataType::Int)
            {
                has_pk = true;
                break;
            }
        if (!has_pk)
            return nullptr;
        // Return cached or open new
        auto it = indices_.find(tname);
        if (it != indices_.end())
            return it->second.get();
        auto bt = make_unique<BTree>(indexPath(tname));
        BTree *ptr = bt.get();
        indices_[tname] = move(bt);
        return ptr;
    }

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
            execBegin();
            break;
        case StmtKind::Commit:
            execCommit();
            break;
        case StmtKind::Rollback:
            execRollback();
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

        // Blank .db file
        Pager pager(tablePath(s.table_name));

        // Create .idx file if table has an INT primary key
        openIndex(s.table_name);

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

        // If a transaction is active, buffer the insert and log to WAL
        if (txn_active_) {
            wal_->logInsert(txn_id_, s.table_name, encoded);
            pending_.push_back({s.table_name, encoded});
            cout << "1 row buffered (pending COMMIT).\n";
            return;
        }

        Pager pager(tablePath(s.table_name));
        auto loc = pager.appendRow(encoded);

        // Update B+Tree index on INT primary key column
        BTree *idx = openIndex(s.table_name);
        if (idx)
        {
            for (size_t i = 0; i < meta->columns.size(); i++)
            {
                if (meta->columns[i].is_primary_key && meta->columns[i].type == DataType::Int)
                {
                    idx->insert(get<int64_t>(ordered[i]), loc.page_id, loc.row_off);
                    break;
                }
            }
        }

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

        // Index-assisted point lookup when WHERE is on INT primary key
        BTree *idx = openIndex(s.table_name);
        if (idx && s.where_eq.has_value())
        {
            const auto &eq = s.where_eq.value();
            int pk_col = -1;
            for (size_t i = 0; i < meta->columns.size(); i++)
                if (meta->columns[i].name == eq.column && meta->columns[i].is_primary_key && meta->columns[i].type == DataType::Int)
                {
                    pk_col = (int)i;
                    break;
                }

            if (pk_col >= 0 && holds_alternative<int64_t>(eq.value))
            {
                BTreeEntry entry;
                if (idx->find(get<int64_t>(eq.value), entry))
                {
                    // Read the specific page + offset directly
                    auto page = pager.readPage(entry.page_id);
                    uint32_t row_len = 0;
                    memcpy(&row_len, page.data() + 2 + entry.row_off + 1, 4);
                    auto row = decodeRow(*meta, page.data() + 2 + entry.row_off + 5, row_len);
                    for (size_t k = 0; k < col_idx.size(); k++)
                    {
                        if (k)
                            cout << " | ";
                        visit([](auto &v)
                              { cout << v; }, row[col_idx[k]]);
                    }
                    cout << "\n";
                    row_count = 1;
                }
                cout << "(" << row_count << (row_count == 1 ? " row)\n" : " rows)\n");
                return;
            }
        }

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

    // ── BEGIN ─────────────────────────────────────────────────────────────────
    void Executor::execBegin() {
        if (txn_active_) { cout << "ERROR: already in a transaction.\n"; return; }
        txn_active_ = true;
        txn_id_++;
        pending_.clear();
        wal_->logBegin(txn_id_);
        cout << "BEGIN\n";
    }

    // ── COMMIT ────────────────────────────────────────────────────────────────
    void Executor::execCommit() {
        if (!txn_active_) { cout << "ERROR: no active transaction.\n"; return; }
        // Apply all pending inserts to pager + index
        for (auto& [tname, encoded] : pending_) {
            TableMeta* meta = catalog.getTable(tname);
            if (!meta) continue;
            Pager pager(tablePath(tname));
            auto loc = pager.appendRow(encoded);
            BTree* idx = openIndex(tname);
            if (idx) {
                auto row_vals = decodeRow(*meta, encoded.data(), encoded.size());
                for (size_t i = 0; i < meta->columns.size(); i++) {
                    if (meta->columns[i].is_primary_key &&
                        meta->columns[i].type == DataType::Int) {
                        idx->insert(get<int64_t>(row_vals[i]), loc.page_id, loc.row_off);
                        break;
                    }
                }
            }
        }
        wal_->logCommit(txn_id_);
        pending_.clear();
        txn_active_ = false;
        cout << "COMMIT\n";
    }

    // ── ROLLBACK ──────────────────────────────────────────────────────────────
    void Executor::execRollback() {
        if (!txn_active_) { cout << "ERROR: no active transaction.\n"; return; }
        wal_->logRollback(txn_id_);
        pending_.clear();
        txn_active_ = false;
        cout << "ROLLBACK\n";
    }

    // ── WAL crash recovery ────────────────────────────────────────────────────
    // Replay any committed transactions whose inserts weren't yet applied.
    // Strategy: collect inserts per txn_id; apply those with a Commit record
    // that have no matching data already (idempotent: best-effort, skips
    // re-apply since pager rows are not deduplicated here; for a school
    // project we just skip replaying WAL if index says key already exists).
    void Executor::recoverFromWal() {
        auto entries = wal_->readAll();
        if (entries.empty()) return;

        // Group inserts by txn_id, track which txns committed/rolled-back
        map<uint64_t, vector<pair<string, vector<uint8_t>>>> txn_inserts;
        map<uint64_t, bool> committed;

        for (auto& e : entries) {
            if (e.op == Wal::Op::Insert) {
                txn_inserts[e.txn_id].push_back({e.table, e.row_data});
            } else if (e.op == Wal::Op::Commit) {
                committed[e.txn_id] = true;
            } else if (e.op == Wal::Op::Rollback) {
                committed[e.txn_id] = false;
            }
        }

        // Update next txn_id_ so we don't reuse old ids
        for (auto& [id, _] : committed) if (id >= txn_id_) txn_id_ = id;

        // For each committed txn_id, try to replay inserts
        for (auto& [tid, rows] : txn_inserts) {
            if (!committed.count(tid) || !committed[tid]) continue;
            for (auto& [tname, encoded] : rows) {
                TableMeta* meta = catalog.getTable(tname);
                if (!meta) continue;
                // Check if this row is already present via index key lookup
                auto row_vals = decodeRow(*meta, encoded.data(), encoded.size());
                BTree* idx = openIndex(tname);
                if (idx) {
                    for (size_t i = 0; i < meta->columns.size(); i++) {
                        if (meta->columns[i].is_primary_key &&
                            meta->columns[i].type == DataType::Int) {
                            BTreeEntry dummy;
                            int64_t pk = get<int64_t>(row_vals[i]);
                            if (idx->find(pk, dummy)) goto next_row; // already applied
                            break;
                        }
                    }
                }
                {
                    Pager pager(tablePath(tname));
                    auto loc = pager.appendRow(encoded);
                    if (idx) {
                        for (size_t i = 0; i < meta->columns.size(); i++) {
                            if (meta->columns[i].is_primary_key &&
                                meta->columns[i].type == DataType::Int) {
                                idx->insert(get<int64_t>(row_vals[i]), loc.page_id, loc.row_off);
                                break;
                            }
                        }
                    }
                }
                next_row:;
            }
        }
    }

} // namespace minisql::engine
