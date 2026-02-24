#pragma once
#include <string>
#include <vector>
#include <map>

#include "sql/ast.h" // DataType reuse karte hain (Int / Text)

namespace minisql::engine
{

    using namespace std;
    using minisql::sql::DataType;

    // ── Column ki metadata ───────────────────────────────────────────────────────
    struct ColumnMeta
    {
        string name;
        DataType type;
        bool is_primary_key = false;
    };

    // ── Ek table ki poori schema ─────────────────────────────────────────────────
    struct TableMeta
    {
        string table_name;
        vector<ColumnMeta> columns;
    };

    // ── Catalog: sabhi tables ki schema + disk save/load ─────────────────────────
    class Catalog
    {
    public:
        // memory mein table add karo
        void createTable(TableMeta meta);

        // table dhundo; nullptr agar nahi mila
        TableMeta *getTable(const string &name);

        // binary file mein poora catalog likho
        void saveToDisk(const string &filepath);

        // binary file se poora catalog wapas pado
        void loadFromDisk(const string &filepath);

        // saari tables (baaki layers ko chahiye)
        map<string, TableMeta> tables;
    };

} // namespace minisql::engine
