#include "engine/catalog.h"

#include <fstream>
#include <stdexcept>

namespace minisql::engine
{

    using namespace std;

    // ── helper: string binary mein likhna ────────────────────────────────────────
    static void write_string(ofstream &f, const string &s)
    {
        uint32_t len = static_cast<uint32_t>(s.size());
        f.write(reinterpret_cast<char *>(&len), sizeof(len));
        f.write(s.data(), len);
    }

    // ── helper: string binary se padhna ─────────────────────────────────────────
    static string read_string(ifstream &f)
    {
        uint32_t len = 0;
        f.read(reinterpret_cast<char *>(&len), sizeof(len));
        string s(len, '\0');
        f.read(s.data(), len);
        return s;
    }

    // ─────────────────────────────────────────────────────────────────────────────

    void Catalog::createTable(TableMeta meta)
    {
        tables[meta.table_name] = move(meta);
    }

    TableMeta *Catalog::getTable(const string &name)
    {
        auto it = tables.find(name);
        if (it == tables.end())
            return nullptr;
        return &it->second;
    }

    void Catalog::saveToDisk(const string &filepath)
    {
        ofstream f(filepath, ios::binary | ios::trunc);
        if (!f)
            throw runtime_error("catalog: cannot open file for writing: " + filepath);

        // [num_tables: 4 bytes]
        uint32_t num_tables = static_cast<uint32_t>(tables.size());
        f.write(reinterpret_cast<char *>(&num_tables), sizeof(num_tables));

        for (auto &[tname, tmeta] : tables)
        {
            // [table_name]
            write_string(f, tmeta.table_name);

            // [num_columns: 4 bytes]
            uint32_t num_cols = static_cast<uint32_t>(tmeta.columns.size());
            f.write(reinterpret_cast<char *>(&num_cols), sizeof(num_cols));

            for (auto &col : tmeta.columns)
            {
                // [col_name]
                write_string(f, col.name);

                // [col_type: 1 byte]  0=INT, 1=TEXT
                uint8_t type_byte = (col.type == DataType::Int) ? 0 : 1;
                f.write(reinterpret_cast<char *>(&type_byte), sizeof(type_byte));

                // [is_pk: 1 byte]
                uint8_t pk_byte = col.is_primary_key ? 1 : 0;
                f.write(reinterpret_cast<char *>(&pk_byte), sizeof(pk_byte));
            }
        }
    }

    void Catalog::loadFromDisk(const string &filepath)
    {
        ifstream f(filepath, ios::binary);
        if (!f)
            throw runtime_error("catalog: cannot open file for reading: " + filepath);

        tables.clear();

        uint32_t num_tables = 0;
        f.read(reinterpret_cast<char *>(&num_tables), sizeof(num_tables));

        for (uint32_t i = 0; i < num_tables; i++)
        {
            TableMeta tmeta;
            tmeta.table_name = read_string(f);

            uint32_t num_cols = 0;
            f.read(reinterpret_cast<char *>(&num_cols), sizeof(num_cols));

            for (uint32_t j = 0; j < num_cols; j++)
            {
                ColumnMeta col;
                col.name = read_string(f);

                uint8_t type_byte = 0;
                f.read(reinterpret_cast<char *>(&type_byte), sizeof(type_byte));
                col.type = (type_byte == 0) ? DataType::Int : DataType::Text;

                uint8_t pk_byte = 0;
                f.read(reinterpret_cast<char *>(&pk_byte), sizeof(pk_byte));
                col.is_primary_key = (pk_byte == 1);

                tmeta.columns.push_back(move(col));
            }

            tables[tmeta.table_name] = move(tmeta);
        }
    }

} // namespace minisql::engine
