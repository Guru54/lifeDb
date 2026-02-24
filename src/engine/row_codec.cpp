#include "engine/row_codec.h"

#include <cstring>
#include <stdexcept>

namespace minisql::engine
{

    using namespace std;
    using minisql::sql::DataType;
    using minisql::sql::Value;

    // ── Encode ────────────────────────────────────────────────────────────────────
    vector<uint8_t> encodeRow(const TableMeta &meta, const vector<Value> &row)
    {
        if (row.size() != meta.columns.size())
            throw runtime_error("encodeRow: column count mismatch");

        vector<uint8_t> buf;

        for (size_t i = 0; i < meta.columns.size(); i++)
        {
            if (meta.columns[i].type == DataType::Int)
            {
                // INT: 8 bytes little-endian
                int64_t v = get<int64_t>(row[i]);
                uint8_t b[8];
                memcpy(b, &v, 8);
                buf.insert(buf.end(), b, b + 8);
            }
            else
            {
                // TEXT: [4-byte length][bytes]
                const string &s = get<string>(row[i]);
                uint32_t len = static_cast<uint32_t>(s.size());
                uint8_t lb[4];
                memcpy(lb, &len, 4);
                buf.insert(buf.end(), lb, lb + 4);
                buf.insert(buf.end(), s.begin(), s.end());
            }
        }

        return buf;
    }

    // ── Decode ────────────────────────────────────────────────────────────────────
    vector<Value> decodeRow(const TableMeta &meta, const uint8_t *data, size_t len)
    {
        vector<Value> row;
        size_t off = 0;

        for (auto &col : meta.columns)
        {
            if (col.type == DataType::Int)
            {
                if (off + 8 > len)
                    throw runtime_error("decodeRow: buffer too short for INT");
                int64_t v;
                memcpy(&v, data + off, 8);
                off += 8;
                row.push_back(v);
            }
            else
            {
                if (off + 4 > len)
                    throw runtime_error("decodeRow: buffer too short for TEXT len");
                uint32_t slen;
                memcpy(&slen, data + off, 4);
                off += 4;
                if (off + slen > len)
                    throw runtime_error("decodeRow: buffer too short for TEXT data");
                string s(reinterpret_cast<const char *>(data + off), slen);
                off += slen;
                row.push_back(s);
            }
        }

        return row;
    }

} // namespace minisql::engine
