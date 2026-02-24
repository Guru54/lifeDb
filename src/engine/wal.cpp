#include "engine/wal.h"

#include <cstring>
#include <stdexcept>

namespace minisql::engine
{

    using namespace std;

    // ── Constructor / Destructor ──────────────────────────────────────────────────
    Wal::Wal(const string &filepath) : filepath_(filepath)
    {
        file_.open(filepath, ios::binary | ios::in | ios::out | ios::app);
        if (!file_.is_open())
        {
            {
                ofstream tmp(filepath, ios::binary | ios::trunc);
            }
            file_.open(filepath, ios::binary | ios::in | ios::out | ios::app);
        }
        if (!file_.is_open())
            throw runtime_error("wal: cannot open: " + filepath);
    }

    Wal::~Wal()
    {
        if (file_.is_open())
            file_.close();
    }

    // ── Helper: write raw bytes ───────────────────────────────────────────────────
    static void write_u8(fstream &f, uint8_t v) { f.write(reinterpret_cast<char *>(&v), 1); }
    static void write_u16(fstream &f, uint16_t v) { f.write(reinterpret_cast<char *>(&v), 2); }
    static void write_u32(fstream &f, uint32_t v) { f.write(reinterpret_cast<char *>(&v), 4); }
    static void write_u64(fstream &f, uint64_t v) { f.write(reinterpret_cast<char *>(&v), 8); }

    // ── appendEntry ──────────────────────────────────────────────────────────────
    void Wal::appendEntry(const Entry &e)
    {
        // Build payload first
        vector<uint8_t> payload;

        if (e.op == Op::Insert)
        {
            uint16_t tlen = static_cast<uint16_t>(e.table.size());
            uint32_t rlen = static_cast<uint32_t>(e.row_data.size());
            payload.resize(2 + tlen + 4 + rlen);
            size_t off = 0;
            memcpy(payload.data() + off, &tlen, 2);
            off += 2;
            memcpy(payload.data() + off, e.table.data(), tlen);
            off += tlen;
            memcpy(payload.data() + off, &rlen, 4);
            off += 4;
            memcpy(payload.data() + off, e.row_data.data(), rlen);
        }

        uint32_t plen = static_cast<uint32_t>(payload.size());

        file_.seekp(0, ios::end);
        write_u64(file_, e.txn_id);
        write_u8(file_, static_cast<uint8_t>(e.op));
        write_u32(file_, plen);
        if (plen > 0)
            file_.write(reinterpret_cast<char *>(payload.data()), plen);
        file_.flush();
    }

    // ── Public log helpers ────────────────────────────────────────────────────────
    void Wal::logBegin(uint64_t txn_id)
    {
        Entry e;
        e.txn_id = txn_id;
        e.op = Op::Begin;
        appendEntry(e);
    }

    void Wal::logInsert(uint64_t txn_id, const string &table,
                        const vector<uint8_t> &row_data)
    {
        Entry e;
        e.txn_id = txn_id;
        e.op = Op::Insert;
        e.table = table;
        e.row_data = row_data;
        appendEntry(e);
    }

    void Wal::logCommit(uint64_t txn_id)
    {
        Entry e;
        e.txn_id = txn_id;
        e.op = Op::Commit;
        appendEntry(e);
    }

    void Wal::logRollback(uint64_t txn_id)
    {
        Entry e;
        e.txn_id = txn_id;
        e.op = Op::Rollback;
        appendEntry(e);
    }

    // ── readAll ───────────────────────────────────────────────────────────────────
    vector<Wal::Entry> Wal::readAll()
    {
        vector<Entry> result;
        file_.clear();
        file_.seekg(0, ios::beg);

        while (true)
        {
            uint64_t txn_id = 0;
            uint8_t op_raw = 0;
            uint32_t plen = 0;

            file_.read(reinterpret_cast<char *>(&txn_id), 8);
            if (file_.gcount() < 8)
                break;
            file_.read(reinterpret_cast<char *>(&op_raw), 1);
            file_.read(reinterpret_cast<char *>(&plen), 4);

            vector<uint8_t> payload(plen, 0);
            if (plen > 0)
                file_.read(reinterpret_cast<char *>(payload.data()), plen);

            Entry e;
            e.txn_id = txn_id;
            e.op = static_cast<Op>(op_raw);

            if (e.op == Op::Insert && plen >= 2)
            {
                uint16_t tlen = 0;
                memcpy(&tlen, payload.data(), 2);
                if (plen >= (size_t)(2 + tlen + 4))
                {
                    e.table.assign(reinterpret_cast<char *>(payload.data() + 2), tlen);
                    uint32_t rlen = 0;
                    memcpy(&rlen, payload.data() + 2 + tlen, 4);
                    size_t data_off = 2 + tlen + 4;
                    if (plen >= data_off + rlen)
                    {
                        e.row_data.assign(payload.begin() + data_off,
                                          payload.begin() + data_off + rlen);
                    }
                }
            }

            result.push_back(e);
        }
        return result;
    }

    // ── truncate ─────────────────────────────────────────────────────────────────
    void Wal::truncate()
    {
        file_.close();
        {
            ofstream tmp(filepath_, ios::binary | ios::trunc);
        }
        file_.open(filepath_, ios::binary | ios::in | ios::out | ios::app);
    }

} // namespace minisql::engine
