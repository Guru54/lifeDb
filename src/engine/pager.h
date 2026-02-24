#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <functional>

namespace minisql::engine
{

    using namespace std;

    // ── Constants ─────────────────────────────────────────────────────────────────
    static constexpr uint32_t PAGE_SIZE = 4096;
    static constexpr uint32_t PAGER_MAGIC = 0x4C51534D; // 'MSQL'

    // ── File layout ───────────────────────────────────────────────────────────────
    // Page 0  — header page:
    //   [0..3]   magic        (uint32)
    //   [4..7]   num_pages    (uint32)  — total pages including page 0
    //   [8..11]  num_rows     (uint32)
    //   [12..15] write_page   (uint32)  — page currently accepting writes (0 = none)
    //   [16..17] write_offset (uint16)  — bytes used so far in write_page
    //   [18..PAGE_SIZE-1] padding
    //
    // Pages 1..N — data pages:
    //   [0..1]   used_bytes   (uint16)  — row bytes written in this page
    //   [2..]    rows: [row_len: uint32][row_bytes...]
    //
    // Max row data per page = PAGE_SIZE - 2 = 4094 bytes

    class Pager
    {
    public:
        explicit Pager(const string &filepath);
        ~Pager();

        // Ek nayi row append karo
        void appendRow(const vector<uint8_t> &row_data);

        // Sabhi stored rows ko scan karo
        void scan(function<void(const uint8_t *, uint32_t)> callback);

        uint32_t numRows() const { return hdr_.num_rows; }

    private:
        static constexpr uint32_t DATA_CAPACITY = PAGE_SIZE - 2;

        struct Header
        {
            uint32_t magic = PAGER_MAGIC;
            uint32_t num_pages = 1;
            uint32_t num_rows = 0;
            uint32_t write_page = 0; // 0 means no data page yet
            uint16_t write_offset = 0;
        };

        void readHeader();
        void writeHeader();
        vector<uint8_t> readPage(uint32_t page_id);
        void writePage(uint32_t page_id, const vector<uint8_t> &data);
        uint32_t allocPage();

        fstream file_;
        string filepath_;
        Header hdr_;
    };

} // namespace minisql::engine
