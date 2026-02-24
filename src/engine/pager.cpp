#include "engine/pager.h"

#include <cstring>
#include <stdexcept>

namespace minisql::engine
{

    using namespace std;

    // ── Open / Create ─────────────────────────────────────────────────────────────
    Pager::Pager(const string &filepath) : filepath_(filepath)
    {
        // Pehle existing file try karo
        file_.open(filepath, ios::binary | ios::in | ios::out);

        if (!file_.is_open())
        {
            // Nayi file banao
            {
                ofstream tmp(filepath, ios::binary | ios::trunc);
                if (!tmp)
                    throw runtime_error("pager: cannot create: " + filepath);
            }
            file_.open(filepath, ios::binary | ios::in | ios::out);
            if (!file_)
                throw runtime_error("pager: cannot open: " + filepath);

            // Blank header page likho
            hdr_ = Header{};
            writeHeader();
        }
        else
        {
            readHeader();
            if (hdr_.magic != PAGER_MAGIC)
                throw runtime_error("pager: invalid file (bad magic): " + filepath);
        }
    }

    Pager::~Pager()
    {
        if (file_.is_open())
            file_.close();
    }

    // ── Header Helpers ────────────────────────────────────────────────────────────
    void Pager::readHeader()
    {
        file_.seekg(0, ios::beg);
        file_.read(reinterpret_cast<char *>(&hdr_.magic), 4);
        file_.read(reinterpret_cast<char *>(&hdr_.num_pages), 4);
        file_.read(reinterpret_cast<char *>(&hdr_.num_rows), 4);
        file_.read(reinterpret_cast<char *>(&hdr_.write_page), 4);
        file_.read(reinterpret_cast<char *>(&hdr_.write_offset), 2);
    }

    void Pager::writeHeader()
    {
        vector<uint8_t> page(PAGE_SIZE, 0);
        memcpy(page.data() + 0, &hdr_.magic, 4);
        memcpy(page.data() + 4, &hdr_.num_pages, 4);
        memcpy(page.data() + 8, &hdr_.num_rows, 4);
        memcpy(page.data() + 12, &hdr_.write_page, 4);
        memcpy(page.data() + 16, &hdr_.write_offset, 2);

        file_.seekp(0, ios::beg);
        file_.write(reinterpret_cast<char *>(page.data()), PAGE_SIZE);
        file_.flush();
    }

    // ── Raw Page I/O ──────────────────────────────────────────────────────────────
    vector<uint8_t> Pager::readPage(uint32_t page_id)
    {
        vector<uint8_t> data(PAGE_SIZE, 0);
        file_.clear();
        file_.seekg(static_cast<streamoff>(page_id) * PAGE_SIZE, ios::beg);
        file_.read(reinterpret_cast<char *>(data.data()), PAGE_SIZE);
        return data;
    }

    void Pager::writePage(uint32_t page_id, const vector<uint8_t> &data)
    {
        file_.seekp(static_cast<streamoff>(page_id) * PAGE_SIZE, ios::beg);
        file_.write(reinterpret_cast<const char *>(data.data()), PAGE_SIZE);
        file_.flush();
    }

    uint32_t Pager::allocPage()
    {
        uint32_t new_id = hdr_.num_pages;
        hdr_.num_pages++;

        // File ko PAGE_SIZE bytes se extend karo
        vector<uint8_t> blank(PAGE_SIZE, 0);
        file_.seekp(0, ios::end);
        file_.write(reinterpret_cast<char *>(blank.data()), PAGE_SIZE);
        file_.flush();

        return new_id;
    }

    // ── Row Append ────────────────────────────────────────────────────────────────
    // Row format: [flags:1][row_len:4][row_bytes...]  flags=0 alive, flags=1 deleted
    Pager::RowLocation Pager::appendRow(const vector<uint8_t> &row_data)
    {
        uint32_t row_len = static_cast<uint32_t>(row_data.size());
        uint32_t needed = 1 + 4 + row_len; // tombstone + len + data

        // Nayi page chahiye?
        if (hdr_.write_page == 0 || hdr_.write_offset + needed > DATA_CAPACITY)
        {
            uint32_t new_pid = allocPage();
            hdr_.write_page = new_pid;
            hdr_.write_offset = 0;
        }

        vector<uint8_t> page = readPage(hdr_.write_page);

        uint32_t row_off = hdr_.write_offset; // offset of the flags byte
        uint8_t *ptr = page.data() + 2 + row_off;
        ptr[0] = 0; // flags = alive
        memcpy(ptr + 1, &row_len, 4);
        memcpy(ptr + 5, row_data.data(), row_len);

        hdr_.write_offset += static_cast<uint16_t>(needed);
        memcpy(page.data(), &hdr_.write_offset, 2);
        writePage(hdr_.write_page, page);

        hdr_.num_rows++;
        writeHeader();

        return {hdr_.write_page, row_off};
    }

    // ── Row Scan (alive only) ─────────────────────────────────────────────────────
    void Pager::scan(function<void(const uint8_t *, uint32_t)> callback)
    {
        for (uint32_t pid = 1; pid < hdr_.num_pages; pid++)
        {
            vector<uint8_t> page = readPage(pid);
            uint16_t used = 0;
            memcpy(&used, page.data(), 2);
            uint32_t off = 0;
            while (off + 5 <= static_cast<uint32_t>(used)) // 1+4 minimum
            {
                uint8_t flags = page[2 + off];
                uint32_t row_len = 0;
                memcpy(&row_len, page.data() + 2 + off + 1, 4);
                if (off + 5 + row_len > static_cast<uint32_t>(used))
                    break;
                if (!(flags & 0x01))
                    callback(page.data() + 2 + off + 5, row_len);
                off += 1 + 4 + row_len;
            }
        }
    }

    // ── Extended scan (gives row location for DELETE/UPDATE) ──────────────────────
    void Pager::scanEx(function<void(uint32_t, uint32_t, const uint8_t *, uint32_t)> callback)
    {
        for (uint32_t pid = 1; pid < hdr_.num_pages; pid++)
        {
            vector<uint8_t> page = readPage(pid);
            uint16_t used = 0;
            memcpy(&used, page.data(), 2);
            uint32_t off = 0;
            while (off + 5 <= static_cast<uint32_t>(used))
            {
                uint8_t flags = page[2 + off];
                uint32_t row_len = 0;
                memcpy(&row_len, page.data() + 2 + off + 1, 4);
                if (off + 5 + row_len > static_cast<uint32_t>(used))
                    break;
                if (!(flags & 0x01))
                    callback(pid, off, page.data() + 2 + off + 5, row_len);
                off += 1 + 4 + row_len;
            }
        }
    }

    // ── Tombstone: mark row deleted ───────────────────────────────────────────────
    void Pager::markDeleted(uint32_t page_id, uint32_t row_off)
    {
        vector<uint8_t> page = readPage(page_id);
        page[2 + row_off] = 0x01;
        writePage(page_id, page);
    }
} // namespace minisql::engine