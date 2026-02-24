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
    void Pager::appendRow(const vector<uint8_t> &row_data)
    {
        uint32_t row_len = static_cast<uint32_t>(row_data.size());
        uint32_t needed = 4 + row_len; // 4-byte length prefix + data

        // Nayi page chahiye?
        if (hdr_.write_page == 0 || hdr_.write_offset + needed > DATA_CAPACITY)
        {
            uint32_t new_pid = allocPage();
            hdr_.write_page = new_pid;
            hdr_.write_offset = 0;
        }

        // Current write page read karo
        vector<uint8_t> page = readPage(hdr_.write_page);

        // Row likhna: 2-byte used header ke baad
        uint8_t *ptr = page.data() + 2 + hdr_.write_offset;
        memcpy(ptr, &row_len, 4);
        memcpy(ptr + 4, row_data.data(), row_len);

        hdr_.write_offset += static_cast<uint16_t>(needed);

        // Page ka used_bytes update karo
        memcpy(page.data(), &hdr_.write_offset, 2);
        writePage(hdr_.write_page, page);

        // Header update karo
        hdr_.num_rows++;
        writeHeader();
    }

    // ── Row Scan ─────────────────────────────────────────────────────────────────
    void Pager::scan(function<void(const uint8_t *, uint32_t)> callback)
    {
        for (uint32_t pid = 1; pid < hdr_.num_pages; pid++)
        {
            vector<uint8_t> page = readPage(pid);

            uint16_t used = 0;
            memcpy(&used, page.data(), 2);

            uint32_t off = 0;
            while (off + 4 <= static_cast<uint32_t>(used))
            {
                uint32_t row_len = 0;
                memcpy(&row_len, page.data() + 2 + off, 4);
                off += 4;

                if (off + row_len > static_cast<uint32_t>(used))
                    break; // corruption guard

                callback(page.data() + 2 + off, row_len);
                off += row_len;
            }
        }
    }

} // namespace minisql::engine
